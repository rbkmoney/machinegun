%%%
%%% Riak хранилище для machinegun'а.
%%%
%%% Важный момент, что единовременно не может существовать 2-х процессов записи в БД по одной машине,
%%%  это гарантируется самим MG.
%%%
%%%
%%% ## Схема хранения
%%%
%%% Схема хранения следующая. Есть 3 бакета:
%%%     - для эвентов <ns>_events({machine_id, event_id}, {created_at  , body             });
%%%     - для машин <ns>_machines( machine_id           , {events_range, aux_state, status}).
%%%  Всё энкодится в msgpack и версионируется в метадате.
%%%  Первичная запись — это машина, если ссылки из машины на эвент нет, то это потерянная запись.
%%%
%%%
%%% ## Процессы
%%%
%%% Создание машины:
%%%  - создать объект
%%%  - записать в бд
%%%  - венуть объект
%%%
%%% Получение машины:
%%%  - получить объект
%%%  - венуть машину
%%%
%%% Получение истории:
%%%  - получить объект
%%%  - применить фильтр к списку ID эвентов
%%%  - получить эвенты
%%%  - венуть эвенты
%%%
%%% Обновление машины:
%%%  - добавить новые эвенты
%%%  - обновить объект с машиной
%%%
%%% Удаление машины:
%%%  - удаление записи из таблицы машины (остальное станет мусором и удалится gc) (?)
%%%
%%%
%%% Требования:
%%%  - Данные риака лежат локально желательно на SSD
%%%  - N >= 3, при этом мы сможем безболезненно терять minority
%%%    от N машины из кластера размером S (R=W=PR=PW=DW=quorum).
%%%
%%% Для append only бакетов (эвентов) можно подумать о чтении с R=PR=1 и notfound_ok=false
%%%
%%% Ошибка {error, timeout} — это неопределённость и нужно понять, что с этим делать!
%%%  (Мы предполагаем, что тот факт, что мы получили неопределённость от одной из нод
%%%    транслируется в неопределённость на один запрос)
%%% Нужно делать все записи в базу идемпотентными и при любой ошибке неопределённости или недоступности ретраить.
%%%
%%% Вопросы:
%%%  - Равен ли размера cp кластера MG размеру кластера riak? (нет, это совсем разные кластеры)
%%%  - Что делать с Riak и cross-dc? (пока не думаем)
%%%  - Можно ли при отсутствии after эвента возвращать []? (обсудили — нет)
%%%  - Верна ли гипотеза, что при записи в один поток с R=W=PR=PW=quorum не будет slibing'ов?
%%%  -
%%%
%%% TODO:
%%%  - нужно сделать процесс очистки потеряных данных (gc) (а нужно ли?)
%%%  - классификация и обработка ошибок
%%%  - при отсутствии after эвента венуть исключение
%%%
-module(mg_storage_riak).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, get_machine/3, get_history/5, update_machine/5]).

-type options() :: #{
    host => inet            :ip_address  (),
    port => inet            :port_number (),
    pool => mg_storage_utils:pool_options()
}.

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, _ChildID) ->
    mg_storage_utils:pool_child_spec(pool_options(Options, Namespace)).

-spec get_machine(options(), mg:ns(), mg:id()) ->
    mg_storage:machine() | undefined.
get_machine(Options, Namespace, ID) ->
    do(
        Namespace,
        fun(Pid) ->
            try
                object_to_machine(get_db_object(Pid, Options, Namespace, machine, ID))
            catch throw:not_found ->
                undefined
            end
        end
    ).

-spec get_history(options(), mg:ns(), mg:id(), mg_storage:machine(), mg:history_range()) ->
    mg:history().
get_history(Options, Namespace, ID, Machine, Range) ->
    do(
        Namespace,
        fun(Pid) ->
            [
                begin
                    Event = unpack_from_object(event, get_db_object(Pid, Options, Namespace, event, FullEventID)),
                    Event#{id=>EventID}
                end
                ||
                FullEventID={_, EventID} <- mg_storage_utils:get_machine_events_ids(ID, Machine, Range)
            ]
        end
    ).

-spec update_machine(options(), mg:ns(), mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
update_machine(Options, Namespace, ID, Machine=#{db_state:=undefined}, Update) ->
    update_machine(Options, Namespace, ID, Machine#{db_state:=new_db_object(Namespace, machine, ID)}, Update);
update_machine(Options, Namespace, ID, Machine, Update) ->
    do(
        Namespace,
        fun(Pid) ->
            NewMachine =
                maps:fold(
                    fun(UpdateAction, UpdateValue, MachineAcc) ->
                        apply_machine_update(Pid, Options, Namespace, ID, UpdateAction, UpdateValue, MachineAcc)
                    end,
                    Machine,
                    Update
                ),
            object_to_machine(put_db_object(Pid, machine_to_object(NewMachine), put_options(machine)))
        end
    ).

-spec apply_machine_update(pid(), options(), mg:ns(), mg:id(), atom(), term(), mg_storage:machine()) ->
    mg_storage:machine().
apply_machine_update(_Pid, _Options, _Namespace, _ID, status, NewStatus, Machine) ->
    Machine#{status:=NewStatus};
apply_machine_update(_Pid, _Options, _Namespace, _ID, aux_state, NewAuxState, Machine) ->
    Machine#{aux_state:=NewAuxState};
apply_machine_update(Pid, Options, Namespace, ID, new_events, NewEvents, Machine=#{events_range:=EventsRange}) ->
    #{id:=NewLastEventID} = lists:last(NewEvents),
    NewEventsRange =
        case EventsRange of
            {FirstEventID, _} ->
                {FirstEventID, NewLastEventID};
            undefined ->
                [#{id:=FirstEventID}|_]=NewEvents,
                {FirstEventID, NewLastEventID}
        end,
    _ = lists:foreach(
            fun(Event=#{id:=EventID}) ->
                _ = create_db_object(Pid, Options, Namespace, event, {ID, EventID}, Event)
            end,
            NewEvents
        ),
    Machine#{events_range:=NewEventsRange}.

%%
%% db interation
%%
-type db_object_id() :: mg:id() | {mg:id(), mg:event_id()}.
-type db_object_type() :: machine | event.
-type db_event() :: mg:event().
-type db_machine() :: #{
    aux_state    => mg:aux_state(),
    status       => mg_storage:status(),
    events_range => mg_storage:events_range()
}.
-type db_object() :: db_machine() | db_event().
-type object() :: riakc_obj:riakc_obj().



-spec new_db_object(mg:ns(), db_object_type(), db_object_id()) ->
    object().
new_db_object(Namespace, Type, ID) ->
    riakc_obj:new(get_bucket(Type, Namespace), pack_key(Type, ID)).

-spec create_db_object(pid(), options(), mg:ns(), db_object_type(), db_object_id(), db_object()) ->
    object().
create_db_object(Pid, _Options, Namespace, Type, ID, Data) ->
    Object =
        pack_to_object(
            Type,
            new_db_object(Namespace, Type, ID),
            Data
        ),
    put_db_object(Pid, Object, put_options(Type)).

-spec get_db_object(pid(), options(), mg:ns(), db_object_type(), db_object_id()) ->
    object().
get_db_object(Pid, _Options, Namespace, Type, ID) ->
    case riakc_pb_socket:get(Pid, get_bucket(Type, Namespace), pack_key(Type, ID), get_options(Type)) of
        {ok, Object} ->
            Object;
        {error, notfound} ->
            throw(not_found);
        % TODO понять какие проблемы временные, а какие постоянные
        {error, Reason} ->
            erlang:throw({transient, {storage_unavailable, Reason}})
    end.

-spec put_db_object(pid(), object(), list()) ->
    object().
put_db_object(Pid, Object, Options) ->
    case riakc_pb_socket:put(Pid, Object, [return_body] ++ Options) of
        {ok, NewObject} ->
            NewObject;
        % TODO понять какие проблемы временные, а какие постоянные
        {error, Reason} ->
            erlang:throw({transient, {storage_unavailable, Reason}})
    end.

-spec get_bucket(db_object_type(), mg:ns()) ->
    binary().
get_bucket(Type, Namespace) ->
    mg_utils:concatenate_namespaces(Namespace, erlang:atom_to_binary(Type, utf8)).

%%
%% Про опции посмотреть можно тут
%% https://github.com/basho/riak-erlang-client/blob/develop/src/riakc_pb_socket.erl#L1526
%% Почитать про NRW и прочую магию можно тут http://basho.com/posts/technical/riaks-config-behaviors-part-2/
%%
%% Пока идея в том, чтобы оставить всё максимально консистентно
-spec get_options(_) ->
    list().
get_options(_) ->
    [{r, quorum}, {pr, quorum}].

-spec put_options(_) ->
    list().
put_options(_) ->
    [{w, quorum}, {pw, quorum}, {dw, quorum}].

%%
%% utils
%%
-spec object_to_machine(object()) ->
    mg_storage:machine().
object_to_machine(Object) ->
    #{
        status       := Status,
        aux_state    := AuxState,
        events_range := EventsRange
    } = unpack_from_object(machine, Object),
    #{
        status       => Status,
        events_range => EventsRange,
        aux_state    => AuxState,
        db_state     => Object
    }.

-spec machine_to_object(mg_storage:machine()) ->
    object().
machine_to_object(Machine) ->
    #{
        status       := Status,
        aux_state    := AuxState,
        events_range := EventsRange,
        db_state     := Object
    } = Machine,
    DBMachine = #{status => Status, aux_state => AuxState, events_range => EventsRange},
    % TODO можно апдейтить только в том случае, если изменилось
    pack_to_object(machine, Object, DBMachine).


-spec do(mg:ns(), fun((pid()) -> Result)) ->
    Result.
do(Namespace, Fun) ->
    mg_storage_utils:pool_do(ns_to_atom(Namespace), Fun).

-spec pool_options(options(), mg:ns()) ->
    mg_storage_utils:pool_options().
pool_options(Options=#{pool:=PoolOptions}, Namespace) ->
    PoolOptions#{
        start_mfa =>
            {
                riakc_pb_socket,
                start_link,
                [
                    maps:get(host, Options, "riakdb"),
                    maps:get(port, Options, 8087    )
                ]
            },
        % имя пула может быть только атомом  :-\
        name => ns_to_atom(Namespace)
    }.

-spec ns_to_atom(mg:ns()) ->
    atom().
ns_to_atom(Namespace) ->
    % !!! осторожнее, тут можно нечаянно нагенерить атомов
    % предполагается, что их конечное и небольшое кол-во
    erlang:binary_to_atom(Namespace, utf8).

%%
%% packer
%%
-type type     () :: event | machine.
-type riak_key () :: binary().
-type msg_value() :: _.
-define(msgpack_ct, "application/x-msgpack").
-define(msgpack_options, [
    {spec           , new        },
    {allow_atom     , none       },
    {unpack_str     , as_binary  },
    {validate_string, false      },
    {pack_str       , none       },
    {map_format     , map        }
]).
-define(schema_version_md_key, <<"schema-version">>).
-define(schema_version_1     , <<"1">>).

-spec pack_key(type(), _) ->
    riak_key().
pack_key(event, {MachineID, EventID}) ->
    <<MachineID/binary, "_", (erlang:integer_to_binary(EventID))/binary>>;
pack_key(machine, MachineID) ->
    MachineID.

-spec pack_to_object(type(), object(), db_object()) ->
    object().
pack_to_object(Type, Object, Data) ->
    riakc_obj:update_value(
        riakc_obj:update_content_type(
            riakc_obj:update_metadata(
                Object,
                riakc_obj:set_user_metadata_entry(
                    riakc_obj:get_metadata(Object),
                    {?schema_version_md_key, ?schema_version_1}
                )
            ),
            ?msgpack_ct
        ),
        msgpack_pack(Type, Data)
    ).

-spec unpack_from_object(type(), object()) ->
    db_object().
unpack_from_object(Type, Object) ->
    % TODO сделать через версионирование
    Metadata          = riakc_obj:get_metadata(Object),
    ?schema_version_1 = riakc_obj:get_user_metadata_entry(Metadata, ?schema_version_md_key),
    ?msgpack_ct       = riakc_obj:get_content_type(Object),
    msgpack_unpack(Type, riakc_obj:get_value(Object)).

%% в функции msgpack:pack неверный спек
-dialyzer({nowarn_function, msgpack_pack/2}).
-spec msgpack_pack(Type::atom(), _) ->
    msg_value().
msgpack_pack(Type, Value) ->
    case msgpack:pack(msgpack_pack_(Type, Value), ?msgpack_options) of
        Data when is_binary(Data) ->
            Data;
        {error, Reason} ->
            erlang:error(msgpack_pack_error, [Type, Value, Reason])
    end.

-spec msgpack_unpack(Type::atom(), msg_value()) ->
    _.
msgpack_unpack(Type, MsgpackValue) ->
    case msgpack:unpack(MsgpackValue, ?msgpack_options) of
        {ok, Data} ->
            msgpack_unpack_(Type, Data);
        {error, Reason} ->
            erlang:error(msgpack_unpack_error, [Type, MsgpackValue, Reason])
    end.

%% мы хотим, чтобы всё было компактно
-spec msgpack_pack_(Type::atom(), _) ->
    msg_value().
msgpack_pack_(_, undefined) ->
    null;
msgpack_pack_(term, Term) ->
    erlang:term_to_binary(Term);
msgpack_pack_(opaque, Opaque) ->
    Opaque;
msgpack_pack_(date, Time) ->
    msgpack_pack_(integer, Time);
msgpack_pack_(integer, Integer) when is_integer(Integer) ->
    Integer;
msgpack_pack_(event, #{created_at:=CreatedAt, body:=Body}) ->
    #{
        <<"ca">> => msgpack_pack_(date  , CreatedAt),
        <<"b" >> => msgpack_pack_(opaque, Body     )
    };
msgpack_pack_(machine_status, working) ->
    #{
        <<"n">> => <<"w">>
    };
msgpack_pack_(machine_status, {error, Reason}) ->
    #{
        <<"n">> => <<"e">>,
        <<"r">> => msgpack_pack_(term, Reason)
    };
msgpack_pack_(event_id, EventID) ->
    msgpack_pack_(integer, EventID);
msgpack_pack_(events_range, {FirstEventID, LastEventID}) ->
    #{
        <<"f">> => msgpack_pack_(event_id, FirstEventID),
        <<"l">> => msgpack_pack_(event_id, LastEventID )
    };
msgpack_pack_(aux_state, Status) ->
    msgpack_pack_(opaque, Status);
msgpack_pack_(machine, #{status:=Status, events_range:=EventRange, aux_state:=AuxState}) ->
    #{
        <<"s" >> => msgpack_pack_(machine_status, Status    ),
        <<"er">> => msgpack_pack_(events_range  , EventRange),
        <<"as">> => msgpack_pack_(aux_state     , AuxState  )
    };
msgpack_pack_(Type, Value) ->
    erlang:error(badarg, [Type, Value]).

-spec msgpack_unpack_(Type::atom(), msg_value()) ->
    _.
msgpack_unpack_(_, null) ->
    undefined;
msgpack_unpack_(term, Binary) when is_binary(Binary) ->
    erlang:binary_to_term(Binary);
msgpack_unpack_(opaque, Opaque) ->
    Opaque;
msgpack_unpack_(date, Time) ->
    msgpack_unpack_(integer, Time);
msgpack_unpack_(integer, Integer) when is_integer(Integer) ->
    Integer;
msgpack_unpack_(event, #{<<"ca">> := CreatedAt, <<"b">> := Body}) ->
    #{
        created_at => msgpack_unpack_(date  , CreatedAt),
        body       => msgpack_unpack_(opaque, Body     )
    };
msgpack_unpack_(machine_status, #{<<"n">> := <<"w">>}) ->
    working;
msgpack_unpack_(machine_status, #{<<"n">> := <<"e">>, <<"r">> := Reason}) ->
    {error, msgpack_unpack_(term, Reason)};
msgpack_unpack_(event_id, EventID) ->
    msgpack_unpack_(integer, EventID);
msgpack_unpack_(events_range, #{<<"f">> := FirstEventID, <<"l">> := LastEventID}) ->
    {msgpack_unpack_(event_id, FirstEventID), msgpack_unpack_(event_id, LastEventID)};
msgpack_unpack_(aux_state, Status) ->
    msgpack_unpack_(opaque, Status);
msgpack_unpack_(machine, #{<<"s">> := Status, <<"er">> := EventRange, <<"as">> := AuxState}) ->
    #{
        status       => msgpack_unpack_(machine_status, Status    ),
        events_range => msgpack_unpack_(events_range  , EventRange),
        aux_state    => msgpack_unpack_(aux_state     , AuxState  )
    };
msgpack_unpack_(Type, Value) ->
    erlang:error(badarg, [Type, Value]).
