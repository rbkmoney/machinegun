%%%
%%% Riak хранилище для machinegun'а.
%%%
%%% Важный момент, что в один момент времени не может существовать 2х процессов записи в бд по одной машине,
%%%  это гарантируется самим MG.
%%%
%%%
%%% ## Схема хранения
%%%
%%% Схема хранения следующая. Есть 3 бакета:
%%%     - для тэгов <ns>_tags(tag, machine_id);
%%%     - для эвентов <ns>_events(machine_id, event_id, created_at, body);
%%%     - для машин <ns>_machines(machine_id, event_ids, tags, status).
%%%  Всё энкодится в thrift (схему ещё нужно сделать, поэтому пока в term_to_binary) (
%%%    в json не вариант потому, что нужно хранить много бинарей,
%%%    в msgpack — потому, что мы его нигде больше не используем, и у нас уже есть thrift
%%%  )
%%%  Первичная запись — это машина, если ссылки из машины на эвент или тэг нет, то это потерянная запись.
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
%%% Резолвинг тэга(пока сделано попроще, без проверки коллизии тэгов):
%%%  - найти все ID машин по тегу
%%%  - получить все объекты
%%%  - все те тэги которых нет в объекте удалить (они просто потеряны)
%%%  - если машин больше одной, перевести их в ошибочное состояние (как?)
%%%  - если одна, вернуть её id
%%%
%%% Обновление машины:
%%%  - добавить новые эвенты
%%%  - добавить новый тэг
%%%  - обновить объект с машиной
%%%
%%% Удаление машины:
%%%  - удаление записи из таблицы машины (остальное станет мусором и удалится gc)
%%%
%%%
%%% ## Таймеры
%%%
%%% Таймеров пока нет.
%%% Для реализации таймеров есть 2 рабочих варианта:
%%%  - Таймеры работают как пара отдельных процессов, один является mg_timers,
%%%     второй — просто gen_server, котрый раз в dT / 2 выгружает ближайшие таймеры и выставляет их в mg_timers
%%%     Самый главный вопрос тут — это сложность реализации.
%%%  - При старте вычитываются все таймеры и загружаются в mg_timers, дальше в процессе работы все изменения
%%%     тоже туда применяются. Тут всё просто, но есть подозрения, что могут быть проблемы из-за слишком
%%%     большого количества таймеров.
%%%
%%% Как можно сделать хранение таймеров:
%%%  bucket type "timers"
%%%  bucket "<ns>_timers"
%%%  для начала можно сделать только одну запись
%%%  а вообще {Year, Month, Day} - TimersCRDTSet
%%% Остаётся вопрос с тем, как в эту табличку писать (писать до основной и проверять, что данные корректны).
%%%
%%%
%%% Требования:
%%%  - Данные риака, не хранятся на ceph, а лежат локально
%%%  - N >= 3, при этом мы сможем безболезненно терять minority
%%%    от N машины из кластера размером S (PR, PW, DW = quorum).
%%%
%%% Для append only бакетов (эвентов, тэгов) можно будет сделать хак с r=1 и notfound_ok=false
%%%
%%% Ответ {error, timeout} — это неопределённость и нужно понять, что с этим делать!
%%%  (Мы предполагаем, что тот факт, что мы получили неопределённость от одной из нод
%%%    транслируется в неопределённость на один запрос)
%%%
%%% Вопросы:
%%%  - Равен ли размера cp кластера mg размеру кластера riak? (нет, это совсем разные кластеры)
%%%  - Что делать с riak и cross-dc?
%%%  - можно ли при отсутствии after эвента возвращать [] (обсудили — нет)
%%%
%%% TODO:
%%%  - нужно сделать процесс очистки потеряных данных (gc)
%%%  -
%%%
-module(mg_storage_riak).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, create_machine/4, get_machine/3, get_history/5, update_machine/5]).

-type options() :: #{
    host      => inet            :ip_address  (),
    port      => inet            :port_number (),
    pool      => mg_storage_utils:pool_options()
}.

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, _ChildID) ->
    mg_storage_utils:pool_child_spec(pool_options(Options, Namespace)).

-spec create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, Namespace, ID, Args) ->
    do(
        Namespace,
        fun(Pid) ->
            object_to_machine(
                create_db_object(Pid, Options, Namespace, machine, ID,
                    #{
                        status       => {created, Args},
                        events_range => undefined,
                        tags         => []
                    }
                )
            )
        end
    ).

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

-spec get_history(options(), mg:ns(), mg:id(), mg_storage:machine(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, Namespace, ID, Machine, Range) ->
    do(
        Namespace,
        fun(Pid) ->
            [
                unpack(event, riakc_obj:get_value(get_db_object(Pid, Options, Namespace, event, FullEventID)))
                ||
                FullEventID <- mg_storage_utils:get_machine_events_ids(ID, Machine, Range)
            ]
        end
    ).

-spec update_machine(options(), mg:ns(), mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
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
-type db_obj_type() :: machine | event | tag.
% -type db_machine() :: #{
%     status =>  mg:status(),
%     events => [mg:event_id()]
% }.
-type object() :: riakc_obj:riakc_obj().
% -type db_event_id() :: {mg:id(), mg:event_id()}.

-spec create_db_object(pid(), options(), mg:ns(), db_obj_type(), mg:id(), _Data) ->
    object().
create_db_object(Pid, _Options, Namespace, Type, ID, Data) ->
    Object = riakc_obj:new(get_bucket(Type, Namespace), pack({id, Type}, ID), pack(Type, Data)),
    put_db_object(Pid, Object, put_options(Type)).


-spec get_db_object(pid(), options(), mg:ns(), db_obj_type(), mg:id()) ->
    object().
get_db_object(Pid, _Options, Namespace, Type, ID) ->
    case riakc_pb_socket:get(Pid, get_bucket(Type, Namespace), pack({id, Type}, ID), get_options(Type)) of
        {ok, Object} ->
            Object;
        {error, notfound} ->
            throw(not_found);
        % TODO понять какие проблемы временные, а какие постоянные
        {error, _Reason} ->
            % TODO log
            erlang:throw({temporary, storage_unavailable})
    end.

-spec put_db_object(pid(), object(), list()) ->
    object().
put_db_object(Pid, Object, Options) ->
    case riakc_pb_socket:put(Pid, Object, [return_body] ++ Options) of
        {ok, NewObject} ->
            NewObject;
        % TODO понять какие проблемы временные, а какие постоянные
        {error, _Reason} ->
            % TODO log
            erlang:throw({temporary, storage_unavailable})
    end.

-spec get_bucket(db_obj_type(), mg:ns()) ->
    binary().
get_bucket(Type, Namespace) ->
    stringify({Namespace, Type}).

%%
%% Про опции посмотреть можно тут
%% https://github.com/basho/riak-erlang-client/blob/develop/src/riakc_pb_socket.erl#L1526
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
        events_range := EventsRange
    } = unpack(machine, riakc_obj:get_value(Object)),
    #{
        status       => Status,
        events_range => EventsRange,
        db_state     => Object
    }.

-spec machine_to_object(mg_storage:machine()) ->
    object().
machine_to_object(Machine) ->
    #{
        status       := Status,
        events_range := EventsRange,
        db_state     := Object
    } = Machine,
    riakc_obj:update_value(Object, pack(machine, #{status => Status, events_range => EventsRange})).

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
        name => ns_to_atom(Namespace)
    }.

-spec ns_to_atom(mg:ns()) ->
    atom().
ns_to_atom(Namespace) ->
    erlang:binary_to_atom(stringify(Namespace), utf8).

%%
%% packer
%%
%% TODO thrift
%% TODO подумать как правильно генерить ключи
-spec pack(_, _) ->
    binary().
pack({id, event}, FullEventID) ->
    stringify(FullEventID);
pack({id, machine}, MachineID) ->
    MachineID;
pack(event, Event) ->
    <<(erlang:term_to_binary(Event))/binary>>;
pack(machine, Machine) ->
    <<(erlang:term_to_binary(Machine))/binary>>.

-spec unpack(_, binary()) ->
    _.
unpack(event, Data) ->
    erlang:binary_to_term(Data);
unpack(machine, Data) ->
    erlang:binary_to_term(Data).

%%

-type stringify_primitive() ::
      binary()
    | atom()
    | integer()
    | list(stringify_primitive())
    | tuple()
.

-spec stringify(stringify_primitive()) ->
    binary().
stringify(Binary) when is_binary(Binary) ->
    Binary;
stringify(Integer) when is_integer(Integer) ->
    erlang:integer_to_binary(Integer);
stringify(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom, utf8);
stringify(Tuple) when is_tuple(Tuple) ->
    stringify(erlang:tuple_to_list(Tuple));
stringify(List) when is_list(List) ->
    erlang:iolist_to_binary(mg_utils:join(<<"-">>, [stringify(Element) || Element <- List])).
