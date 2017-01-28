%%%
%%% Riak хранилище для machinegun'а.
%%%
%%% Важный момент, что единовременно не может существовать 2-х процессов записи в БД по одной машине,
%%%  это гарантируется самим MG.
%%%
%%%  Всё энкодится в msgpack и версионируется в метадате.
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
%%%  - классификация и обработка ошибок
%%%
-module(mg_storage_riak).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, put/5, get/3, delete/4]).


-type options() :: #{
    host      => inet            :ip_address  (),
    port      => inet            :port_number (),
    pool      =>                  pooler_options(),
    r_options => _,
    w_options => _,
    d_options => _
}.
-type pooler_options() :: #{
    name                 => term(),
    start_mfa            => {atom(), atom(), list()},
    max_count            => non_neg_integer     (),
    init_count           => non_neg_integer     (),
    cull_interval        => pooler_time_interval(),
    max_age              => pooler_time_interval(),
    member_start_timeout => pooler_time_interval()
}.
%% в pooler'е нет типов :(
-type pooler_time_interval() :: {non_neg_integer(), min | sec | ms}.

-type context() :: riakc_obj:vclock().

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, _ChildID) ->
    % ChildID pooler генерит сам добавляя префикс _pooler_
    pooler:pool_child_spec(maps:to_list(pooler_options(Options, Namespace))).

-spec put(options(), mg:ns(), mg_storage:key(), context() | undefined, mg_storage:value()) ->
    context().
put(Options, Namespace, Key, Context, Value) ->
    do(Options, Namespace, fun(Pid) ->
        Object = to_riak_obj(Namespace, Key, Context, Value),
        case riakc_pb_socket:put(Pid, Object, [return_body] ++ get_option(w_options, Options)) of
            {ok, NewObject} ->
                riakc_obj:vclock(NewObject);
            % TODO понять какие проблемы временные, а какие постоянные
            {error, Reason} ->
                erlang:throw({transient, {storage_unavailable, Reason}})
        end
    end).

-spec get(options(), mg:ns(), mg_storage:key()) ->
    {context(), mg_storage:value()} | undefined.
get(Options, Namespace, Key) ->
    do(Options, Namespace, fun(Pid) ->
        case riakc_pb_socket:get(Pid, Namespace, Key, get_option(r_options, Options)) of
            {ok, Object} ->
                from_riak_obj(Object);
            {error, notfound} ->
                undefined;
            {error, Reason} ->
                erlang:throw({transient, {storage_unavailable, Reason}})
        end
    end).

-spec delete(options(), mg:ns(), mg_storage:key(), context()) ->
    ok.
delete(Options, Namespace, Key, Context) ->
    do(Options, Namespace, fun(Pid) ->
        case riakc_pb_socket:delete_vclock(Pid, Namespace, Key, Context, get_option(d_options, Options)) of
            ok ->
                ok;
            {error, Reason} ->
                erlang:throw({transient, {storage_unavailable, Reason}})
        end
    end).

%%
%% pool
%%
-spec pooler_options(options(), mg:ns()) ->
    pooler_options().
pooler_options(Options=#{pool:=PoolOptions}, Namespace) ->
    PoolOptions#{
        start_mfa =>
            {
                riakc_pb_socket,
                start_link,
                [
                    get_option(host, Options),
                    get_option(port, Options)
                ]
            },
        % имя пула может быть только атомом  :-\
        name => pool_name(Namespace)
    }.

-spec do(options(), mg:ns(), fun((pid()) -> R)) ->
    R.
do(Options, Namespace, Fun) ->
    PoolName = pool_name(Namespace),
    Pid =
        case pooler:take_member(PoolName, get_option(pool_take_timeout, Options)) of
            error_no_members ->
                throw({transient, {storage_unavailable, {'pool is overloaded', PoolName}}});
            Pid_ ->
                Pid_
        end,
    try
        R = Fun(Pid),
        ok = pooler:return_member(PoolName, Pid, ok),
        R
    catch Class:Reason ->
        ok = pooler:return_member(PoolName, Pid, fail),
        erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

-spec pool_name(mg:ns()) ->
    atom().
pool_name(Namespace) ->
    % !!! осторожнее, тут можно нечаянно нагенерить атомов
    % предполагается, что их конечное и небольшое кол-во
    erlang:binary_to_atom(Namespace, utf8).

%%
%% packer
%%
%% фи-фи подтекает абстракция вызова mg_storage:opaque_to_binary(Value)
-define(msgpack_ct           , "application/x-msgpack").
-define(schema_version_md_key, <<"schema-version">>   ).
-define(schema_version_1     , <<"1">>                ).

-spec to_riak_obj(mg:ns(), mg_storage:key(), context(), mg_storage:value()) ->
    riakc_obj:riakc_obj().
to_riak_obj(Namespace, Key, Context, Value) ->
    Object =
        riakc_obj:set_vclock(
            riakc_obj:new(Namespace, Key, mg_storage:opaque_to_binary(Value)),
            Context
        ),
    riakc_obj:update_content_type(
        riakc_obj:update_metadata(
            Object,
            riakc_obj:set_user_metadata_entry(
                riakc_obj:get_metadata(Object),
                {?schema_version_md_key, ?schema_version_1}
            )
        ),
        ?msgpack_ct
    ).

-spec from_riak_obj(riakc_obj:riakc_obj()) ->
    {context(), mg_storage:value()}.
from_riak_obj(Object) ->
    Metadata          = riakc_obj:get_metadata(Object),
    ?schema_version_1 = riakc_obj:get_user_metadata_entry(Metadata, ?schema_version_md_key),
    ?msgpack_ct       = riakc_obj:get_content_type(Object),
    {riakc_obj:vclock(Object), mg_storage:binary_to_opaque(riakc_obj:get_value(Object))}.

%%

-spec get_option(atom(), options()) ->
    _.
get_option(Key, Options) ->
    maps:get(Key, Options, default_option(Key)).

%%
%% Про опции посмотреть можно тут
%% https://github.com/basho/riak-erlang-client/blob/develop/src/riakc_pb_socket.erl#L1526
%% Почитать про NRW и прочую магию можно тут http://basho.com/posts/technical/riaks-config-behaviors-part-2/
%%
-spec default_option(atom()) ->
    _.
default_option(host) -> "riakdb";
default_option(port) -> 8087;
default_option(pool_take_timeout) -> {5, 'sec'};
default_option(r_options) -> [{r, quorum}, {pr, quorum}];
default_option(w_options) -> [{w, quorum}, {pw, quorum}, {dw, quorum}];
default_option(d_options) -> []. % ?
