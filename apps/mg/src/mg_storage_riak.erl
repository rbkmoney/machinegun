%%%
%%% Riak хранилище для machinegun'а.
%%%
%%% Важный момент, что единовременно не может существовать 2-х процессов записи в БД по одной машине,
%%%  это гарантируется самим MG (а точнее mg_workers).
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
-include_lib("riakc/include/riakc.hrl").

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, put/6, get/3, search/3, delete/4]).

%% internal
-export([connect_to_riak/2]).

-type options() :: #{
    host      => inet:ip_address    (),
    port      => inet:port_number   (),
    pool      =>      pooler_options(),
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

-type context() :: riakc_obj:vclock() | undefined.

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, _ChildID) ->
    % ChildID pooler генерит сам добавляя префикс _pooler_
    pooler:pool_child_spec(maps:to_list(pooler_options(Options, Namespace))).

-spec put(options(), mg:ns(), mg_storage:key(), context(), mg_storage:value(), [mg_storage:index_update()]) ->
    context().
put(Options, Namespace, Key, Context, Value, IndexesUpdates) ->
    do(Options, Namespace,
        fun(Pid) ->
            Object = to_riak_obj(Namespace, Key, Context, Value, IndexesUpdates),
            NewObject =
                handle_riak_response(
                    riakc_pb_socket:put(Pid, Object, [return_body] ++ get_option(w_options, Options))
                ),
            riakc_obj:vclock(NewObject)
        end
    ).

-spec get(options(), mg:ns(), mg_storage:key()) ->
    {context(), mg_storage:value()} | undefined.
get(Options, Namespace, Key) ->
    do(Options, Namespace, fun(Pid) ->
        case riakc_pb_socket:get(Pid, Namespace, Key, get_option(r_options, Options)) of
            {error, notfound} ->
                undefined;
            Result ->
                Object = handle_riak_response(Result),
                from_riak_obj(Object)
        end
    end).

-spec search(options(), mg:ns(), mg_storage:index_query()) ->
    [{mg_storage:index_value(), mg_storage:key()}] | [mg_storage:key()].
search(Options, Namespace, Query) ->
    do(Options, Namespace,
        fun(Pid) ->
                Result = handle_riak_response(do_get_index(Pid, Namespace, Query)),
                get_index_response(Query, Result)
        end
    ).

-spec do_get_index(pid(), mg:ns(), mg_storage:index_query()) ->
    _.
do_get_index(Pid, Namespace, {IndexName, {From, To}}) ->
    riakc_pb_socket:get_index_range(Pid, Namespace, prepare_index_name(IndexName), From, To, [{return_terms, true}]);
do_get_index(Pid, Namespace, {IndexName, Value}) ->
    riakc_pb_socket:get_index_eq(Pid, Namespace, prepare_index_name(IndexName), Value).

-spec get_index_response(mg_storage:index_query(), get_index_results()) ->
    [{mg_storage:index_value(), mg_storage:key()}] | [mg_storage:key()].
get_index_response({_, {_, _}}, #index_results_v1{keys = []}) ->
    % это какой-то пипец, а не код, они там все упоролись что-ли?
    [];
get_index_response({_, {_, _}}, #index_results_v1{terms = Terms}) ->
    % получить из риака стабильный порядок следования не получилось,
    % поэтому пришлось сделать небольшой хак
    lists:sort(
        lists:map(
            fun({IndexValue, Key}) ->
                {erlang:binary_to_integer(IndexValue), Key}
            end,
            Terms
        )
    );
get_index_response({_, _}, #index_results_v1{keys = Keys}) ->
    Keys.

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
                ?MODULE,
                connect_to_riak,
                [
                    get_option(host, Options),
                    get_option(port, Options)
                ]
            },
        % имя пула может быть только атомом  :-\
        name => pool_name(Namespace)
    }.

-spec connect_to_riak(inet:ip_address() | inet:hostname(), inet:port_number()) ->
    {ok, pid()} | {error, term()}.
connect_to_riak(Host, Port) ->
    riakc_pb_socket:start_link(lists_random(get_addrs_by_host(Host)), Port).

-spec lists_random(list(T)) ->
    T.
lists_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

-spec get_addrs_by_host(inet:ip_address() | inet:hostname()) ->
    [inet:ip_address()].
get_addrs_by_host(Host) ->
    case inet_parse:address(Host) of
        {ok, Addr} ->
            [Addr];
        {error, _} ->
            Timer = inet:start_timer(5000), % TODO надо прокидывать свыше!
            R = erlang:apply(inet_db:tcp_module(), getaddrs, [Host, Timer]),
            _ = inet:stop_timer(Timer),
            case R of
                {ok, Addrs} ->
                    Addrs;
                {error, _} ->
                    exit({'invalid host address', Host})
            end
    end.

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

-spec to_riak_obj(mg:ns(), mg_storage:key(), context(), mg_storage:value(), [mg_storage:index_update()]) ->
    riakc_obj:riakc_obj().
to_riak_obj(Namespace, Key, Context, Value, IndexesUpdates) ->
    Object =
        riakc_obj:set_vclock(
            riakc_obj:new(Namespace, Key, mg_storage:opaque_to_binary(Value)),
            Context
        ),
    riakc_obj:update_content_type(
        riakc_obj:update_metadata(
            Object,
            riakc_obj:set_user_metadata_entry(
                riakc_obj:set_secondary_index(
                    riakc_obj:get_metadata(Object),
                    prepare_indexes_updates(IndexesUpdates)
                ),
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

-type riak_index_name  () :: {integer_index, list()}.
-type riak_index_update() :: {riak_index_name(), [mg_storage:index_value()]}.
-type get_index_results() :: #index_results_v1{}.

-spec prepare_indexes_updates([mg_storage:index_update()]) ->
    [riak_index_update()].
prepare_indexes_updates(IndexesUpdates) ->
    [prepare_index_update(IndexUpdate) || IndexUpdate <- IndexesUpdates].

-spec prepare_index_update(mg_storage:index_update()) ->
    riak_index_update().
prepare_index_update({IndexName, IndexValue}) ->
    {prepare_index_name(IndexName), [IndexValue]}.

-spec prepare_index_name(mg_storage:index_name()) ->
    riak_index_name().
prepare_index_name({binary, Name}) ->
    {binary_index, erlang:binary_to_list(Name)};
prepare_index_name({integer, Name}) ->
    {integer_index, erlang:binary_to_list(Name)}.

%%

-spec get_option(atom(), options()) ->
    _.
get_option(Key, Options) ->
    maps:get(Key, Options, default_option(Key)).

-spec handle_riak_response(ok | {ok, T} | {error, _Reason}) ->
    T | no_return().
handle_riak_response(ok) ->
    ok;
handle_riak_response({ok, Value}) ->
    Value;
handle_riak_response({error, Reason}) ->
    % TODO понять какие проблемы временные, а какие постоянные
    erlang:throw({transient, {storage_unavailable, Reason}}).

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
