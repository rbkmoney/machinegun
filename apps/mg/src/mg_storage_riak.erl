%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

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
-export([child_spec/2, child_spec/3, do_request/3]).

%% internal
-export([start_link/1]).
-export([start_link/2]).

% from riakc
% -type bucket() :: binary().
-type options() :: #{
    host            := inet:ip_address() | inet:hostname() | binary(),
    port            := inet:port_number(),
    bucket          := bucket(),
    resolve_timeout => timeout(),
    connect_timeout => timeout(),
    request_timeout => timeout(),
    r_options       => _,
    w_options       => _,
    d_options       => _
}.

-type context() :: riakc_obj:vclock() | undefined.

-type index_opt() :: {timeout, timeout()} |
                     {call_timeout, timeout()} |
                     {stream, boolean()} |
                     {continuation, binary()} |
                     {pagination_sort, boolean()} |
                     {max_results, non_neg_integer() | all}.
-type range_index_opt() :: {return_terms, boolean()} |
                           {term_regex, binary()}.
-type range_index_opts() :: [index_opt() | range_index_opt()].
-type self_ref() :: mg_utils:gen_ref().

%%
%% internal API
%%
-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options = #{port := Port}) ->
    IP = get_riak_addr(Options),
    riakc_pb_socket:start_link(IP, Port, [{connect_timeout, get_option(connect_timeout, Options)}]).

-spec start_link(options(), mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(Options = #{port := Port}, RegName) ->
    IP = get_riak_addr(Options),
    riakc_pb_socket:start_link(IP, Port, [{connect_timeout, get_option(connect_timeout, Options)}], RegName).

%%
%% mg_storage_pool callbacks
%%
-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, RegName) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, RegName]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec do_request(options(), self_ref(), mg_storage:request()) ->
    mg_storage:response().
do_request(Options, SelfRef, Request) ->
    case Request of
        {put, Key, Context, Value, IndexesUpdates} ->
            put(Options, SelfRef, Key, Context, Value, IndexesUpdates);
        {get, Key} ->
            get(Options, SelfRef, Key);
        {search, Query} ->
            search(Options, SelfRef, Query);
        {delete, Key, Context} ->
            delete(Options, SelfRef, Key, Context)
    end.

%%
%% local
%%
%% MG-123
%% При отваливании сокета процесс riakc_pb_socket падает c reason `disconnected`.
%% По его мнению это нестандартная ситуация (и нужно именно падать, а не выходить с normal),
%% но мы думаем иначе.
%%
-define(SAFE(Expr),
    try
        Expr
    catch
        exit:{disconnected, _} ->
            {error, disconnected};
        % в риак клиенте все ошибки при подключении будут выглядеть так
        % https://github.com/rbkmoney/riak-erlang-client/blob/edba3d0f/src/riakc_pb_socket.erl#L1378
        exit:{tcp, _} ->
            {error, disconnected}
    end
).
-spec put(options(), self_ref(), mg_storage:key(), context(), mg_storage:value(), [mg_storage:index_update()]) ->
    context().
put(Options = #{bucket := Bucket}, SelfRef, Key, Context, Value, IndexesUpdates) ->
    Object = to_riak_obj(Bucket, Key, Context, Value, IndexesUpdates),
    Timeout = get_option(request_timeout, Options),
    NewObject =
        handle_riak_response(
            ?SAFE(riakc_pb_socket:put(SelfRef, Object, [return_body] ++ get_option(w_options, Options), Timeout))
        ),
    riakc_obj:vclock(NewObject).

-spec get(options(), self_ref(), mg_storage:key()) ->
    {context(), mg_storage:value()} | undefined.
get(_Options, _SelfRef, <<"">>) ->
    undefined;
get(Options = #{bucket := Bucket}, SelfRef, Key) ->
    Timeout = get_option(request_timeout, Options),
    case ?SAFE(riakc_pb_socket:get(SelfRef, Bucket, Key, get_option(r_options, Options), Timeout)) of
        {error, notfound} ->
            undefined;
        Result ->
            Object = handle_riak_response_(Result),
            from_riak_obj(Object)
    end.

-spec search(options(), self_ref(), mg_storage:index_query()) ->
    mg_storage:search_result().
search(Options = #{bucket := Bucket}, SelfRef, Query) ->
    LiftedQuery = lift_query(Query),
    Result = handle_riak_response_(do_get_index(SelfRef, Bucket, LiftedQuery, Options)),
    get_index_response(LiftedQuery, Result).

-spec delete(options(), self_ref(), mg_storage:key(), context()) ->
    ok.
delete(Options = #{bucket := Bucket}, SelfRef, Key, Context) ->
    case ?SAFE(riakc_pb_socket:delete_vclock(SelfRef, Bucket, Key, Context, get_option(d_options, Options))) of
        ok ->
            ok;
        {error, Reason} ->
            erlang:throw({transient, {storage_unavailable, Reason}})
    end.

%%

-spec do_get_index(self_ref(), bucket(), mg_storage:index_query(), options()) ->
    _.
do_get_index(SelfRef, Bucket, {IndexName, {From, To}, IndexLimit, Continuation}, Options) ->
    SearchOptions = index_opts([{return_terms, true}], Options, IndexLimit, Continuation),
    ?SAFE(riakc_pb_socket:get_index_range(SelfRef, Bucket, prepare_index_name(IndexName), From, To, SearchOptions));
do_get_index(SelfRef, Bucket, {IndexName, Value, IndexLimit, Continuation}, Options) ->
    SearchOptions = index_opts(Options, IndexLimit, Continuation),
    ?SAFE(riakc_pb_socket:get_index_eq(SelfRef, Bucket, prepare_index_name(IndexName), Value, SearchOptions)).

-spec get_index_response(mg_storage:index_query(), get_index_results()) ->
    mg_storage:search_result().
get_index_response({_, Val, Limit, _}, #index_results_v1{keys = [], continuation = Cont}) when is_tuple(Val) ->
    % это какой-то пипец, а не код, они там все упоролись что-ли?
    wrap_index_response([], Limit, Cont);
get_index_response({_, Val, Limit, _}, #index_results_v1{terms = Terms, continuation = Cont}) when is_tuple(Val) ->
    Res = lists:map(
        fun({IndexValue, Key}) ->
            {erlang:binary_to_integer(IndexValue), Key}
        end,
        Terms
    ),
    wrap_index_response(Res, Limit, Cont);
get_index_response({_, _, Limit, _}, #index_results_v1{keys = Keys, continuation = Cont}) ->
    wrap_index_response(Keys, Limit, Cont).

-spec wrap_index_response(_, mg_storage:index_limit(), continuation()) ->
    mg_storage:search_result().
wrap_index_response(Res, Limit, Cont) ->
    case Limit of
        inf -> Res;
        _   -> {Res, Cont}
    end.

-spec lift_query(mg_storage:index_query()) ->
    mg_storage:index_query().
lift_query({Name, Val}) ->
    {Name, Val, inf, undefined};
lift_query({Name, Val, Limit}) ->
    {Name, Val, Limit, undefined};
lift_query({Name, Val, Limit, Continuation}) ->
    {Name, Val, Limit, Continuation}.

-spec index_opts(options(), mg_storage:index_limit(), continuation()) ->
    range_index_opts().
index_opts(Options, IndexLimit, Continuation) ->
    index_opts([], Options, IndexLimit, Continuation).

-spec index_opts(range_index_opts(), options(), mg_storage:index_limit(), continuation()) ->
    range_index_opts().
index_opts(DefaultOpts, Options, IndexLimit, Continuation) ->
    lists:append([
        common_index_opts(Options),
        max_result_opts(IndexLimit),
        continuation_opts(Continuation),
        DefaultOpts
    ]).

-spec continuation_opts(continuation()) ->
    range_index_opts().
continuation_opts(Continuation) ->
    case Continuation of
        undefined -> [];
        _         -> [{continuation, Continuation}]
    end.

-spec max_result_opts(mg_storage:index_limit()) ->
    range_index_opts().
max_result_opts(IndexLimit) ->
    case IndexLimit of
        inf -> [];
        _   -> [{max_results, IndexLimit}]
    end.

-spec common_index_opts(options()) ->
    range_index_opts().
common_index_opts(Options) ->
    [{pagination_sort, true}, {timeout, get_option(request_timeout, Options)}].

%%
%% packer
%%
%% фи-фи подтекает абстракция вызова mg_storage:opaque_to_binary(Value)
-define(msgpack_ct           , "application/x-msgpack").
-define(schema_version_md_key, <<"schema-version">>   ).
-define(schema_version_1     , <<"1">>                ).

-spec to_riak_obj(bucket(), mg_storage:key(), context(), mg_storage:value(), [mg_storage:index_update()]) ->
    riakc_obj:riakc_obj().
to_riak_obj(Bucket, Key, Context, Value, IndexesUpdates) ->
    Object = riakc_obj:set_vclock(new_riak_object(Bucket, Key, Value), Context),
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

-spec new_riak_object(bucket(), mg_storage:key(), mg_storage:value()) ->
    riakc_obj:riakc_obj().
new_riak_object(Bucket, Key, Value) ->
    case riakc_obj:new(Bucket, Key, mg_storage:opaque_to_binary(Value)) of
        {error, Reason} ->
            exit({storage_unexpected_error, Reason});
        Obj ->
            Obj
    end.

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
handle_riak_response(V) ->
    handle_riak_response_(V).

-spec handle_riak_response_({ok, T} | {error, _Reason}) ->
    T | no_return().
handle_riak_response_({ok, Value}) ->
    Value;
handle_riak_response_({error, Reason}) ->
    % TODO понять какие проблемы временные, а какие постоянные
    erlang:throw({transient, {storage_unavailable, Reason}}).

%%
%% Про опции посмотреть можно тут
%% https://github.com/basho/riak-erlang-client/blob/develop/src/riakc_pb_socket.erl#L1526
%% Почитать про NRW и прочую магию можно тут http://basho.com/posts/technical/riaks-config-behaviors-part-2/
%%
-spec default_option(atom()) ->
    _.
default_option(resolve_timeout) -> 5000;
default_option(connect_timeout) -> 5000;
default_option(request_timeout) -> 10000;
default_option(r_options) -> [{r, quorum}, {pr, quorum}];
default_option(w_options) -> [{w, quorum}, {pw, quorum}, {dw, quorum}];
default_option(d_options) -> []. % ?

-spec get_riak_addr(options()) ->
    inet:ip_address().
get_riak_addr(Options=#{host := Host}) ->
    lists_random(get_addrs_by_host(Host, get_option(resolve_timeout, Options))).

-spec lists_random(list(T)) ->
    T.
lists_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

-spec get_addrs_by_host(inet:ip_address() | inet:hostname() | binary(), timeout()) ->
    [inet:ip_address()].
get_addrs_by_host(Host, Timeout) when is_binary(Host) ->
    get_addrs_by_host(unicode:characters_to_list(Host, utf8), Timeout);
get_addrs_by_host(Host, Timeout) ->
    case inet_parse:address(Host) of
        {ok, Addr} ->
            [Addr];
        {error, _} ->
            Timer = inet:start_timer(Timeout),
            R = erlang:apply(inet_db:tcp_module(), getaddrs, [Host, Timer]),
            _ = inet:stop_timer(Timer),
            case R of
                {ok, Addrs} ->
                    Addrs;
                {error, _} ->
                    exit({'invalid host address', Host})
            end
    end.
