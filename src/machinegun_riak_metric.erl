%%%
%%% Copyright 2020 RBKmoney
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

-module(machinegun_riak_metric).
-behaviour(gen_server).

%% API

-export([setup/0]).
-export([start_link/1]).
-export([child_spec/3]).

%% gen_server callbacks

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% pooler callbacks

-export([update_or_create/4]).

%% Types

-type options() :: #{
    namespace := mg_core:ns(),
    type := storage_type(),
    interval => timeout()
}.
-export_type([options/0]).

%% Internal types

-record(state, {
    interval :: timeout(),
    namespace :: mg_core:ns(),
    storage_type :: storage_type(),
    storage :: storage(),
    timer = undefined :: undefined | reference()
}).

-type state() :: #state{}.
-type storage() :: mg_core_storage:options().
-type storage_type() :: atom().
% -type metric() :: how_are_you:metric().
% -type metric_key() :: how_are_you:metric_key().
% -type metric_value() :: how_are_you:metric_value().
% -type metrics() :: [metric()].
-type bin_type() :: machinegun_hay_utils:bin_type().
-type pooler_metrics() :: [{atom(), number()}].

-type pooler_metric_type() :: counter | histogram | history | meter.

%% API

-spec child_spec(options(), storage(), term()) ->
    supervisor:child_spec().
child_spec(Options, Storage, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [{Options, Storage}]},
        restart => permanent,
        type => worker
    }.

-spec start_link({options(), storage()}) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%% Sets all metrics up. Call this when the app starts.
-spec setup() ->
    ok.
setup() ->
    % Pool utilization metrics
    true = prometheus_gauge:declare([
        {name, mg_riak_pool_connections_limit},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "The limit of the Machinegun riak pool connections number."}
    ]),
    true = prometheus_gauge:declare([
        {name, mg_riak_pool_connections_in_use},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "The number of used Machinegun riak pool connections."}
    ]),
    true = prometheus_gauge:declare([
        {name, mg_riak_pool_connections_free},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "The number of free Machinegun riak pool connections."}
    ]),
    true = prometheus_gauge:declare([
        {name, mg_riak_pool_queued_requests},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "The number of queued requests in the Machingun riak pool."}
    ]),
    true = prometheus_gauge:declare([
        {name, mg_riak_pool_queued_requests_limit},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "The limit of queued requests in the Machingun riak pool."}
    ]),
    % Handler metrics
    true = prometheus_histogram:declare([
        {name, mg_riak_pool_connections_in_use_per_request},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, size_buckets()},
        {help, "The number of used Machinegun riak pool connections per request."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_riak_pool_connections_free_per_request},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, size_buckets()},
        {help, "The number of free Machinegun riak pool connections per request."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_riak_pool_queued_requests_per_request},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, size_buckets()},
        {help, "The number of queued requests in Machinegun riak pool per request."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_no_free_connection_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of no free connection errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_connect_timeout_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of connect timeout errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_queue_limit_reached_errors_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of queue limit reached errors in Machinegun riak pool."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_killed_free_connections_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of killed free Machinegun riak pool connections."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_riak_pool_killed_in_use_connections_total},
        {registry, registry()},
        {labels, [namespace, name]},
        {help, "Total number of killed used Machinegun riak pool connections."}
    ]),
    ok.

%% genserver callbacks

-spec init({options(), storage()}) -> {ok, state()}.
init({Options, Storage}) ->
    State = #state{
        interval = maps:get(interval, Options, 10 * 1000),
        namespace = maps:get(namespace, Options),
        storage_type = maps:get(type, Options),
        storage = Storage
    },
    {ok, start_timer(State)}.

-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(timeout, State0) ->
    State = restart_timer(State0),
    ok = process_metrics(State),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% pooler callbacks

-spec update_or_create([binary()], number(), pooler_metric_type(), []) ->
    ok.
update_or_create(Key, Value, counter, []) ->
    {ok, {NS, Type, MetricName}} = decode_key(Key),
    ok = hay_metrics:push(create_hay_inc(rebuild_hay_key(NS, Type, MetricName), Value)),
    ok = dispatch_prometheus_metric(NS, Type, MetricName, Value);
update_or_create(_Key, _Value, meter, []) ->
    ok;
update_or_create(_Key, _Value, history, []) ->
    ok;
update_or_create(Key, Value, histogram, []) ->
    {ok, {NS, Type, MetricName}} = decode_key(Key),
    ok = hay_metrics:push(create_hay_bin_inc(rebuild_hay_key(NS, Type, MetricName), queue_length, Value)),
    ok = dispatch_prometheus_metric(NS, Type, MetricName, Value);
update_or_create(Key, Value, Type, []) ->
    logger:warning("Unexpected pool metric ~p ~p=~p", [Type, Key, Value]).

%% internal


-spec restart_timer(state()) -> state().
restart_timer(State = #state{timer = undefined}) ->
    start_timer(State);

restart_timer(State = #state{timer = TimerRef}) ->
    _ = erlang:cancel_timer(TimerRef),
    start_timer(State#state{timer = undefined}).

-spec start_timer(state()) -> state().
start_timer(State = #state{timer = undefined, interval = Interval}) ->
    State#state{timer = erlang:send_after(Interval, self(), timeout)}.

-spec process_metrics(state()) -> ok.
process_metrics(State) ->
    Metrics = gather_metrics(State),
    ok = push_hay_metrics(State, Metrics),
    ok = push_prometheus_metrics(State, Metrics),
    ok.

-spec gather_metrics(state()) -> pooler_metrics().
gather_metrics(#state{storage = Storage} = State) ->
    {mg_core_storage_riak, StorageOptions} = mg_core_utils:separate_mod_opts(Storage),
    case mg_core_storage_riak:pool_utilization(StorageOptions) of
        {ok, Metrics} ->
            Metrics;
        {error, Reason} ->
            Namespace = State#state.namespace,
            StorageType = State#state.storage_type,
            logger:warning("Can not gather ~p ~p riak pool utilization: ~p", [Namespace, StorageType, Reason]),
            []
    end.

-spec push_hay_metrics(state(), pooler_metrics()) ->
    ok.
push_hay_metrics(#state{namespace = NS, storage_type = Type}, Metrics) ->
    KeyPrefix = [mg, storage, NS, Type, pool],
    HayMetrics = [how_are_you:metric_construct(gauge, [KeyPrefix, Key], Value) || {Key, Value} <- Metrics],
    machinegun_hay_utils:push(HayMetrics).

-spec push_prometheus_metrics(state(), pooler_metrics()) ->
    ok.
push_prometheus_metrics(#state{namespace = NS, storage_type = Type}, Metrics) ->
    lists:foreach(
        fun({Key, Value}) ->
            ok = dispatch_prometheus_utilization_metric(NS, Type, Key, Value)
        end,
        Metrics
    ).

%% see https://github.com/seth/pooler/blob/9c28fb479f9329e2a1644565a632bc222780f1b7/src/pooler.erl#L946
%% for metric details
-spec dispatch_prometheus_utilization_metric(mg_core:ns(), storage_type(), atom(), number()) -> ok.
dispatch_prometheus_utilization_metric(NS, Type, max_count, Value) ->
    ok = set(mg_riak_pool_connections_limit, [NS, Type], Value);
dispatch_prometheus_utilization_metric(NS, Type, in_use_count, Value) ->
    ok = set(mg_riak_pool_connections_in_use, [NS, Type], Value);
dispatch_prometheus_utilization_metric(NS, Type, free_count, Value) ->
    ok = set(mg_riak_pool_connections_free, [NS, Type], Value);
dispatch_prometheus_utilization_metric(NS, Type, queue_max, Value) ->
    ok = set(mg_riak_pool_queued_requests_limit, [NS, Type], Value);
dispatch_prometheus_utilization_metric(NS, Type, queued_count, Value) ->
    ok = set(mg_riak_pool_queued_requests, [NS, Type], Value);
dispatch_prometheus_utilization_metric(_NS, _Type, _Other, _Value) ->
    ok.

-spec dispatch_prometheus_metric(mg_core:ns(), storage_type(), binary(), number()) -> ok.
dispatch_prometheus_metric(NS, Type, <<"in_use_count">>, Value) ->
    ok = observe(mg_riak_pool_connections_in_use_per_request, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"free_count">>, Value) ->
    ok = observe(mg_riak_pool_connections_free_per_request, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"queue_count">>, Value) ->
    ok = observe(mg_riak_pool_queued_requests_per_request, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"error_no_members_count">>, Value) ->
    ok = inc(mg_riak_pool_no_free_connection_errors_total, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"starting_member_timeout">>, Value) ->
    ok = inc(mg_riak_pool_connect_timeout_errors_total, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"queue_max_reached">>, Value) ->
    ok = inc(mg_riak_pool_queue_limit_reached_errors_total, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"killed_free_count">>, Value) ->
    ok = inc(mg_riak_pool_killed_free_connections_total, [NS, Type], Value);
dispatch_prometheus_metric(NS, Type, <<"killed_in_use_count">>, Value) ->
    ok = inc(mg_riak_pool_killed_in_use_connections_total, [NS, Type], Value);
dispatch_prometheus_metric(_NS, _Type, _Other, _Value) ->
    ok.

-spec create_hay_inc(how_are_you:metric_key(), non_neg_integer()) ->
    how_are_you:metric().
create_hay_inc(Key, Number) ->
    machinegun_hay_utils:create_inc(Key, Number).

-spec create_hay_bin_inc(how_are_you:metric_key(), bin_type(), number()) ->
    how_are_you:metric().
create_hay_bin_inc(KeyPrefix, BinType, Value) ->
    machinegun_hay_utils:create_bin_inc(KeyPrefix, BinType, Value).

%% see https://github.com/seth/pooler/blob/9c28fb479f9329e2a1644565a632bc222780f1b7/src/pooler.erl#L877
%% for key format details
-spec decode_key([binary()]) ->
    {ok, {mg_core:ns(), storage_type(), binary()}}.
decode_key([<<"pooler">>, PoolName, MetricName]) ->
    {ok, {NS, Type}} = try_decode_pool_name(PoolName),
    {ok, {NS, Type, MetricName}}.

-spec rebuild_hay_key(mg_core:ns(), storage_type(), binary()) ->
    how_are_you:metric_key().
rebuild_hay_key(NS, Type, MetricName) ->
    [mg, storage, NS, Type, pool, MetricName].

-spec try_decode_pool_name(binary()) ->
    {ok, {mg_core:ns(), storage_type()}} | {error, _Details}.
try_decode_pool_name(PoolName) ->
    %% TODO: Try to pass options through `pooler` metric mod option instead of pool name parsing
    try erlang:binary_to_term(base64:decode(PoolName), [safe]) of
        {NS, Module, Type} when is_binary(NS), is_atom(Type), is_atom(Module) ->
            {ok, {NS, Type}};
        Other ->
            {error, {unexpected_name_format, Other}}
    catch
        error:Error ->
            {error, {not_bert, Error, PoolName}}
    end.

-spec inc(prometheus_metric:name(), [term()], number()) ->
    ok.
inc(Name, Labels, Value) ->
    _ = prometheus_counter:inc(registry(), Name, Labels, Value),
    ok.

-spec set(prometheus_metric:name(), [term()], number()) ->
    ok.
set(Name, Labels, Value) ->
    _ = prometheus_gauge:set(registry(), Name, Labels, Value),
    ok.

-spec observe(prometheus_metric:name(), [term()], number()) ->
    ok.
observe(Name, Labels, Value) ->
    _ = prometheus_histogram:observe(registry(), Name, Labels, Value),
    ok.

-spec registry() ->
    prometheus_registry:registry().
registry() ->
    default.

-spec size_buckets() ->
    [number()].
size_buckets() ->
    [
        1,
        5,
        10,
        20,
        50,
        100,
        250,
        500,
        1000,
        5000,
        10000
    ].
