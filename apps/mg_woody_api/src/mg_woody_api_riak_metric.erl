%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_woody_api_riak_metric).
-behaviour(hay_metrics_handler).

%% API

-export([child_spec/3]).

%% pooler callbacks
-export([update_or_create/4]).

%% how_are_you callbacks
-export([init/1]).
-export([get_interval/1]).
-export([gather_metrics/1]).

%% Types

-type options() :: #{
    namespace := mg:ns(),
    type := storage_type(),
    interval => timeout()
}.

-export_type([options/0]).

%% Internal types

-record(state, {
    interval :: timeout(),
    namespace :: mg:ns(),
    storage_type :: storage_type(),
    storage :: storage()
}).
-type state() :: #state{}.

-type storage() :: mg_storage:options().
-type storage_type() :: atom().
-type metric() :: how_are_you:metric().
-type metric_key() :: how_are_you:metric_key().
-type metric_value() :: how_are_you:metric_value().
-type metrics() :: [metric()].
-type bin_type() :: mg_woody_api_metric_utils:bin_type().

-type metric_type() :: counter | histogram | history | meter.

%% API
-spec child_spec(options() | undefined, storage(), term()) ->
    supervisor:child_spec().
child_spec(Options0, Storage, ChildID) ->
    Options = genlib:define(Options0, #{}),
    hay_metrics_handler:child_spec({?MODULE, {Options, Storage}}, ChildID).

%% how_are_you callbacks

-spec init({options(), storage()}) -> {ok, state()}.
init({Options, Storage}) ->
    {ok, #state{
        interval = maps:get(interval, Options, 10 * 1000),
        namespace = maps:get(namespace, Options),
        storage_type = maps:get(type, Options),
        storage = Storage
    }}.

-spec get_interval(state()) -> timeout().
get_interval(#state{interval = Interval}) ->
    Interval.

-spec gather_metrics(state()) -> metrics().
gather_metrics(#state{storage = Storage} = State) ->
    {mg_storage_riak, StorageOptions} = mg_utils:separate_mod_opts(Storage),
    Metrics = mg_storage_riak:pool_utilization(StorageOptions),
    KeyPrefix = build_key_prefix(State),
    [gauge([KeyPrefix, Key], Value) || {Key, Value} <- Metrics].

%% pooler callbacks

-spec update_or_create(metric_key(), metric_value(), metric_type(), []) ->
    ok.
update_or_create(Key, Value, counter, []) ->
    ok = push([create_inc(rebuild_key(Key), Value)]);
update_or_create(_Key, _Value, meter, []) ->
    ok;
update_or_create(_Key, _Value, history, []) ->
    ok;
update_or_create(Key, Value, histogram, []) ->
    ok = push([create_bin_inc(rebuild_key(Key), queue_length, Value)]);
update_or_create(Key, Value, Type, []) ->
    logger:warning("Unexpected pool metric ~p ~p=~p", [Type, Key, Value]).

%% Utils

-spec build_key_prefix(state()) ->
    metric_key().
build_key_prefix(#state{namespace = NS, storage_type = Type}) ->
    [mg, storage, NS, Type, pool].

-spec gauge(metric_key(), metric_value()) ->
    metric().
gauge(Key, Value) ->
    how_are_you:metric_construct(gauge, Key, Value).

-spec push(metrics()) ->
    ok.
push(Metrics) ->
    mg_woody_api_metric_utils:push(Metrics).

-spec create_inc(metric_key(), non_neg_integer()) ->
    metric().
create_inc(Key, Number) ->
    mg_woody_api_metric_utils:create_inc(Key, Number).

-spec create_bin_inc(metric_key(), bin_type(), number()) ->
    metric().
create_bin_inc(KeyPrefix, BinType, Value) ->
    mg_woody_api_metric_utils:create_bin_inc(KeyPrefix, BinType, Value).

%% see https://github.com/seth/pooler/blob/9c28fb479f9329e2a1644565a632bc222780f1b7/src/pooler.erl#L877
%% for key formay details
-spec rebuild_key([binary()]) ->
    metric_key().
rebuild_key([<<"pooler">>, PoolName, MetricName]) ->
    case try_decode_pool_name(PoolName) of
        {ok, {NS, Type}} ->
            [mg, storage, NS, Type, pool, MetricName];
        error ->
            [mg, storage, PoolName, pool, MetricName]
    end;
rebuild_key(Key) ->
    [mg | Key].

-spec try_decode_pool_name(binary()) ->
    {ok, {mg:ns(), storage_type()}} | error.
try_decode_pool_name(PoolName) ->
    %% TODO: Try to pass options through `pooler` metric mod option instead of pool name parsing
    try erlang:binary_to_term(base64:decode(PoolName), [safe]) of
        {NS, Module, Type} when is_binary(NS), is_atom(Type), is_atom(Module) ->
            {ok, {NS, Type}};
        _Other ->
            error
    catch
        error:_Error ->
            error
    end.
