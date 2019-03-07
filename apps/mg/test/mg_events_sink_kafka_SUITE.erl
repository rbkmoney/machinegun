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

-module(mg_events_sink_kafka_SUITE).
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups        /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([add_events_test                   /1]).

%% Pulse
-export([handle_beat/2]).

-define(TOPIC, <<"test_event_sink">>).
-define(SOURCE_NS, <<"source_ns">>).
-define(SOURCE_ID, <<"source_id">>).
-define(BROKERS, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]).
-define(CLIENT, mg_kafka_client).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        {group, main}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {main, [], [
            add_events_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_events_sink_kafka, '_', '_'}, x),
    AppSpecs = [
        {brod, [
            {clients, [
                {?CLIENT, [
                    {endpoints, ?BROKERS},
                    {auto_start_producers, true}
                ]}
            ]}
        ]},
        {mg, []}
    ],
    Apps = lists:flatten([genlib_app:start_application_with(App, AppConf) || {App, AppConf} <- AppSpecs]),
    {Events, _} = mg_events:generate_events_with_range([{#{}, Body} || Body <- [1, 2, 3]], undefined),
    [{apps, Apps}, {events, Events}| C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- lists:reverse(?config(apps, C))].

%%
%% tests
%%

-spec add_events_test(config()) ->
    _.
add_events_test(C) ->
    ?assertEqual(ok, add_events(C)),
    ?assertEqual([], [{?SOURCE_NS, ?SOURCE_ID, E} || E <- ?config(events, C)] -- read_all_events()).

%%
%% utils
%%

-spec add_events(config()) ->
    ok.
add_events(C) ->
    F = fun() ->
        mg_events_sink_kafka:add_events(
            event_sink_options(),
            ?SOURCE_NS,
            ?SOURCE_ID,
            ?config(events, C),
            null,
            mg_utils:default_deadline()
        )
    end,
    call_with_retry(F, mg_retry:new_strategy({linear, 10, 500})).

-spec read_all_events() ->
    ok.
read_all_events() ->
    {ok, PartitionsCount} = brod:get_partitions_count(?CLIENT, ?TOPIC),
    do_read_all(?BROKERS, ?TOPIC, PartitionsCount - 1, 0, []).

-spec do_read_all([brod:endpoint()], brod:topic(), brod:partition(), brod:offset(), [mg_event:event()]) ->
    ok.
do_read_all(_Hosts, _Topic, Partition, _Offset, Result) when Partition < 0 ->
    lists:reverse(Result);
do_read_all(Hosts, Topic, Partition, Offset, Result) ->
    case brod:fetch(Hosts, Topic, Partition, Offset) of
        {ok, {Offset, []}} ->
            do_read_all(Hosts, Topic, Partition - 1, Offset, Result);
        {ok, {NewOffset, Records}} when NewOffset =/= Offset ->
            NewRecords = lists:reverse([
                erlang:binary_to_term(Value) || #kafka_message{value = Value} <- Records
            ]),
            do_read_all(Hosts, Topic, Partition, NewOffset, NewRecords ++ Result)
    end.

-spec event_sink_options() ->
    mg_events_sink_machine:ns_options().
event_sink_options() ->
    #{
        name    => kafka,
        client  => ?CLIENT,
        topic   => ?TOPIC,
        pulse   => ?MODULE,
        encoder => fun(NS, ID, Event) ->
            erlang:term_to_binary({NS, ID, Event})
        end
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

-spec call_with_retry(fun(() -> Result), mg_retry:strategy()) ->
    Result.
call_with_retry(Fun, Strategy) ->
    try
        Fun()
    catch
        throw:(Reason = {transient, _Details}) ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NewStrategy} ->
                    ok = timer:sleep(Timeout),
                    call_with_retry(Fun, NewStrategy);
                finish ->
                    erlang:throw(Reason)
            end
    end.
