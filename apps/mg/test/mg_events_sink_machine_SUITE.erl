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

-module(mg_events_sink_machine_SUITE).
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups        /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([add_events_test                   /1]).
-export([get_unexisted_event_test          /1]).
-export([not_idempotent_add_get_events_test/1]).

%% Pulse
-export([handle_beat/2]).

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
        {main, [sequence], [
            add_events_test,
            get_unexisted_event_test,
            not_idempotent_add_get_events_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_events_sink_machine, '_', '_'}, x),
    Apps = mg_ct_helper:start_applications([consuela, mg]),
    Pid = start_event_sink(event_sink_options()),
    true = erlang:unlink(Pid),
    {Events, _} = mg_events:generate_events_with_range([{#{}, Body} || Body <- [1, 2, 3]], undefined),
    [{apps, Apps}, {pid, Pid}, {events, Events}| C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    true = erlang:exit(?config(pid, C), kill),
    mg_ct_helper:stop_applications(?config(apps, C)).


%%
%% tests
%%
-define(ES_ID, <<"event_sink_id">>).
-define(SOURCE_NS, <<"source_ns">>).
-define(SOURCE_ID, <<"source_id">>).

-spec add_events_test(config()) ->
    _.
add_events_test(C) ->
    ?assertEqual(ok, add_events(C)).

-spec get_unexisted_event_test(config()) ->
    _.
get_unexisted_event_test(_C) ->
    [] = mg_events_sink_machine:get_history(event_sink_options(), ?ES_ID, {42, undefined, forward}).

-spec not_idempotent_add_get_events_test(config()) ->
    _.
not_idempotent_add_get_events_test(C) ->
    ?assertEqual(ok, add_events(C)),
    ConfigEvents =
        [
            #{event => Event, source_ns => ?SOURCE_NS, source_id => ?SOURCE_ID}
            || Event <- ?config(events, C)
        ],
    ExpectedEvents = lists:zip(
        lists:seq(1, erlang:length(?config(events, C)) * 2),
        ConfigEvents ++ ConfigEvents
    ),
    ?assertEqual(ExpectedEvents, get_history(C)).

%%
%% utils
%%

-spec add_events(config()) ->
    _.
add_events(C) ->
    mg_events_sink_machine:add_events(event_sink_options(), ?SOURCE_NS, ?SOURCE_ID,
        ?config(events, C), null, mg_deadline:default()).

-spec get_history(config()) ->
    _.
get_history(_C) ->
    HRange = {undefined, undefined, forward},
    % _ = ct:pal("~p", [PreparedEvents]),
    EventsSinkEvents = mg_events_sink_machine:get_history(event_sink_options(), ?ES_ID, HRange),
    [{ID, Body} || #{id := ID, body := Body} <- EventsSinkEvents].

-spec start_event_sink(mg_events_sink_machine:ns_options()) ->
    pid().
start_event_sink(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_events_sink_machine:child_spec(Options, event_sink)]
        )
    ).

-spec event_sink_options() ->
    mg_events_sink_machine:ns_options().
event_sink_options() ->
    #{
        name                   => machine,
        machine_id             => ?ES_ID,
        namespace              => ?ES_ID,
        storage                => mg_storage_memory,
        worker                 => #{registry => mg_procreg_gproc},
        pulse                  => ?MODULE,
        duplicate_search_batch => 1000,
        events_storage         => mg_storage_memory
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
