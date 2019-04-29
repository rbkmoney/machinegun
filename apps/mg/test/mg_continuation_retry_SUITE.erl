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

-module(mg_continuation_retry_SUITE).
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups        /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([continuation_delayed_retries_test/1]).

%% mg_machine
-behaviour(mg_machine).
-export([process_machine/7]).

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
            continuation_delayed_retries_test
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
    Apps = mg_ct_helper:start_applications([mg]),
    Pid = start_event_sink(event_sink_options()),
    true = erlang:unlink(Pid),
    [{apps, Apps}, {pid, Pid} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    true = erlang:exit(?config(pid, C), kill),
    mg_ct_helper:stop_applications(?config(apps, C)).


%%
%% tests
%%
-define(TEST_FAIL_COUNT, 3).
-define(TEST_INTERVALS, [100, 200, 300]).
-define(TEST_INTERVAL_THRESHOLD, 99).
-define(REQ_CTX, <<"req_ctx">>).
-define(ES_ID, <<"event_sink_id">>).
-define(MH_ID, <<"42">>).
-define(MH_NS, <<"42_ns">>).
-define(ETS_NS, ?MODULE).

-spec continuation_delayed_retries_test(config()) ->
    _.
continuation_delayed_retries_test(_C) ->
    Options = automaton_options(),
    _  = start_automaton(Options),
    ID = ?MH_ID,
    ok = mg_machine:start(Options, ID, #{},  ?REQ_CTX, mg_utils:default_deadline()),
    ok = mg_machine:call (Options, ID, test, ?REQ_CTX, mg_utils:default_deadline()),
    ok = await_process_done(Options, ID, 100),
    ok = check_event_intervals().

%%
%% processor
%%

-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result() | no_return().
process_machine(_, _, {_, fail}, _, ?REQ_CTX, _, _) ->
    _ = exit(1),
    {noreply, sleep, []};
process_machine(_, _, {init, InitState}, _, ?REQ_CTX, _, null) ->
    _ = ets:new(?ETS_NS, [set, named_table, public]),
    {{reply, ok}, sleep, InitState};
process_machine(_, _, {call, test}, _, ?REQ_CTX, _, State) ->
    true = ets:insert(?ETS_NS, {fail_count, 0}),
    {{reply, ok}, {continue, #{}}, State};
process_machine(_, _, continuation, _, ?REQ_CTX, _, State) ->
    [{fail_count, FailCount}] = ets:lookup(?ETS_NS, fail_count),
    case FailCount of
        ?TEST_FAIL_COUNT ->
            _ = add_event(<<"done">>, FailCount),
            {noreply, sleep, State#{<<"done">> => true}};
        _ ->
            _ = add_event(<<"fail">>, FailCount),
            true = ets:insert(?ETS_NS, {fail_count, FailCount + 1}),
            throw({transient, not_yet})
    end.

%%
%% utils
%%
-spec await_process_done(mg_machine:options(), binary(), timeout()) ->
    ok.
await_process_done(Options, ID, Timeout) ->
    case get_last_event() of
        {_, _, <<"done">>} -> ok;
        _ ->
            timer:sleep(Timeout),
            await_process_done(Options, ID, Timeout)
    end.

-spec check_event_intervals() ->
    ok.
check_event_intervals() ->
    History = get_history(),
    _ = ct:pal("~p", [History]),
    Invervals = count_intervals(History),
    ?assertEqual(true, assert_intervals(?TEST_INTERVALS, Invervals, ?TEST_INTERVAL_THRESHOLD)),
    ok.

-spec assert_intervals(_Target, _Actual, _Threshhold) ->
    boolean().
assert_intervals([H1 | T1], [H2 | T2], Threshhold) when H1 + Threshhold >= H2 ->
    assert_intervals(T1, T2, Threshhold);
assert_intervals([], [], _) ->
    true;
assert_intervals(_, _, _) ->
    false.

-spec count_intervals(_History) ->
    _.
count_intervals([{_, CreatedAt, _} | Rest]) ->
    count_intervals(CreatedAt, Rest, []).

-spec count_intervals(_PrevAt, _History, _Acc) ->
    _.
count_intervals(PrevAt, [{_, CurrentAt, _} | Rest], Acc) ->
    count_intervals(CurrentAt, Rest, Acc ++ [CurrentAt - PrevAt]);
count_intervals(_, [], Acc) ->
    Acc.

-spec add_event(_EventBody, _ID) ->
    _.
add_event(EventBody, ID) ->
    Event = #{
        id         => ID,
        created_at => os:system_time(millisecond),
        body       => {#{}, EventBody}
    },
    mg_events_sink_machine:add_events(event_sink_options(), ?MH_NS, ?MH_ID,
        [Event], null, mg_utils:default_deadline()).

-spec get_last_event() ->
    _.
get_last_event() ->
    case get_history({undefined, 1, backward}) of
        [Event] -> Event;
        [] -> undefined
    end.

-spec get_history() ->
    _.
get_history() ->
    HRange = {undefined, undefined, forward},
    get_history(HRange).

-define(parse_event(ID, CreatedAt, Body),
    #{id := ID, body := #{event := #{created_at := CreatedAt, body := {_, Body}}}}).

-spec get_history(Range::tuple()) ->
    _.
get_history(HRange) ->
    EventsSinkEvents = mg_events_sink_machine:get_history(event_sink_options(), ?MH_ID, HRange),
    [{ID, CreatedAt, Body} || ?parse_event(ID, CreatedAt, Body) <- EventsSinkEvents].

-spec start_event_sink(mg_events_sink_machine:ns_options()) ->
    pid().
start_event_sink(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_events_sink_machine:child_spec(Options, event_sink)]
        )
    ).

-spec start_automaton(mg_machine:options()) ->
    pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_machine:start_link(Options)).

-spec automaton_options() ->
    mg_machine:options().
automaton_options() ->
    #{
        namespace => ?MH_NS,
        processor => ?MODULE,
        storage   => mg_storage_memory,
        pulse     => ?MODULE,
        retries   => #{
            continuation => {intervals, ?TEST_INTERVALS}
        },
        schedulers => #{
            timers         => #{ interval => 1000 },
            timers_retries => #{ interval => 1000 },
            overseer       => #{ interval => 1000 }
        }
    }.

-spec event_sink_options() ->
    mg_events_sink_machine:options().
event_sink_options() ->
    #{
        name                   => machine,
        machine_id             => ?MH_ID,
        namespace              => ?MH_NS,
        storage                => mg_storage_memory,
        pulse                  => ?MODULE,
        duplicate_search_batch => 1000,
        events_storage         => mg_storage_memory
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
