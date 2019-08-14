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

-module(mg_instant_timer_task_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([instant_start_test/1]).
-export([without_shedulers_test/1]).

%% mg_machine
-behaviour(mg_machine).
-export([pool_child_spec/2]).
-export([process_machine/7]).

-export([start/0]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()] | {group, atom()}.
all() ->
    [
       instant_start_test,
       without_shedulers_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine, '_', '_'}, x),
    Apps = mg_ct_helper:start_applications([mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%%
%% tests
%%
-define(req_ctx, <<"req_ctx">>).

-spec instant_start_test(config()) ->
    _.
instant_start_test(_C) ->
    NS = <<"test">>,
    ID = <<"machine">>,
    Options = automaton_options(NS),
    _  = start_automaton(Options),

    ok = mg_machine:start(Options, ID, 0, ?req_ctx, mg_utils:default_deadline()),
     0 = mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline()),
    ok = mg_machine:call(Options, ID, force_timeout, ?req_ctx, mg_utils:default_deadline()),
    F = fun() ->
            mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline())
        end,
    mg_ct_helper:assert_wait_expected(1, F, mg_retry:new_strategy({linear, _Retries = 10, _Timeout = 100})).

-spec without_shedulers_test(config()) ->
    _.
without_shedulers_test(_C) ->
    NS = <<"test">>,
    ID = <<"machine">>,
    Options = automaton_options_wo_shedulers(NS),
    _  = start_automaton(Options),

    ok = mg_machine:start(Options, ID, 0, ?req_ctx, mg_utils:default_deadline()),
     0 = mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline()),
    ok = mg_machine:call(Options, ID, force_timeout, ?req_ctx, mg_utils:default_deadline()),
    % machine is still alive
    _  = mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline()).

%%
%% processor
%%
-record(machine_state, {
    counter = 0 :: integer(),
    timer = undefined :: {genlib_time:ts(), mg_machine:request_context()} | undefined
}).
-type machine_state() :: #machine_state{}.

-spec pool_child_spec(_Options, atom()) ->
    supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id    => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg:id(),
    Impact :: mg_machine:processor_impact(),
    PCtx :: mg_machine:processing_context(),
    ReqCtx :: mg_machine:request_context(),
    Deadline :: mg_deadline:deadline(),
    MachineState :: mg_machine:machine_state(),
    Result :: mg_machine:processor_result().
process_machine(_, _, Impact, _, ReqCtx, _, EncodedState) ->
    State = decode_state(EncodedState),
    {Reply, Action, NewState} = do_process_machine(Impact, ReqCtx, State),
    {Reply, try_set_timer(NewState, Action), encode_state(NewState)}.

-spec do_process_machine(mg_machine:processor_impact(), mg_machine:request_context(), machine_state()) ->
    mg_machine:processor_result().
do_process_machine({init, Counter}, ?req_ctx, State) ->
    {{reply, ok}, sleep, State#machine_state{counter = Counter}};
do_process_machine({call, get}, ?req_ctx, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter is ~p", [Counter]),
    {{reply, Counter}, sleep, State};
do_process_machine({call, force_timeout}, ?req_ctx = ReqCtx, State) ->
    TimerTarget = genlib_time:unow(),
    {{reply, ok}, sleep, State#machine_state{timer = {TimerTarget, ReqCtx}}};
do_process_machine(timeout, ?req_ctx, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter updated to ~p", [Counter]),
    {{reply, ok}, sleep, State#machine_state{counter = Counter + 1, timer = undefined}}.

-spec encode_state(machine_state()) -> mg_machine:machine_state().
encode_state(#machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}}) ->
    [Counter, TimerTarget, ReqCtx];
encode_state(#machine_state{counter = Counter, timer = undefined}) ->
    [Counter].

-spec decode_state(mg_machine:machine_state()) -> machine_state().
decode_state(null) ->
    #machine_state{};
decode_state([Counter, TimerTarget, ReqCtx]) ->
    #machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}};
decode_state([Counter]) ->
    #machine_state{counter = Counter}.

-spec try_set_timer(machine_state(), mg_machine:processor_flow_action()) ->
    mg_machine:processor_flow_action().
try_set_timer(#machine_state{timer = {TimerTarget, ReqCtx}}, sleep) ->
    {wait, TimerTarget, ReqCtx, 5000};
try_set_timer(#machine_state{timer = undefined}, Action) ->
    Action.

%%
%% utils
%%
-spec start()->
    ignore.
start() ->
    ignore.

-spec start_automaton(mg_machine:options()) ->
    pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_machine:start_link(Options)).

-spec automaton_options(mg:ns()) ->
    mg_machine:options().
automaton_options(NS) ->
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_storage_memory,
        pulse     => ?MODULE,
        schedulers => #{
            timers         => #{ interval => timer:hours(1) },
            timers_retries => #{ interval => timer:hours(1) },
            overseer       => #{ interval => timer:hours(1) }
        }
    }.

-spec automaton_options_wo_shedulers(mg:ns()) ->
    mg_machine:options().
automaton_options_wo_shedulers(NS) ->
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_storage_memory,
        pulse     => ?MODULE,
        schedulers => #{
        }
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
