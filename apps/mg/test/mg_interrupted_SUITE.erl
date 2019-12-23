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

-module(mg_interrupted_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([interrupted_machines_resumed/1]).

%% mg_machine
-behaviour(mg_machine).
-export([process_machine/7, process_repair/6]).

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
       interrupted_machines_resumed
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%%
%% tests
%%
-define(init_args , <<"normies">>).
-define(req_ctx   , <<"req_ctx">>).

-spec interrupted_machines_resumed(config()) ->
    _.
interrupted_machines_resumed(_C) ->
    NS = genlib:to_binary(?FUNCTION_NAME),
    {ok, StoragePid} = mg_storage_memory:start_link(#{name => ?MODULE}),
    true = erlang:unlink(StoragePid),
    Options = automaton_options(NS, ?MODULE),

    N = 8,
    Runtime = 1000,
    Answer = 42,

    Pid1 = start_automaton(Options),
    IDs = [genlib:to_binary(I) || I <- lists:seq(1, N)],
    _ = [
        begin
            ok = mg_machine:start(Options, ID, ?init_args, ?req_ctx, mg_deadline:default()),
            ?assertEqual(undefined, mg_machine:call(Options, ID, answer, ?req_ctx, mg_deadline:default())),
            ?assertEqual(ok, mg_machine:call(Options, ID, {run, Runtime, Answer}, ?req_ctx, mg_deadline:default()))
        end
    || ID <- IDs],
    ok = stop_automaton(Pid1),

    Pid2 = start_automaton(Options),
    ok = timer:sleep(Runtime * 2),
    _ = [
        ?assertEqual(Answer, mg_machine:call(Options, ID, answer, ?req_ctx, mg_deadline:default()))
    || ID <- IDs],
    ok = stop_automaton(Pid2),

    ok = proc_lib:stop(StoragePid).

%%
%% processor
%%
-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result() | no_return().
process_machine(_, _, {init, ?init_args}, _, ?req_ctx, _, null) ->
    {{reply, ok}, sleep, #{}};
process_machine(_, _, {call, {run, Runtime, Answer}}, _, ?req_ctx, _, State) ->
    {{reply, ok}, {continue, #{}}, State#{<<"run">> => [Runtime, Answer]}};
process_machine(_, _, continuation, _, ?req_ctx, _, #{<<"run">> := [Runtime, Answer]}) ->
    ok = timer:sleep(Runtime),
    {noreply, sleep, #{<<"answer">> => Answer}};
process_machine(_, _, {call, answer}, _, ?req_ctx, _, State) ->
    {{reply, maps:get(<<"answer">>, State, undefined)}, sleep, State}.

-spec process_repair(Options, ID, Args, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg:id(),
    Args :: term(),
    ReqCtx :: mg_machine:request_context(),
    Deadline :: mg_deadline:deadline(),
    MachineState :: mg_machine:machine_state(),
    Result :: mg_machine:processor_repair_result().
process_repair(_Options, _ID, _Args, _ReqCtx, _Deadline, State) ->
    {ok, {{reply, ok}, sleep, State}}.

%%
%% utils
%%
-spec start_automaton(mg_machine:options()) ->
    pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_machine:start_link(Options)).

-spec stop_automaton(pid()) ->
    ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid).

-spec automaton_options(mg:ns(), mg_storage:name()) ->
    mg_machine:options().
automaton_options(NS, StorageName) ->
    Scheduler = #{
        min_scan_interval => 1000,
        target_cutoff     => 15
    },
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_ct_helper:build_storage(NS, {mg_storage_memory, #{
            existing_storage_name => StorageName
        }}),
        worker    => #{
            registry => mg_procreg_gproc
        },
        pulse     => ?MODULE,
        schedulers => #{
            overseer => Scheduler
        }
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, {squad, _}) ->
    ok;
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
