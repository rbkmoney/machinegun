%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_force_task_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([test_timeout/1]).

%% mg_machine
-behaviour(mg_machine).
-export([pool_child_spec/2, process_machine/7]).

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
       test_timeout
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

-spec test_timeout(config()) ->
    _.
test_timeout(_C) ->
    BinTestName = genlib:to_binary(expired_task),
    NS = BinTestName,
    ID = BinTestName,
    Options = automaton_options(NS),
    _  = start_automaton(Options),

    ok = mg_machine:start(Options, ID, <<"normal">>, ?req_ctx, mg_utils:default_deadline()),
    R  = mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline()),
    ok.

%%
%% processor
%%
-spec pool_child_spec(_Options, atom()) ->
    supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id    => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result() | no_return().
process_machine(_, _, {init, Mode}, _, ?req_ctx, _, null) ->
    {{reply, ok}, build_timer(init), [Mode, 0]};
process_machine(_, _, {call, get}, _, ?req_ctx, _, [_Mode, Counter] = State) ->
    {{reply, Counter}, build_timer(call), State};
process_machine(_, _, {call, {set_mode, NewMode}}, _, ?req_ctx, _, [_Mode, Counter]) ->
    {{reply, ok}, build_timer(call), [NewMode, Counter]};
process_machine(_, _, timeout, _, ?req_ctx, _, [<<"normal">>, _Counter] = State) ->
    {{reply, ok}, build_timer(timeout), State};
process_machine(_, _, timeout, _, ?req_ctx, _, [<<"counting">> = Mode, Counter]) ->
    {{reply, ok}, build_timer(timeout), [Mode, Counter + 1]};
process_machine(_, _, timeout, _, ?req_ctx, _, [<<"failing">>, _Counter]) ->
    erlang:throw({transient, oops}).

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
            timers         => #{ interval => 100 },
            timers_retries => #{ interval => 100 },
            overseer       => #{ interval => 100 }
        }
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

-spec build_timer(any()) ->
    mg_machine:processor_flow_action().
build_timer(init) ->
    {wait, genlib_time:unow() + 1, ?req_ctx, 5000};
build_timer(_) ->
    {wait, genlib_time:unow() - 1, ?req_ctx, 5000}.
