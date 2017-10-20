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

-module(mg_machine_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([simple_test/1]).

%% mg_machine
-behaviour(mg_machine).
-export([pool_child_spec/2, process_machine/6]).

-export([start/0]).

%% logger
-export([handle_machine_logging_event/2]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
       simple_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine, '_', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

%%
%% tests
%%
-define(req_ctx, <<"req_ctx">>).

-spec simple_test(config()) ->
    _.
simple_test(_) ->
    Options = automaton_options(),
    TestKey = <<"test_key">>,
    ID = <<"42">>,
    _  = start_automaton(Options),

    machine_not_found = (catch mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline())),

    ok = mg_machine:start(Options, ID, {TestKey, 0}, ?req_ctx, mg_utils:default_deadline()),
    machine_already_exist = (catch mg_machine:start(Options, ID, {TestKey, 0}, ?req_ctx, mg_utils:default_deadline())),

    0  = mg_machine:call (Options, ID, get              , ?req_ctx, mg_utils:default_deadline()),
    ok = mg_machine:call (Options, ID, increment        , ?req_ctx, mg_utils:default_deadline()),
    1  = mg_machine:call (Options, ID, get              , ?req_ctx, mg_utils:default_deadline()),
    ok = mg_machine:call (Options, ID, delayed_increment, ?req_ctx, mg_utils:default_deadline()),
    ok = timer:sleep(2000),
    2  = mg_machine:call (Options, ID, get              , ?req_ctx, mg_utils:default_deadline()),

    % call fail/simple_repair
    machine_failed = (catch mg_machine:call         (Options, ID, fail, ?req_ctx, mg_utils:default_deadline())),
    ok             =        mg_machine:simple_repair(Options, ID,       ?req_ctx, mg_utils:default_deadline()),
    2              =        mg_machine:call         (Options, ID, get , ?req_ctx, mg_utils:default_deadline()),

    % call fail/repair
    machine_failed = (catch mg_machine:call  (Options, ID, fail      , ?req_ctx, mg_utils:default_deadline())),
    repaired       =        mg_machine:repair(Options, ID, repair_arg, ?req_ctx, mg_utils:default_deadline()),
    2              =        mg_machine:call  (Options, ID, get       , ?req_ctx, mg_utils:default_deadline()),

    % call fail/repair fail/repair
    machine_failed = (catch mg_machine:call(Options, ID, fail, ?req_ctx, mg_utils:default_deadline())),
    machine_failed = (catch mg_machine:repair(Options, ID, fail, ?req_ctx, mg_utils:default_deadline())),
    repaired = mg_machine:repair(Options, ID, repair_arg, ?req_ctx, mg_utils:default_deadline()),
    machine_already_working = (catch mg_machine:repair(Options, ID, repair_arg, ?req_ctx, mg_utils:default_deadline())),
    2  = mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline()),

    ok  = mg_machine:call(Options, ID, remove, ?req_ctx, mg_utils:default_deadline()),

    machine_not_found = (catch mg_machine:call(Options, ID, get, ?req_ctx, mg_utils:default_deadline())),

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

-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result() | no_return().
process_machine(_, _, {_, fail}, _, ?req_ctx, _) ->
    _ = exit(1),
    {noreply, sleep, []};
process_machine(_, _, {init, {TestKey, TestValue}}, _, ?req_ctx, null) ->
    {{reply, ok}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, get}, _, ?req_ctx, [TestKey, TestValue]) ->
    {{reply, TestValue}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, increment}, _, ?req_ctx, [TestKey, TestValue]) ->
    {{reply, ok}, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {call, delayed_increment}, _, ?req_ctx, State) ->
    {{reply, ok}, {wait, genlib_time:now() + 1, ?req_ctx, 5000}, State};
process_machine(_, _, {call, remove}, _, ?req_ctx, State) ->
    {{reply, ok}, remove, State};
process_machine(_, _, timeout, _, ?req_ctx, [TestKey, TestValue]) ->
    {noreply, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {repair, repair_arg}, _, ?req_ctx, [TestKey, TestValue]) ->
    {{reply, repaired}, sleep, [TestKey, TestValue]}.

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

-spec automaton_options() ->
    mg_machine:options().
automaton_options() ->
    #{
        namespace => <<"test">>,
        processor => ?MODULE,
        storage   => mg_storage_memory,
        logger    => ?MODULE,
        scheduled_tasks => #{
            timers   => #{ interval => 1000, limit => 10 },
            overseer => #{ interval => 1000, limit => 10 }
        }
    }.

-spec handle_machine_logging_event(_, mg_machine_logger:event()) ->
    ok.
handle_machine_logging_event(_, {NS, ID, ReqCtx, SubEvent}) ->
    ct:pal("[~s:~s:~s] ~p", [NS, ID, ReqCtx, SubEvent]).
