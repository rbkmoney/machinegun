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
-export([process_machine/5]).

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
    % dbg:tpl({mg_workers_manager, '_', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

%%
%% tests
%%
-spec simple_test(config()) ->
    _.
simple_test(_) ->
    Options = automaton_options(),
    TestKey = <<"test_key">>,
    ID = <<"42">>,
    _  = start_automaton(Options),
    ok = mg_machine:start(Options, ID, {TestKey, 0}     , mg_utils:default_deadline()),
    0  = mg_machine:call (Options, ID, get              , mg_utils:default_deadline()),
    ok = mg_machine:call (Options, ID, increment        , mg_utils:default_deadline()),
    1  = mg_machine:call (Options, ID, get              , mg_utils:default_deadline()),
    ok = mg_machine:call (Options, ID, delayed_increment, mg_utils:default_deadline()),
    ok = timer:sleep(2000),
    2  = mg_machine:call (Options, ID, get              , mg_utils:default_deadline()),

    % fail-simple_repair
    machine_failed = (catch mg_machine:call(Options, ID, fail, mg_utils:default_deadline())),
    ok = mg_machine:simple_repair(Options, ID, mg_utils:default_deadline()),
    2  = mg_machine:call(Options, ID, get, mg_utils:default_deadline()),

    % fail-repair
    machine_failed = (catch mg_machine:call(Options, ID, fail, mg_utils:default_deadline())),
    repaired = mg_machine:repair(Options, ID, repair_arg, mg_utils:default_deadline()),
    2  = mg_machine:call(Options, ID, get, mg_utils:default_deadline()),
    ok.

%%
%% processor
%%
-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, mg_machine:machine_state()) ->
    mg_machine:processor_result() | no_return().
process_machine(_, _, {init, {TestKey, TestValue}}, _, null) ->
    {{reply, ok}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, get}, _, [TestKey, TestValue]) ->
    {{reply, TestValue}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, increment}, _, [TestKey, TestValue]) ->
    {{reply, ok}, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {call, delayed_increment}, _, State) ->
    {{reply, ok}, {wait, 1}, State};
process_machine(_, _, timeout, _, [TestKey, TestValue]) ->
    {noreply, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {call, fail}, _, [TestKey, TestValue]) ->
    _ = exit(1),
    {{reply, ok}, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {repair, repair_arg}, _, [TestKey, TestValue]) ->
    {{reply, repaired}, sleep, [TestKey, TestValue]}.

%%
%% utils
%%
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
        storage   => mg_storage_memory
    }.
