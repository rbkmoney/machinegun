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
    ok = mg_machine:start(Options, ID, {TestKey, 0}),
    0  = mg_machine:call (Options, ID, get         ),
    ok = mg_machine:call (Options, ID, increment   ),
    1  = mg_machine:call (Options, ID, get         ),
    ok.


%%
%% processor
%%
-spec process_machine(_Options, mg:id(), mg_machine:processor_impact(), _, mg_machine:machine_state()) ->
    mg_machine:processor_result().
process_machine(_, _, {init, {TestKey, TestValue}}, _, null) ->
    {{reply, ok}, wait, [TestKey, TestValue]};
process_machine(_, _, {call, get}, _, [TestKey, TestValue]) ->
    {{reply, TestValue}, wait, [TestKey, TestValue]};
process_machine(_, _, {call, increment}, _, [TestKey, TestValue]) ->
    {{reply, ok}, wait, [TestKey, TestValue + 1]}.

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
