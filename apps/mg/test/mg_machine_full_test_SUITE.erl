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

%%%
%%% Тест, который в течение некоторого времени (5 сек) прогоняет машину через цепочку стейтов.
%%% Логика переходов случайна (но генератор инициализируется от ID машины для воспроизводимости результатов).
%%% Тест ещё нужно доделывать (см TODO).
%%%
-module(mg_machine_full_test_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([full_test/1]).

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
    [test_name()].
all() ->
    [
       full_test
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
-spec full_test(config()) ->
    _.
full_test(_) ->
    Options = automaton_options(),
    AutomatonPid = start_automaton(Options),
    % TODO убрать константы
    Pids =
        lists:map(
            fun(ID) ->
                erlang:spawn_link(fun() -> check_chain(Options, ID), timer:sleep(100) end)
            end,
            lists:seq(1, 10)
        ),
    ok = timer:sleep(5 * 1000),
    mg_utils:stop_wait_all(Pids ++ [AutomatonPid], shutdown, 5000).

%% TODO wait, simple_repair, kill, continuation
-type id() :: pos_integer().
-type seq() :: pos_integer().
-type result() :: ok | failed | already_exist | not_found | already_working.
-type state() :: not_exists | sleeping | failed.
-type flow_action() :: sleep | fail | remove.
-type action() :: {start, flow_action()} | fail | {repair, flow_action()} | {call, flow_action()}.

-spec all_flow_actions() ->
    [flow_action()].
all_flow_actions() ->
    [sleep, fail, remove].

-spec all_actions() ->
    [action()].
all_actions() ->
       [{start , FlowAction} || FlowAction <- all_flow_actions()]
    ++ [fail]
    ++ [{repair, FlowAction} || FlowAction <- all_flow_actions()]
    ++ [{call  , FlowAction} || FlowAction <- all_flow_actions()]
.

-spec check_chain(mg_machine:options(), id()) ->
    ok.
check_chain(Options, ID) ->
    _ = rand:seed(exsplus, {ID, ID, ID}),
    check_chain(Options, ID, 0, all_actions(), not_exists).

-spec check_chain(mg_machine:options(), id(), seq(), [action()], state()) ->
    ok.
% TODO убрать константы
check_chain(_, _, 100000, _, _) ->
    ok;
check_chain(Options, ID, Seq, AllActions, State) ->
    Action = lists_random(AllActions),
    NewState = next_state(State, Action, do_action(Options, ID, Seq, Action)),
    check_chain(Options, ID, Seq + 1, AllActions, NewState).

-spec do_action(mg_machine:options(), id(), seq(), action()) ->
    result().
do_action(Options, ID, Seq, Action) ->
    try
        case Action of
            {start, ResultAction} ->
                mg_machine:start(Options, id(ID), ResultAction, req_ctx(ID, Seq), mg_utils:default_deadline());
            fail ->
                mg_machine:fail(Options, id(ID), req_ctx(ID, Seq), mg_utils:default_deadline());
            {repair, ResultAction} ->
                mg_machine:repair(Options, id(ID), ResultAction, req_ctx(ID, Seq), mg_utils:default_deadline());
            {call, ResultAction} ->
                mg_machine:call(Options, id(ID), ResultAction, req_ctx(ID, Seq), mg_utils:default_deadline())
        end
    catch
        throw:{logic, machine_failed         } -> failed;
        throw:{logic, machine_already_exist  } -> already_exist;
        throw:{logic, machine_not_found      } -> not_found;
        throw:{logic, machine_already_working} -> already_working
    end.

-spec req_ctx(id(), seq()) ->
    mg:request_context().
req_ctx(ID, Seq) ->
    [ID, Seq].

-spec id(id()) ->
    mg:id().
id(ID) ->
    erlang:integer_to_binary(ID).

-spec next_state(state(), action(), result()) ->
    state().

%% not_exists / start & remove
next_state(_, {_, remove}, ok       ) -> not_exists;
next_state(_, {_, remove}, not_found) -> not_exists;

next_state(not_exists, {start, sleep }, ok           ) -> sleeping;
next_state(not_exists, {start, fail  }, failed       ) -> not_exists;
next_state(S         , {start, _     }, already_exist) -> S;

next_state(not_exists, _, not_found) -> not_exists;
next_state(State = not_exists, Action, Result) -> erlang:error(bad_transition, [State, Action, Result]);

%% failed / fail & rapair
next_state(_, fail, ok) -> failed;

next_state(failed, {repair, sleep }, ok             ) -> sleeping;
next_state(failed, {repair, fail  }, failed         ) -> failed;
next_state(failed, _               , failed         ) -> failed;
next_state(S     , {repair, _     }, already_working) -> S;

next_state(State = failed, Action, Result) -> erlang:error(bad_transition, [State, Action, Result]);

%% sleeping / sleep
next_state(sleeping, {call, sleep }, ok    ) -> sleeping;
next_state(sleeping, {call, fail  }, failed) -> failed;

next_state(State, Action, Result) -> erlang:error(bad_transition, [State, Action, Result]).


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
process_machine(_, _, {init, FlowAction}, _, ReqCtx, _Deadline, AS) ->
    {{reply, ok}, map_flow_action(FlowAction, ReqCtx), AS};
process_machine(_, _, {call, FlowAction}, _, ReqCtx, _Deadline, AS) ->
    {{reply, ok}, map_flow_action(FlowAction, ReqCtx), AS};
% process_machine(_, _, timeout, ReqCtx, ?req_ctx, AS) ->
%     {noreply, sleep, AS};
process_machine(_, _, {repair, FlowAction}, _, ReqCtx, _Deadline, AS) ->
    {{reply, ok}, map_flow_action(FlowAction, ReqCtx), AS}.

-spec map_flow_action(flow_action(), mg:request_context()) ->
    mg_machine:processor_flow_action().
map_flow_action(sleep , _  ) -> sleep;
% map_flow_action(wait  , Ctx) -> {wait, 99, Ctx, 5000};
map_flow_action(remove, _  ) -> remove;
map_flow_action(fail  , _  ) -> exit(fail).


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
        pulse     => ?MODULE
    }.

-spec lists_random(list(T)) ->
    T.
lists_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
% для отладки может понадобится
% handle_beat(_, Beat) ->
%     ct:pal("~p", [Beat]).
handle_beat(_Options, _Beat) ->
    ok.
