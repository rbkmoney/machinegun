%%%
%%% Worker implementation
%%%
-module(mg_stress_testing_worker_impl).
-behaviour(mg_stress_testing_worker).

%% Worker callbacks
-export([start /1]).
-export([action/1]).

-type state() :: mg_stress_testing_worker:state().

-type step() :: first | second.

-type local_state() :: #{
    step := step()
}.

-spec start(state()) ->
    state().
start(S) ->
    LocalState = init_local_state(S),
    update_local_state(LocalState, S).

-spec action(state()) ->
    state().
action(S) ->
    LocalState0 = local_state(S),
    LocalState  = do_action(LocalState0),
    update_local_state(LocalState, S).

-spec do_action(local_state()) ->
    local_state().
do_action(LS) ->
    case step(LS) of
        first -> 
            print(first),
            LS#{step => last};
        last -> 
            print(last),
            LS#{step => first}
    end.

%%
%% Utils
%%
-spec local_state(state()) ->
    local_state().
local_state(S) ->
    maps:get(local_state, S).

-spec step(local_state()) ->
    step().
step(S) ->
    maps:get(step, S).

-spec init_local_state(state()) ->
    local_state().
init_local_state(_S) ->
    #{
        step => first
    }.

-spec update_local_state(local_state(), state()) ->
    state().
update_local_state(LocalState, S) ->
    S#{local_state => LocalState}.

-spec print(atom()) ->
    ok.
print(Step) ->
    io:format("~p from ~p\n", [Step, self()]).

