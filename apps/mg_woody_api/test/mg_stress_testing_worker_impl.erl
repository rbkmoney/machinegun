%%%
%%% Worker implementation
%%%
-module(mg_stress_testing_worker_impl).
-behaviour(mg_stress_testing_worker).

%% Worker callbacks
-export([do_action/1]).

-type state() :: term().

-spec do_action(state()) ->
    state().
do_action(S) ->
    io:format("~p from ~p", ["hello", self()]),
    S.

