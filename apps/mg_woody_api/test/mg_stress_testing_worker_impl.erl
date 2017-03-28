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
do_action(undefined, ) ->
    io:format("~p from ~p", ["hello", self()]),
    first;
do_action(first) ->