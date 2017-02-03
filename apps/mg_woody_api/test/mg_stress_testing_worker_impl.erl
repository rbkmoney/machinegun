%%%
%%% Имплементация воркера для нагрузочного тестирования
%%%

-module   (mg_stress_testing_worker_impl).
-behaviour(mg_stress_testing_worker).

%% Worker callbacks
-export([child_spec/2, start_session/1, do_action/2, finish_session/1]).

-type state() :: mg_stress_testing_worker:state(map()). 

-spec child_spec(state()) ->
    supervisor:child_spec().
child_spec(ChildId, Options) ->
    #{
        id       => ChildId,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill,
    }.

-spec start_session(state()) ->
    {noreply, state()} | {noreply, state(), term()}.
start_session(State) ->
    {noreply, State}.

-spec do_action(atom(), state()) ->
    state().
do_action(_, State) ->
    State.

-spec finish_session(state()) ->
    {stop, normal, state()}.
finish_session(State) ->
    {stop, normal, State}.
    
