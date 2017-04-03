%%%
%%% Корневой модуль для проведения стресс тестов
%%%
-module(mg_stress_testing).

%% API
-export([start_link/1]).

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    worker_mod       := module (), % Worker implementation module
    ccw              := integer(), % Concurrent workers
    aps              := integer(), % Actions per second
    wps              := integer(), % Workers per second
    session_duration := integer(),
    total_duration   := integer()
}.

-spec start_link(options()) ->
    supervisor:startlink_ret().
start_link(Options) ->
    ManagerSpec   = mg_stress_testing_worker_manager:child_spec(worker_manager, manager_options(Options)),
    WorkerSupSpec = mg_stress_testing_worker_supervisor:child_spec(worker_sup, supervisor_options(Options)),

    mg_utils_supervisor_wrapper:start_link(#{strategy => one_for_all}, [WorkerSupSpec, ManagerSpec]).

%%
%% Utils
%%
-spec manager_options(options()) ->
    mg_stress_testing_worker_manager:options().
manager_options(Options) ->
    #{
        name             => worker_manager,
        ccw              => maps:get(ccw,              Options),
        aps              => maps:get(aps,              Options),
        wps              => maps:get(wps,              Options),
        session_duration => maps:get(session_duration, Options),
        total_duration   => maps:get(total_duration,   Options)
    }.

-spec supervisor_options(options()) ->
    mg_stress_testing_worker_supervisor:options().
supervisor_options(Options) ->
    #{
        name   => worker_sup,
        worker => {maps:get(worker_mod, Options), []}
    }.

