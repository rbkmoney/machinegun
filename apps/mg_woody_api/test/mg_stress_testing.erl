%%%
%%% Корневой модуль для проведения стресс тестов
%%%
-module(mg_stress_testing).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    worker_mod       => module (),
    ccu              => integer(),
    session_duration => integer(),
    total_duration   => integer(),
    cps              => integer(),
    rps              => integer()

-spec start_link(options()) ->
    startlink_ret().
start_link(Options) ->
    ManagerSpec   = mg_stress_testing_worker_manager:child_spec(manager, manager_options(Options)),
    WorkerSupSpec = mg_stress_testing_worker_supervisor:child_spec(worker_sup, worker_supervisor_options(Options)),
    
    mg_utils_supervisor_wrapper:start_link({#{strategy => one_for_all}, [WorkerSupSpec, ManagerSpec]}).

%%
%% Utils
%%
-spec manager_options(options()) ->
    mg_stress_testing_worker_manager:options().
manager_options(Options) ->
    #{
        name             => manager,
        ccu              => maps:get(ccu,              Options),
        session_duration => maps:get(session_duration, Options),
        total_duration   => maps:get(total_duration,   Options),
        cps              => maps:get(cps,              Options),
        rps              => maps:get(rps,              Options)
    }.

-spec worker_supervisor_options(options()) ->
    mg_stress_testing_worker_supervisor:options().
worker_supervisor_options(Options) ->
    #{
        name   => worker_sup,
        worker => {maps:get(worker_mod, Options), #{}}
    }.

