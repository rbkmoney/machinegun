%%% Корневой модуль для проведения стресс тестов
-module(mg_stress_testing).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%
%% API
%%
-type options() :: #{
    manager    => mg_utils:mod_opts(mg_stress_testing_worker_manager:options()),
    worker_sup => mg_stress_testing_worker_supervisor:options()
}.

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
    ManagerOpts = maps:get(manager, Options),
    ManagerSpec = mg_stress_testing_worker_manager:child_spec(manager, ManagerOpts),

    WorkerSupOpts = maps:get(worker, Options),
    WorkerSupSpec = mg_stress_testing_worker_supervisor:child_spec(worker_supervisor, WorkerOpts),
    
    mg_utils_supervisor_wrapper:start_link({#{strategy => one_for_all}, [WorkerSupSpec, ManagerSpec]}).

