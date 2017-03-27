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
    worker_mod       => module (),
    ccw              => integer(),
    aps              => integer(),
    wps              => integer(),
    session_duration => integer(),
    total_duration   => integer()
}.

-spec start_link(options()) ->
    supervisor:startlink_ret().
start_link(_) ->
    Options = #{
        worker_mod       => mg_stress_testing_worker_impl,
        ccw              => 1,
        aps              => 1,
        wps              => 1,
        session_duration => 10000,
        total_duration   => 10000
    },
    ManagerSpec   = mg_stress_testing_worker_manager:child_spec(worker_manager, manager_options(Options)),
    WorkerSupSpec = mg_stress_testing_worker_supervisor:child_spec(worker_sup, worker_supervisor_options(Options)),

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

-spec worker_supervisor_options(options()) ->
    mg_stress_testing_worker_supervisor:options().
worker_supervisor_options(#{worker_mod := WorkerMod}) ->
    #{
        name   => worker_sup,
        worker => {WorkerMod, #{}}
    }.

