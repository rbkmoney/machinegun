%% Корневой модуль для проведения стресс тестов
-module(mg_stress_testing).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%
%% API
%%
-export_type([worker_sup_options/0]).
-type worker_sup_options() :: #{
    worker_mod  => module(),
    worker_opts => term()
}.

-export_type([worker_options/0]).
-type worker_sup() :: #{
    worker_sup_name => term(),               % Supervisor name
    worker_sup_mod  => module(),             % Supervisor module
    worker_sup_opts => worker_sup_options()  % Supervisor options (worker options)
}.

-export_type([manager_options/0]).
-type manager_options() :: #{
    ccu              => integer(), % Concurrent users
    session_duration => integer(),
    total_duration   => integer(),
    cps              => integer(), % Connects per second
    rps              => integer()  % Requests per second
}.

-export_type([manager/0]).
-type manager() :: #{
    manager_mod  => module(),
    manager_opts => manager_options(),
}.

-export_type([options/0]).
-type options() :: #{
    manager    => manager(),
    worker_sup => worker_sup()
}.

-spec start_link(options()) ->
    startlink_ret().
start_link(Options) ->
    Flags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },

    Manager     = maps:get(worker_manager, Options),
    ManagerMod  = maps:get(module, Manager),
    ManagerOpts = maps:get(opts, Manager),
    ManagerSpec => #{
        id => ManagerMod,
        start => {ManagerMod, start_link, ManagerOpts},
        restart => temporary,
        shutdown => 10000,
        type => worker,
        modules => [ManagerMod]
    },

    {WorkerSupMod, WorkerSupOpts} = maps:get(worker_sup, Options),
    WorkerSupSpec = #{
        id => WorkerSupMod,
        start => {WorkerSupMod, start_link, WorkerSupOpts},
        restart => temporary,
        shutdown => 10000,
        type => supervisor,
        modules => [WorkerSupMod]
    },

    mg_utils_supervisor_wrapper:start_link({Flags, [WorkerSupSpec, ManagerSpec]}).
