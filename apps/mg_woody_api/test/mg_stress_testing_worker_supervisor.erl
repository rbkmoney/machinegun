%% Супервизор, контроллирующий тестовых воркеров
-module(mg_stress_testing_worker_supervisor).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%
%% API
%%
-spec start_link(mg_stress_testing:worker_sup()) ->
    supervisor:startlink_ret().
start_link(Options) ->
    Name = maps:get(worker_sup_name, Options),
    supervisor:start_link({via, gproc, Name}, ?MODULE, Options).

%%
%% Supervisor callbacks
%%
-spec init(mg_stress_testing:worker_sup()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    #{
        worker_mod  := WorkerMod,
        worker_opts := WorkerOpts
    } = maps:get(worker_sup_opts, Options),

    Flags     = #{strategy => simple_one_for_one},
    ChildSpec = #{
        id => WorkerMod,
        start => {WorkerMod, start_link, WorkerOpts},
        restart => temporary,
        shutdown => 10000,
        type => worker,
        modules => [WorkerMod]
    },
    {ok, Flags, [ChildSpec]}.
