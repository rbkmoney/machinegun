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
-spec start_link(mg_stress_testing:worker_options()) ->
    startlink_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

%%
%% Supervisor callbacks
%%
-spec init(mg_stress_testing:worker_options()) ->
    {ok, {sup_flags(), [child_spec()]}}.
init({StressTestingArgs, WorkerMod, WorkerArgs}) ->
    StartFunc = {WorkerMod, start_link, WorkerArgs},
    ChildSpec = {WorkerMod, StartFunc, temporary, 10000, worker, [WorkerMod]},
    {ok, {#{strategy => simple_one_for_one}, [ChildSpec]}}.
