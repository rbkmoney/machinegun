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
-export_type([worker_options/0]).
-type worker_options() ::
    {module(), term()}
.

-export_type([manager_options/0]).
-type manager_options() ::
    {ccu             , integer()} % Concurrent users
  | {session_duration, integer()}
  | {total_duration  , integer()}
  | {cps             , integer()} % Connects per second
  | {rps             , integer()} % Requests per second
.

-export_type([manager/0]).
-type manager() ::
    {module, module         ()}
  | {opts  , manager_options()}
.

-type options() ::
    {worker_manager, manager       ()}
  | {worker_options, worker_options()}
.

-spec start_link(options()) ->
    startlink_ret().
start_link(Options) ->
   supervisor:start_link(?MODULE, Options).

-spec init(options()) ->
   {ok, {sup_flags(), [child_spec()]}}.
init(Options) ->
    Manager          = proplists:get_value(worker_manager, Options),
    ManagerMod       = proplists:get_value(module, Manager),
    ManagerOpts      = proplists:get_value(opts, Manager),
    ManagerStartFunc = {ManagerMod, start_link, ManagerOpts},
    ManagerSpec      = {ManagerMod, ManagerStartFunc, temporary, 10000, worker, [ManagerMod]},

    WorkerSupMod = proplists:get_value(worker_sup_mod, Options),
    WorkerOpts   = proplists:get_value(worker_options, Options),
    StartFunc    = {WorkerSupMod, start_link, WorkerOpts},
    WorkerSpec   = {WorkerSupMod, StartFunc, temporary, 10000, worker, [WorkerSupMod]},
    {ok, {{simple_one_for_one, 0, 1}}, [WorkerSpec, ManagerSpec]}.
