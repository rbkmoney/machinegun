%%%
%%% Супервизор, контроллирующий тестовых воркеров
%%%
-module(mg_stress_testing_worker_supervisor).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init      /1]).
-export([child_spec/2]).

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    name   := atom(),
    worker := module()
}.

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildId, Options) ->
    #{
        id       => ChildId,
        start    => {?MODULE, start_link, [Options]},
        type     => supervisor,
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link({via, gproc, {n, l, maps:get(name, Options)}}, ?MODULE, Options).

%%
%% Supervisor callbacks
%%
-spec init(options()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    WorkerSpec = mg_stress_testing_worker:child_spec(worker, maps:get(worker, Options)),
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 100}, [WorkerSpec]}}.

