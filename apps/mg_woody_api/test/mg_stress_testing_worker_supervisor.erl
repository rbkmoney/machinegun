%%%
%%% Супервизор, контроллирующий тестовых воркеров
%%%
-module(mg_stress_testing_worker_supervisor).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-export([child_spec/1]).

%%
%% API
%%
-type options() :: #{
    name := atom()
}.
-export_type([options/0]).

-spec child_spec(options()) ->
    supervisor:child_spec().
child_spec(Name) ->
    #{
        id       => Name,
        start    => {?MODULE, start_link, [Name]},
        type     => supervisor,
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Name) ->
    supervisor:start_link({via, gproc, {n, l, Name}}, ?MODULE, []).

%%
%% Supervisor callbacks
%%
-spec init(_) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    WorkerSpec = erlang:apply(mg_stress_testing_worker, child_spec, []),
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 100}, [WorkerSpec]}}.

