%%%
%%% Супервизор, контроллирующий тестовых воркеров
%%%
-module(mg_stress_testing_worker_supervisor).
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-export([child_spec/2]).

%%
%% API
%%
-type options() :: #{
    name   := atom(),
    worker := mg_utils:mod_opts(mg_stress_testing_worker:options())
}.
-export_type([options/0]).

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildId, Options) ->
    #{
        id       => ChildId,
        start    => {?MODULE, start_link, [Options]},
        type     => supervisor,
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    Name = maps:get(name, Options),
    supervisor:start_link({via, gproc, {n,l,Name}}, ?MODULE, maps:get(worker, Options)).

%%
%% Supervisor callbacks
%%
-spec init(options()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({WorkerMod, WorkerOpts}) ->
    WorkerSpec = erlang:apply(mg_stress_testing_worker, child_spec, [WorkerOpts]),
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 100}, [WorkerSpec]}}.

