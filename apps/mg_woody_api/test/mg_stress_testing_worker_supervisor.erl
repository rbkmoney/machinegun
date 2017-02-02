%%% Супервизор, контроллирующий тестовых воркеров
-module(mg_stress_testing_worker_supervisor).
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
    name   => atom(),
    module => module(),
    worker => mg_utils:mod_opts(mg_stress_testing_worker:options())
}.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    Name = maps:get(name, Options),
    supervisor:start_link({via, gproc, Name}, ?MODULE, Options).

%%
%% Supervisor callbacks
%%
-spec init(options()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    {WorkerMod, WorkerOpts} = maps:get(worker, Options),

    Flags     = #{strategy => simple_one_for_one},
    erlang:apply(Worker
    ChildSpec = #{
        id => WorkerMod,
        start => {WorkerMod, start_link, WorkerOpts},
        restart => temporary,
        shutdown => 10000,
        type => worker,
        modules => [WorkerMod]
    },
    {ok, Flags, [ChildSpec]}.
