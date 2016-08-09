%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Распределяются по кластеру и реплицируют свой стейт через эвенты.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%%
-module(mg_workers_manager).
-behaviour(supervisor).

%% API
-export([child_spec /2]).
-export([start_link /1]).
-export([call       /3]).
-export([cast       /3]).

%% Supervisor callbacks
-export([init/1]).


%%
%% API
%%
-spec child_spec(atom(), _Options) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor,
        shutdown => brutal_kill
    }.

-spec start_link(_Options) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(self_reg_name(Options), ?MODULE, Options).

-define(START_IF_NEEDED(Options, ID, Expr),
    try
        Expr
    catch exit:{noproc, _} ->
        case start_child(Options, ID) of
            {ok, _} ->
                Expr;
            {error, {already_started, _}} ->
                Expr
        end
    end
).

% sync
-spec call(_Options, _ID, _Call) ->
    _Reply.
call(Options, ID, Call) ->
    ?START_IF_NEEDED(Options, ID, mg_worker:call(ID, Call)).

% async
-spec cast(_Options, _ID, _Cast) ->
    ok.
cast(Options, ID, Cast) ->
    ok = ?START_IF_NEEDED(Options, ID, mg_worker:cast(ID, Cast)).


%%
%% supervisor callbacks
%%
-spec init(_Options) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    SupFlags = #{strategy => simple_one_for_one},
    {ok, {SupFlags, [mg_worker:child_spec(machine, Options)]}}.

%%
%% local
%%
-spec start_child(_Options, _ID) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID) ->
    supervisor:start_child(self_ref(Options), [ID]).


-spec self_ref(_Options) ->
    mg_utils:gen_ref().
self_ref(Options) ->
    {via, gproc, {n, l, wrap(Options)}}.

-spec self_reg_name(_Options) ->
    mg_utils:gen_reg_name().
self_reg_name(Options) ->
    {via, gproc, {n, l, wrap(Options)}}.

-spec wrap(_) ->
    term().
wrap(V) ->
    {?MODULE, V}.
