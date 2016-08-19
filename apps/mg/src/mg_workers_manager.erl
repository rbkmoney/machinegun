%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:loading, Error}
%%%
-module(mg_workers_manager).
-behaviour(supervisor).

%% API
-export_type([options     /0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([call       /3]).
-export([cast       /3]).

%% Supervisor callbacks
-export([init/1]).


%%
%% API
%%
-type options() :: #{
    name           => _,
    worker_options => mg_worker:options()
}.

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(self_reg_name(Options), ?MODULE, Options).

% sync
-spec call(options(), _ID, _Call) ->
    _Reply | {error, _}.
call(Options, ID, Call) ->
    start_if_needed(Options, ID, fun() -> mg_worker:call(ID, Call) end).

% async
-spec cast(options(), _ID, _Cast) ->
    ok | {error, _}.
cast(Options, ID, Cast) ->
    start_if_needed(Options, ID, fun() -> mg_worker:cast(ID, Cast) end).

%%
%% supervisor callbacks
%%
-spec init(options()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Options) ->
    SupFlags = #{strategy => simple_one_for_one},
    {ok, {SupFlags, [mg_worker:child_spec(worker, maps:get(worker_options, Options))]}}.

%%
%% local
%%
-spec start_if_needed(options(), _ID, fun()) ->
    _ | {error, _}.
start_if_needed(Options, ID, Expr) ->
    start_if_needed_iter(Options, ID, Expr, 10).

-spec start_if_needed_iter(options(), _ID, fun(), non_neg_integer()) ->
    _.
start_if_needed_iter(_, _, _, 0) ->
    % такого быть не должно
    erlang:exit(unexpected_behaviour);
start_if_needed_iter(Options, ID, Expr, Attempts) ->
    try
        Expr()
    catch
        exit:_ ->
            % NOTE возможно тут будут проблемы
            case start_child(Options, ID) of
                {ok, _} ->
                    start_if_needed_iter(Options, ID, Expr, Attempts - 1);
                {error, {already_started, _}} ->
                    start_if_needed_iter(Options, ID, Expr, Attempts - 1);
                Error={error, _} ->
                    Error
            end
    end.

-spec start_child(options(), _ID) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID) ->
    supervisor:start_child(self_ref(Options), [ID]).


-spec self_ref(options()) ->
    mg_utils:gen_ref().
self_ref(Options) ->
    {via, gproc, gproc_key(Options)}.

-spec self_reg_name(options()) ->
    mg_utils:gen_reg_name().
self_reg_name(Options) ->
    {via, gproc, gproc_key(Options)}.

-spec gproc_key(options()) ->
    gproc:key().
gproc_key(Options) ->
    {n, l, wrap(maps:get(name, Options))}.

-spec wrap(_) ->
    term().
wrap(V) ->
    {?MODULE, V}.