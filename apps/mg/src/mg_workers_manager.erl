%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:loading, Error}
%%%
-module(mg_workers_manager).
-behaviour(supervisor).

%% API
-export_type([error       /0]).
-export_type([thrown_error/0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([call       /3]).
-export([cast       /3]).

%% Supervisor callbacks
-export([init/1]).


%%
%% API
%%
%% кидаемая ожидаемая ошибка
-type error       () :: {loading, _Error}.
-type thrown_error() :: {workers, error()}.

-spec child_spec(atom(), _Options) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(_Options) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(self_reg_name(Options), ?MODULE, Options).

% sync
-spec call(_Options, _ID, _Call) ->
    _Reply.
call(Options, ID, Call) ->
    start_if_needed(Options, ID, fun() -> mg_worker:call(ID, Call) end).

% async
-spec cast(_Options, _ID, _Cast) ->
    ok.
cast(Options, ID, Cast) ->
    ok = start_if_needed(Options, ID, fun() -> mg_worker:cast(ID, Cast) end).

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
-spec start_if_needed(_Options, _ID, fun()) ->
    _.
start_if_needed(Options, ID, Expr) ->
    start_if_needed_iter(Options, ID, Expr, 10).

-spec start_if_needed_iter(_Options, _ID, fun(), non_neg_integer()) ->
    _.
start_if_needed_iter(_, _, _, 0) ->
    % такого быть не должно
    erlang:exit(unexpected_behaviour);
start_if_needed_iter(Options, ID, Expr, Attempts) ->
    try
        Expr()
    catch exit:{noproc, _} ->
        case start_child(Options, ID) of
            {ok, _} ->
                start_if_needed_iter(Options, ID, Expr, Attempts - 1);
            {error, {already_started, _}} ->
                start_if_needed_iter(Options, ID, Expr, Attempts - 1);
            {error, Error} ->
                throw_error({loading, Error})
        end
    end.


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

-spec throw_error(error()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({workers, Error}).
