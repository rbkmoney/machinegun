%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:loading, Error}
%%%
%%% TODO:
%%%  - сделать автоматические ретраи со внешней политикой
%%%  - сделать выгрузку не по таймеру, а по занимаемой памяти и времени последней активности
%%%  -
%%%
-module(mg_workers_manager).
-behaviour(supervisor).

%% API
-export_type([options/0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([call      /3]).

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
    call(Options, ID, Call, 10).

-spec call(options(), _ID, _Call, non_neg_integer()) ->
    _Reply | {error, _}.
call(_, _, _, 0) ->
    % такого быть не должно
    {error, unexpected_behaviour};
call(Options, ID, Call, Attempts) ->
    Name = maps:get(name, Options),
    try
        mg_worker:call(Name, ID, Call)
    catch
        exit:Reason ->
            case Reason of
                { noproc    , _} -> start_and_retry_call(Options, ID, Call, Attempts);
                { normal    , _} -> start_and_retry_call(Options, ID, Call, Attempts);
                { shutdown  , _} -> start_and_retry_call(Options, ID, Call, Attempts);
                { timeout   , _} -> {error, timeout};
                Unknown       -> {error, {unexpected_exit, Unknown}}
            end
    end.

-spec start_and_retry_call(options(), _ID, _Call, non_neg_integer()) ->
    _Reply | {error, _}.
start_and_retry_call(Options, ID, Call, Attempts) ->
    %
    % NOTE возможно тут будут проблемы и это место надо очень хорошо отсмотреть
    %  чтобы потом не ловить неожиданных проблем
    %
    % идея в том, что если нет процесса, то мы его запускаем
    %
    case start_child(Options, ID) of
        {ok, _} ->
            call(Options, ID, Call, Attempts - 1);
        {error, {already_started, _}} ->
            call(Options, ID, Call, Attempts - 1);
        Error={error, _} ->
            Error
    end.


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
-spec start_child(options(), _ID) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID) ->
    supervisor:start_child(self_ref(Options), [maps:get(name, Options), ID]).


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
