%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:{loading, Error}
%%%
%%% TODO:
%%%  - сделать выгрузку не по таймеру, а по занимаемой памяти и времени последней активности
%%%  -
%%%
-module(mg_workers_manager).

%% API
-export_type([options/0]).

-export([child_spec    /2]).
-export([start_link    /1]).
-export([call          /5]).
-export([get_call_queue/2]).
-export([brutal_kill   /2]).
-export([is_alive      /2]).

%%
%% API
%%
-type options() :: #{
    name                    => _,
    message_queue_len_limit => pos_integer(),
    worker_options          => mg_worker:options()
}.

-define(default_message_queue_len_limit, 50).

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    mg_utils_supervisor_wrapper:start_link(
        self_reg_name(Options),
        #{strategy => simple_one_for_one},
        [
            mg_worker:child_spec(worker, maps:get(worker_options, Options))
        ]
    ).

% sync
-spec call(options(), _ID, _Call, _ReqCtx, mg_utils:deadline()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline) ->
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            Name = maps:get(name, Options),
            try
                mg_worker:call(Name, ID, Call, ReqCtx, Deadline, message_queue_len_limit(Options))
            catch
                exit:Reason ->
                    handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason)
            end;
        true ->
            {error, {timeout, worker_call_deadline_reached}}
    end.

-spec handle_worker_exit(options(), _ID, _Call, _ReqCtx, mg_utils:deadline(), _Reason) ->
    _Reply | {error, _}.
handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason) ->
    case Reason of
         noproc         -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline);
        {noproc    , _} -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline);
        {normal    , _} -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline);
        {shutdown  , _} -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline);
        {timeout   , _} -> {error, Reason};
         Unknown        -> {error, {unexpected_exit, Unknown}}
    end.

-spec start_and_retry_call(options(), _ID, _Call, _ReqCtx, mg_utils:deadline()) ->
    _Reply | {error, _}.
start_and_retry_call(Options, ID, Call, ReqCtx, Deadline) ->
    %
    % NOTE возможно тут будут проблемы и это место надо очень хорошо отсмотреть
    %  чтобы потом не ловить неожиданных проблем
    %
    % идея в том, что если нет процесса, то мы его запускаем
    %
    case start_child(Options, ID, ReqCtx) of
        {ok, _} ->
            call(Options, ID, Call, ReqCtx, Deadline);
        {error, {already_started, _}} ->
            call(Options, ID, Call, ReqCtx, Deadline);
        Error={error, _} ->
            Error
    end.

-spec get_call_queue(options(), _ID) ->
    [_Call].
get_call_queue(Options, ID) ->
    try
        mg_worker:get_call_queue(maps:get(name, Options), ID)
    catch exit:noproc ->
        []
    end.

-spec brutal_kill(options(), _ID) ->
    ok.
brutal_kill(Options, ID) ->
    try
        mg_worker:brutal_kill(maps:get(name, Options), ID)
    catch exit:noproc ->
        ok
    end.

-spec is_alive(options(), _ID) ->
    boolean().
is_alive(Options, ID) ->
    mg_worker:is_alive(maps:get(name, Options), ID).


%%
%% local
%%
-spec start_child(options(), _ID, _ReqCtx) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID, ReqCtx) ->
    SelfRef = self_ref(Options),
    try
        ok = mg_utils:check_overload(SelfRef, message_queue_len_limit(Options)),
        supervisor:start_child(SelfRef, [maps:get(name, Options), ID, ReqCtx])
    catch
        exit:{timeout, Reason} ->
            {error, {timeout, Reason}};
        exit:overload ->
            {error, {transient, overload}}
    end.

-spec message_queue_len_limit(options()) ->
    pos_integer().
message_queue_len_limit(Options) ->
    maps:get(message_queue_len_limit, Options, ?default_message_queue_len_limit).

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
