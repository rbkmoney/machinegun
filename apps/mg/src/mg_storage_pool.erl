-module(mg_storage_pool).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, do_request/3]).
-export([start_link/2]).

-type options() :: #{
    worker          := mg_storage:options(),
    size            := pos_integer(), % 100
    queue_len_limit := pos_integer(), % 10
    retry_attempts  := pos_integer()  % size
}.
-type worker_id() :: term().


%%
%% mg_storage callbacks
%%
-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, RegName) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, RegName]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options(), mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(#{worker := Worker}, RegName) ->
    mg_utils_supervisor_wrapper:start_link(
        RegName,
        #{strategy => simple_one_for_one},
        [
            mg_storage:child_spec(Worker, worker)
        ]
    ).

-spec do_request(options(), mg_utils:gen_ref(), mg_storage:request()) ->
    mg_storage:response() | no_return().
do_request(Options = #{retry_attempts := RetryAttempts}, SelfRef, Req) ->
    do_request(Options, SelfRef, Req, RetryAttempts).

-spec do_request(options(), mg_utils:gen_ref(), mg_storage:request(), pos_integer()) ->
    mg_storage:response() | no_return().
do_request(_Options, _SelfRef, _Req, 0) ->
    erlang:throw({storage_unavailable, {'storage request error', overload}});
do_request(Options = #{size := Size, worker := Worker, queue_len_limit := Limit}, SelfRef, Req, RemainAttempts) ->
    WorkerID = random_worker_id(Size),
    WorkerRef = worker_ref(SelfRef, WorkerID),
    F = fun() ->
            ok = mg_utils:check_overload(WorkerRef, Limit),
            mg_storage:do_request(Worker, WorkerRef, Req)
        end,
    case try_gen_call(F) of
        {error, noproc} ->
            ok = start_worker(Options, SelfRef, WorkerID),
            do_request(Options, SelfRef, Req, RemainAttempts);
        {error, overload} ->
            do_request(Options, SelfRef, Req, RemainAttempts -1 );
        {error, Reason} ->
            erlang:throw({storage_unavailable, {'storage request error', Reason}});
        R ->
            R
    end.

%%
%% local
%%
-spec start_worker(options(), mg_utils:gen_ref(), worker_id()) ->
    ok | no_return().
start_worker(#{queue_len_limit := Limit}, SelfRef, WorkerID) ->
    F = fun() ->
            ok = mg_utils:check_overload(SelfRef, Limit),
            supervisor:start_child(SelfRef, [worker_reg_name(SelfRef, WorkerID)])
        end,
    case try_gen_call(F) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            erlang:throw({storage_unavailable, {'start pool worker error', Reason}})
    end.

-spec worker_reg_name(mg_utils:gen_ref(), worker_id()) ->
    mg_utils:gen_ref().
worker_reg_name(SelfRef, WorkerID) ->
    {via, gproc, gproc_key(SelfRef, WorkerID)}.

-spec worker_ref(mg_utils:gen_ref(), worker_id()) ->
    mg_utils:gen_ref().
worker_ref(SelfRef, WorkerID) ->
    {via, gproc, gproc_key(SelfRef, WorkerID)}.

-spec gproc_key(mg_utils:gen_ref(), worker_id()) ->
    gproc:key().
gproc_key(SelfRef, WorkerID) ->
    {n, l, wrap(SelfRef, WorkerID)}.

-spec wrap(mg_utils:gen_ref(), worker_id()) ->
    term().
wrap(SelfRef, WorkerID) ->
    % нужно быть аккуратным, если сюда передать pid, то ничего работать не будет :)
    {?MODULE, SelfRef, WorkerID}.

-spec random_worker_id(pos_integer()) ->
    worker_id().
random_worker_id(N) ->
    worker_id(rand:uniform(N)).

-spec worker_id(integer()) ->
    worker_id().
worker_id(N) ->
    list_to_atom(integer_to_list(N)).

-spec try_gen_call(fun(() -> Result)) ->
    {ok, Result} | {error, _}.
try_gen_call(F) ->
    try
        F()
    catch
        exit: noproc             -> {error, noproc};
        exit:{noproc  , _      } -> {error, noproc};
        exit:{normal  , _      } -> {error, noproc};
        exit:{shutdown, _      } -> {error, noproc};
        exit:{timeout , Details} -> {error, {timeout , Details}};
        exit:overload            -> {error, overload}
    end.
