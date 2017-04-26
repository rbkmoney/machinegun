%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% При падении хендлера переводит машину в error состояние.
%%% Оперирует стейтом и набором атрибутов.
%%%
-module(mg_machine).

%%
%% Логика работы с ошибками.
%%
%% Возможные ошибки:
%%  - ожиданные -- throw()
%%   - бизнес-логические
%%    - машина не найдена -- machine_not_found
%%    - машина уже существует -- machine_already_exist
%%    - машина находится в упавшем состоянии -- machine_failed
%%    - что-то ещё?
%%   - временные -- transient
%%    - сервис перегружен —- overload
%%    - хранилище недоступно -- {storage_unavailable  , Details}
%%    - процессор недоступен -- {processor_unavailable, Details}
%%   - таймауты -- {timeout, Details}
%%  - неожиданные
%%   - что-то пошло не так -- падение с любой другой ошибкой
%%
%% Например: throw:machine_not_found, throw:{transient, {storage_unavailable, ...}}, error:badarg
%%
%% Если в процессе обработки внешнего запроса происходит ожидаемая ошибка, она мапится в код ответа,
%%  если неожидаемая ошибка, то запрос падает с internal_error, ошибка пишется в лог.
%%
%% Если в процессе обработки запроса машиной происходит ожидаемая ошибка, то она прокидывается вызывающему коду,
%%  если неожидаемая, то машина переходит в error состояние и в ответ возникает ошибка machine_failed.
%%
%% Хранилище и процессор кидают либо ошибку о недоступности, либо падают.
%%

%% API
-export_type([options               /0]).
-export_type([thrown_error          /0]).
-export_type([logic_error           /0]).
-export_type([transient_error       /0]).
-export_type([throws                /0]).
-export_type([machine_state         /0]).
-export_type([processor_impact      /0]).
-export_type([processing_context    /0]).
-export_type([processor_result      /0]).
-export_type([processor_reply_action/0]).
-export_type([processor_flow_action /0]).
-export_type([search_query          /0]).

-export([child_spec /2]).
-export([start_link /1]).

-export([start               /4]).
-export([simple_repair       /3]).
-export([repair              /4]).
-export([call                /4]).
-export([get                 /2]).
-export([search              /2]).
-export([reply               /2]).
-export([call_with_lazy_start/5]).

%% Internal API
-export([handle_timers         /1]).
-export([handle_timers         /2]).
-export([handle_timer          /2]).
-export([reload_killed_machines/1]).
-export([reload_killed_machines/2]).
-export([touch                 /2]).
-export([touch                 /3]).
-export([all_statuses          /0]).
-export([manager_options       /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/2, handle_call/3, handle_unload/1]).

%%
%% API
%%
-type options() :: #{
    namespace              => mg:ns(),
    storage                => mg_storage:storage          (),
    processor              => mg_utils:mod_opts           (),
    storage_retry_policy   => mg_utils:genlib_retry_policy(), % optional
    processor_retry_policy => mg_utils:genlib_retry_policy()  % optional
}.

-type thrown_error() :: logic_error() | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason}.

-type throws() :: no_return().
-type machine_state() :: mg_storage:opaque().

%%

-type processing_state() :: term().
-type processing_context() :: #{
    call_context => mg_worker:call_context(),
    state        => processing_state()
} | undefined.
-type processor_impact() ::
      {init   , term()}
    | {repair , term()}
    | {call   , term()}
    |  timeout
    |  continuation
.
-type processor_reply_action() :: noreply  | {reply, _}.
-type processor_flow_action() :: sleep | {wait, genlib_time:ts()} | {continue, processing_state()}.
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.

-callback processor_child_spec(_Options) ->
    mg_utils_supervisor_wrapper:child_spec().
-callback process_machine(_Options, mg:id(), processor_impact(), processing_context(), machine_state()) ->
    processor_result().
-optional_callbacks([processor_child_spec/1]).

-type search_query() ::
       sleeping
    |  waiting
    | {waiting, From::genlib_time:ts(), To::genlib_time:ts()}
    |  processing
    |  failed
.

%%

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
        #{strategy => one_for_all},
        [
            mg_workers_manager:child_spec(manager_options      (Options), manager      ),
            mg_storage        :child_spec(storage_options      (Options), storage      ),
            mg_cron           :child_spec(timers_cron_options  (Options), timers_cron  ),
            mg_cron           :child_spec(overseer_cron_options(Options), overseer_cron),
            processor_child_spec(Options)
        ]
    ).

-spec start(options(), mg:id(), term(), mg_utils:deadline()) ->
    _Resp | throws().
start(Options, ID, Args, Deadline) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {start, Args}, Deadline)).

-spec simple_repair(options(), mg:id(), mg_utils:deadline()) ->
    _Resp | throws().
simple_repair(Options, ID, Deadline) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, simple_repair, Deadline)).

-spec repair(options(), mg:id(), term(), mg_utils:deadline()) ->
    _Resp | throws().
repair(Options, ID, Args, Deadline) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {repair, Args}, Deadline)).

-spec call(options(), mg:id(), term(), mg_utils:deadline()) ->
    _Resp | throws().
call(Options, ID, Call, Deadline) ->
    % TODO specify timeout
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {call, Call}, Deadline)).

-spec get(options(), mg:id()) ->
    machine_state() | throws().
get(Options, ID) ->
    #{state:=State} = mg_utils:throw_if_undefined(element(2, get_storage_machine(Options, ID)), machine_not_found),
    State.

-spec search(options(), search_query()) ->
    [{_TODO, mg:id()}] | [mg:id()] | throws().
search(Options, Query) ->
    mg_storage:search(storage_options(Options), storage_search_query(Query)).

-spec call_with_lazy_start(options(), mg:id(), term(), mg_utils:deadline(), term()) ->
    _Resp | throws().
call_with_lazy_start(Options, ID, Call, Deadline, StartArgs) ->
    try
        call(Options, ID, Call, Deadline)
    catch throw:machine_not_found ->
        try
            _ = start(Options, ID, StartArgs, Deadline)
        catch throw:machine_already_exist ->
            % вдруг кто-то ещё делает аналогичный процесс
            ok
        end,
        % если к этому моменту машина не создалась, значит она уже не создастся
        % и исключение будет оправданным
        call(Options, ID, Call, Deadline)
    end.

-spec reply(processing_context(), _) ->
    ok.
reply(undefined, _) ->
    % отвечать уже некому
    ok;
reply(#{call_context := CallContext}, Reply) ->
    ok = mg_worker:reply(CallContext, Reply).

%%
%% Internal API
%%
-spec handle_timers(options()) ->
    ok.
handle_timers(Options) ->
    handle_timers(Options, search(Options, {waiting, 1, genlib_time:now()})).

-spec handle_timers(options(), [{genlib_time:ts(), mg:id()}]) ->
    ok.
handle_timers(Options, Timers) ->
    lists:foreach(
        % такая схема потенциально опасная, но надо попробовать как она себя будет вести
        fun(Timer) ->
            erlang:spawn_link(fun() -> handle_timer(Options, Timer) end)
        end,
        Timers
    ).

-spec handle_timer(options(), {genlib_time:ts(), mg:id()}) ->
    ok.
handle_timer(Options, {Timestamp, MachineID}) ->
    % TODO вообще надо бы как-то это тюнить
    Deadline = mg_utils:timeout_to_deadline(30000),
    Call = {timeout, Timestamp},
    CallQueue = mg_workers_manager:get_call_queue(manager_options(Options), MachineID),
    case lists:member(Call, CallQueue) of
        false ->
            case mg_workers_manager:call(manager_options(Options), MachineID, Call, Deadline) of
                {ok, ok} ->
                    ok;
                {error, Reason} ->
                    ok = log_failed_timer_handling(namespace(Options), MachineID, Reason)
            end;
        true ->
            ok
    end.

-spec reload_killed_machines(options()) ->
    ok.
reload_killed_machines(Options) ->
    reload_killed_machines(Options, search(Options, processing)).

-spec reload_killed_machines(options(), [mg:id()]) ->
    ok.
reload_killed_machines(Options, MachinesIDs) ->
    lists:foreach(
        fun(MachineID) ->
            erlang:spawn_link(fun() -> touch(Options, MachineID) end)
        end,
        MachinesIDs
    ).

-spec touch(options(), mg:id()) ->
    ok.
touch(Options, MachineID) ->
    % TODO вообще надо бы как-то это тюнить
    Deadline = mg_utils:timeout_to_deadline(30000),
    touch(Options, MachineID, Deadline).

-spec touch(options(), mg:id(), mg_utils:deadline()) ->
    ok.
touch(Options, MachineID, Deadline) ->
    % TODO переназвать функцию
    case mg_workers_manager:is_alive(manager_options(Options), MachineID) of
        false ->
            case mg_workers_manager:call(manager_options(Options), MachineID, touch, Deadline) of
                {ok, ok} ->
                    ok;
                {error, Reason} ->
                    ok = log_failed_touch(namespace(Options), MachineID, Reason)
            end;
        true ->
            ok
    end.

-spec all_statuses() ->
    [atom()].
all_statuses() ->
    [sleeping, waiting, processing, failed].

%%
%% mg_worker callbacks
%%
-type state() :: #{
    id              => mg:id(),
    options         => options(),
    storage_machine => storage_machine() | undefined,
    storage_context => mg_storage:context() | undefined
}.

-type storage_machine() :: #{
    status          => machine_status(),
    state           => machine_state()
}.

-type machine_regular_status() :: sleeping | {waiting, genlib_time:ts()} | processing.
-type machine_status() :: machine_regular_status() | {error, _, machine_regular_status()}.

-spec handle_load(_ID, options()) ->
    {ok, state()}.
handle_load(ID, Options) ->
    try
        {StorageContext, StorageMachine} = get_storage_machine(Options, ID),
        State =
            #{
                id              => ID,
                options         => Options,
                storage_machine => StorageMachine,
                storage_context => StorageContext
            },
        {ok, try_finish_processing(State)}
    catch throw:Reason ->
        ok = log_load_error(namespace(Options), ID, {throw, Reason, erlang:get_stacktrace()}),
        {error, Reason}
    end.

-spec handle_call(_Call, mg_worker:call_context(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, State=#{storage_machine:=StorageMachine}) ->
    PCtx = new_processing_context(CallContext),
    case {Call, StorageMachine} of
        % success
        {{start , Args   }, undefined                 } -> {noreply, process_start(Args, PCtx, State)};
        {{call  , SubCall}, #{status:= sleeping      }} -> {noreply, process({call   , SubCall}, PCtx, State)};
        {{call  , SubCall}, #{status:={waiting, _   }}} -> {noreply, process({call   , SubCall}, PCtx, State)};
        {{repair, Args   }, #{status:={error  , _, _}}} -> {noreply, process({repair , Args   }, PCtx, State)};
        { simple_repair   , #{status:={error  , _, _}}} -> {{reply, ok}, process_simple_repair(State)};

        % failure
        {{start  , _     }, #{status:=             _ }} -> {{reply, {error, machine_already_exist  }}, State};
        {{call   , _     }, #{status:={error  , _, _}}} -> {{reply, {error, machine_failed         }}, State};
        {{call   , _     }, undefined                 } -> {{reply, {error, machine_not_found      }}, State};
        {{repair , _     }, #{status:= waiting       }} -> {{reply, {error, machine_already_working}}, State};
        {{repair , _     }, undefined                 } -> {{reply, {error, machine_not_found      }}, State};
        { simple_repair   , #{status:= waiting       }} -> {{reply, {error, machine_already_working}}, State};
        { simple_repair   , undefined                 } -> {{reply, {error, machine_not_found      }}, State};

        % ничего не просходит, просто убеждаемся, что машина загружена
        {touch, _} -> {{reply, {ok, ok}}, State};

        % timers
        {{timeout, Ts0}, #{status:={waiting, Ts1}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, State)};
        {{timeout, _}, #{status:=_}} ->
            {{reply, {ok, ok}}, State}
    end.

-spec handle_unload(state()) ->
    ok.
handle_unload(_) ->
    ok.

%%
%% processing context
%%
-spec new_processing_context(mg_worker:call_context()) ->
    processing_context().
new_processing_context(CallContext) ->
    #{
        call_context => CallContext,
        state        => undefined
    }.

%%
%% storage machine
%%
-spec new_storage_machine() ->
    storage_machine().
new_storage_machine() ->
    #{
        status => sleeping,
        state  => null
    }.

-spec get_storage_machine(options(), mg:id()) ->
    {mg_storage:context(), storage_machine()} | undefined.
get_storage_machine(Options, ID) ->
    case mg_storage:get(storage_options(Options), ID) of
        undefined ->
            {undefined, undefined};
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(PackedMachine)}
    end.

%%
%% packer to opaque
%%
-spec storage_machine_to_opaque(storage_machine()) ->
    mg_storage:opaque().
storage_machine_to_opaque(#{status := Status, state := State }) ->
    [1, machine_status_to_opaque(Status), State].

-spec opaque_to_storage_machine(mg_storage:opaque()) ->
    storage_machine().
opaque_to_storage_machine([1, Status, State]) ->
    #{status => opaque_to_machine_status(Status), state => State}.

-spec machine_status_to_opaque(machine_status()) ->
    mg_storage:opaque().
machine_status_to_opaque(Status) ->
    Opaque =
        case Status of
             sleeping                  ->  1;
            {waiting, TS}              -> [2, TS];
             processing                ->  3;
            % TODO подумать как упаковывать reason
            {error, Reason, OldStatus} -> [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)]
        end,
    Opaque.

-spec opaque_to_machine_status(mg_storage:opaque()) ->
    machine_status().
opaque_to_machine_status(Opaque) ->
    case Opaque of
         1                     ->  sleeping;
        [2, TS               ] -> {waiting, TS};
         3                     ->  processing;
        [4, Reason, OldStatus] -> {error, erlang:binary_to_term(Reason), opaque_to_machine_status(OldStatus)};
        % устаревшее
        [4, Reason           ] -> {error, erlang:binary_to_term(Reason), sleeping}
    end.


%%
%% indexes
%%
-define(status_idx , {integer, <<"status"      >>}).
-define(waiting_idx, {integer, <<"waiting_date">>}).

-spec storage_search_query(search_query()) ->
    mg_storage:index_query().
storage_search_query(sleeping) ->
    {?status_idx, 1};
storage_search_query(waiting) ->
    {?status_idx, 2};
storage_search_query({waiting, FromTs, ToTs}) ->
    {?waiting_idx, {FromTs, ToTs}};
storage_search_query(processing) ->
    {?status_idx, 3};
storage_search_query(failed) ->
    {?status_idx, 4}.

-spec storage_machine_to_indexes(storage_machine()) ->
    [mg_storage:index_update()].
storage_machine_to_indexes(#{status := Status}) ->
    status_index(Status) ++ waiting_date_index(Status).

-spec status_index(machine_status()) ->
    [mg_storage:index_update()].
status_index(Status) ->
    StatusInt =
        case Status of
             sleeping     -> 1;
            {waiting, _}  -> 2;
             processing   -> 3;
            {error, _, _} -> 4
        end,
    [{?status_idx, StatusInt}].

-spec waiting_date_index(machine_status()) ->
    [mg_storage:index_update()].
waiting_date_index({waiting, Timestamp}) ->
    [{?waiting_idx, Timestamp}];
waiting_date_index(_) ->
    [].

%%
%% processing
%%
-spec try_finish_processing(state()) ->
    state().
try_finish_processing(State = #{storage_machine := #{status := processing}}) ->
    process(continuation, undefined, State);
try_finish_processing(State = #{storage_machine := _}) ->
    State.

-spec process_start(term(), processing_context(), state()) ->
    state().
process_start(Args, ProcessingCtx, State) ->
    process({init, Args}, ProcessingCtx, State#{storage_machine := new_storage_machine()}).

-spec process_simple_repair(state()) ->
    state().
process_simple_repair(State = #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}}) ->
    transit_state(
        StorageMachine#{status => OldStatus},
        State
    ).

-spec process(processor_impact(), processing_context(), state()) ->
    state().
process(Impact, ProcessingCtx, State) ->
    try
        process_unsafe(Impact, ProcessingCtx, State)
    catch Class:Reason ->
        ok = do_reply_action({reply, {error, machine_failed}}, ProcessingCtx),
        handle_exception({Class, Reason, erlang:get_stacktrace()}, Impact, State)
    end.

-spec handle_exception(mg_utils:exception(), processor_impact(), state()) ->
    state().
handle_exception(Exception, Impact, State=#{options:=Options, id:=ID, storage_machine := StorageMachine}) ->
    ok = log_machine_error(namespace(Options), ID, Exception),
    #{status := OldStatus} = StorageMachine,
    case {Impact, OldStatus} of
        {{init, _}, _} ->
            State#{storage_machine := undefined};
        {_, {error, _, _}} ->
            State;
        {_, _} ->
            NewStorageMachine = StorageMachine#{ status => {error, Exception, OldStatus} },
            transit_state(NewStorageMachine, State)
    end.

-spec process_unsafe(processor_impact(), processing_context(), state()) ->
    state().
process_unsafe(Impact, ProcessingCtx, State=#{storage_machine := StorageMachine}) ->
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, State),
    NewStorageMachine = StorageMachine#{state := NewMachineState},
    case Action of
        {continue, NewProcessingSubState} ->
            NewState = transit_state(NewStorageMachine#{status := processing}, State),
            ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
            NewRequestCtx = ProcessingCtx#{state:=NewProcessingSubState},
            process(continuation, NewRequestCtx, NewState);
        sleep ->
            NewState = transit_state(NewStorageMachine#{status := sleeping}, State),
            ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
            NewState;
        {wait, Timestamp} ->
            NewState = transit_state(NewStorageMachine#{status := {waiting, Timestamp}}, State),
            ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
            NewState
    end.

-spec call_processor(processor_impact(), processing_context(), state()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, State) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = State,
    F = fun() ->
            processor_process_machine(ID, Impact, ProcessingCtx, MachineState, Options)
        end,
    RetryStrategy = mg_utils:genlib_retry_new(get_options(processor_retry_policy, Options)),
    do_with_retry(namespace(Options), ID, F, RetryStrategy).

-spec processor_process_machine(mg:id(), processor_impact(), processing_context(), state(), options()) ->
    _Result.
processor_process_machine(ID, Impact, ProcessingCtx, MachineState, Options) ->
    mg_utils:apply_mod_opts(
        get_options(processor, Options),
        process_machine,
        [ID, Impact, ProcessingCtx, MachineState]
    ).

-spec processor_child_spec(options()) ->
    mg_utils_supervisor_wrapper:child_spec().
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(get_options(processor, Options), processor_child_spec, empty_child_spec).

-spec do_reply_action(processor_reply_action(), undefined | processing_context()) ->
    ok.
do_reply_action(noreply, _) ->
    ok;
do_reply_action({reply, Reply}, ProcessingCtx) ->
    ok = reply(ProcessingCtx, Reply),
    ok.

-spec wrap_reply_action(_, processor_reply_action()) ->
    processor_reply_action().
wrap_reply_action(_, noreply) ->
    noreply;
wrap_reply_action(Wrapper, {reply, R}) ->
    {reply, {Wrapper, R}}.


-spec transit_state(storage_machine(), state()) ->
    state().
transit_state(NewStorageMachine, State=#{storage_machine := OldStorageMachine})
    when NewStorageMachine =:= OldStorageMachine
->
    State;
transit_state(NewStorageMachine, State=#{id:=ID, options:=Options, storage_context := StorageContext}) ->
    F = fun() ->
            mg_storage:put(
                storage_options(Options),
                ID,
                StorageContext,
                storage_machine_to_opaque(NewStorageMachine),
                storage_machine_to_indexes(NewStorageMachine)
            )
        end,
    RetryStrategy = mg_utils:genlib_retry_new(get_options(storage_retry_policy, Options)),
    NewStorageContext  = do_with_retry(namespace(Options), ID, F, RetryStrategy),
    State#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

%%

-spec manager_options(options()) ->
    mg_workers_manager:options().
manager_options(Options) ->
    #{
        name           => maps:get(namespace, Options),
        worker_options => #{
            worker => {?MODULE, Options}
        }
    }.

-spec storage_options(options()) ->
    mg_storage:options().
storage_options(Options) ->
    #{
        namespace => get_options(namespace, Options),
        module    => get_options(storage  , Options)
    }.

-spec timers_cron_options(options()) ->
    mg_cron:options().
timers_cron_options(Options) ->
    #{
        interval => 1000, % 1 sec
        job      => {?MODULE, handle_timers, [Options]}
    }.

-spec overseer_cron_options(options()) ->
    mg_cron:options().
overseer_cron_options(Options) ->
    #{
        interval => 1000, % 1 sec
        job      => {?MODULE, reload_killed_machines, [Options]}
    }.

-spec namespace(options()) ->
    mg:ns().
namespace(Options) ->
    get_options(namespace, Options).

-spec get_options(atom(), options()) ->
    _.
get_options(Subj=storage_retry_policy, Options) ->
    maps:get(Subj, Options, default_retry_policy());
get_options(Subj=processor_retry_policy, Options) ->
    maps:get(Subj, Options, default_retry_policy());
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec default_retry_policy() ->
    _.
default_retry_policy() ->
    {exponential, infinity, 2, 10, 60 * 1000}.

%%
%% retrying
%%
-spec do_with_retry(mg:ns(), mg:id(), fun(() -> R), genlib_retry:strategy()) ->
    R.
do_with_retry(NS, ID, Fun, RetryStrategy) ->
    try
        Fun()
    catch throw:(Reason={transient, _}) ->
        Exception = {throw, Reason, erlang:get_stacktrace()},
        ok = log_transient_exception(NS, ID, Exception),
        case genlib_retry:next_step(RetryStrategy) of
            {wait, Timeout, NewRetryStrategy} ->
                ok = log_retry(NS, ID, Timeout),
                ok = timer:sleep(Timeout),
                do_with_retry(NS, ID, Fun, NewRetryStrategy);
            finish ->
                throw({timeout, {retry_timeout, Reason}})
        end
    end.

%%
%% logging
%%
-spec log_load_error(mg:ns(), mg:id(), mg_utils:exception()) ->
    ok.
log_load_error(NS, ID, Exception) ->
    ok = error_logger:error_msg("[~s:~s] loading failed ~p", [NS, ID, Exception]).

-spec log_machine_error(mg:ns(), mg:id(), mg_utils:exception()) ->
    ok.
log_machine_error(NS, ID, Exception) ->
    ok = error_logger:error_msg("[~s:~s] processing failed ~p", [NS, ID, Exception]).

-spec log_transient_exception(mg:ns(), mg:id(), mg_utils:exception()) ->
    ok.
log_transient_exception(NS, ID, Exception) ->
    ok = error_logger:warning_msg("[~s:~s] retryable error ~p", [NS, ID, Exception]).

-spec log_retry(mg:ns(), mg:id(), Timeout::pos_integer()) ->
    ok.
log_retry(NS, ID, RetryTimeout) ->
    ok = error_logger:warning_msg("[~s:~s] retrying in ~p msec", [NS, ID, RetryTimeout]).

-spec log_failed_timer_handling(mg:ns(), mg:id(), _Reason) ->
    ok.
log_failed_timer_handling(NS, ID, Reason) ->
    ok = error_logger:warning_msg("[~s:~s] timer handling failed ~p", [NS, ID, Reason]).

-spec log_failed_touch(mg:ns(), mg:id(), _Reason) ->
    ok.
log_failed_touch(NS, ID, Reason) ->
    ok = error_logger:warning_msg("[~s:~s] touch failed ~p", [NS, ID, Reason]).
