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

-export([start               /5]).
-export([simple_repair       /4]).
-export([repair              /5]).
-export([call                /5]).
-export([fail                /4]).
-export([fail                /5]).
-export([get                 /2]).
-export([search              /2]).
-export([reply               /2]).
-export([call_with_lazy_start/6]).

%% Internal API
-export([handle_timers         /1]).
-export([handle_timers         /2]).
-export([resume_interrupted    /1]).
-export([resume_interrupted    /2]).
-export([resume_interrupted_one/2]).
-export([all_statuses          /0]).
-export([manager_options       /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/3, handle_call/4, handle_unload/1]).

%%
%% API
%%
-type options() :: #{
    namespace              => mg:ns(),
    storage                => mg_storage:storage          (),
    processor              => mg_utils:mod_opts           (),
    storage_retry_policy   => mg_utils:genlib_retry_policy(), % optional
    processor_retry_policy => mg_utils:genlib_retry_policy(), % optional
    logger                 => mg_machine_logger:handler   ()
}.

-type thrown_error() :: logic_error() | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason}.

-type throws() :: no_return().
-type machine_state() :: mg_storage:opaque().

%%

-type processing_state() :: term().
% контест обработки, сбрасывается при выгрузке машины
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
-type processor_flow_action() ::
       sleep
    | {wait, genlib_time:ts(), mg:request_context(), HandlingTimeout::pos_integer()}
    | {continue, processing_state()}
    |  remove
.
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.

-callback processor_child_spec(_Options) ->
    mg_utils_supervisor_wrapper:child_spec().
-callback process_machine(_Options, mg:id(), processor_impact(), processing_context(), ReqCtx, machine_state()) ->
    processor_result()
when ReqCtx :: mg:request_context().
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

-spec start(options(), mg:id(), term(), mg:request_context(), mg_utils:deadline()) ->
    _Resp | throws().
start(Options, ID, Args, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(call_(Options, ID, {start, Args}, ReqCtx, Deadline)).

-spec simple_repair(options(), mg:id(), mg:request_context(), mg_utils:deadline()) ->
    _Resp | throws().
simple_repair(Options, ID, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(call_(Options, ID, simple_repair, ReqCtx, Deadline)).

-spec repair(options(), mg:id(), term(), mg:request_context(), mg_utils:deadline()) ->
    _Resp | throws().
repair(Options, ID, Args, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(call_(Options, ID, {repair, Args}, ReqCtx, Deadline)).

-spec call(options(), mg:id(), term(), mg:request_context(), mg_utils:deadline()) ->
    _Resp | throws().
call(Options, ID, Call, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(call_(Options, ID, {call, Call}, ReqCtx, Deadline)).

-spec fail(options(), mg:id(), mg:request_context(), mg_utils:deadline()) ->
    ok.
fail(Options, ID, ReqCtx, Deadline) ->
    fail(Options, ID, {error, explicit_fail, []}, ReqCtx, Deadline).

-spec fail(options(), mg:id(), mg_utils:exception(), mg:request_context(), mg_utils:deadline()) ->
    ok.
fail(Options, ID, Exception, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(call_(Options, ID, {fail, Exception}, ReqCtx, Deadline)).

-spec get(options(), mg:id()) ->
    machine_state() | throws().
get(Options, ID) ->
    #{state:=State} =
        mg_utils:throw_if_undefined(element(2, get_storage_machine(Options, ID)), machine_not_found),
    State.

-spec search(options(), search_query()) ->
    [{_TODO, mg:id()}] | [mg:id()] | throws().
search(Options, Query) ->
    % TODO deadline
    mg_storage:search(storage_options(Options), storage_search_query(Query)).

-spec call_with_lazy_start(options(), mg:id(), term(), mg:request_context(), mg_utils:deadline(), term()) ->
    _Resp | throws().
call_with_lazy_start(Options, ID, Call, ReqCtx, Deadline, StartArgs) ->
    try
        call(Options, ID, Call, ReqCtx, Deadline)
    catch throw:machine_not_found ->
        try
            _ = start(Options, ID, StartArgs, ReqCtx, Deadline)
        catch throw:machine_already_exist ->
            % вдруг кто-то ещё делает аналогичный процесс
            ok
        end,
        % если к этому моменту машина не создалась, значит она уже не создастся
        % и исключение будет оправданным
        call(Options, ID, Call, ReqCtx, Deadline)
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
-define(SAFE(Desc, Expr),
    try
        Expr
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        ok = error_logger:error_msg("error in ~s handler:~n~p", [Desc, Exception])
    end
).
-spec handle_timers(options()) ->
    ok.
handle_timers(Options) ->
    % TODO можно будет убрать возврат тела индекса
    Timers = search(Options, {waiting, 1, genlib_time:now()}),
    handle_timers(Options, Timers).

-spec handle_timers(options(), [{genlib_time:ts(), mg:id()}]) ->
    ok.
handle_timers(Options, Timers) ->
    lists:foreach(
        % такая схема потенциально опасная, но надо попробовать как она себя будет вести
        fun({_, ID}) ->
            erlang:spawn_link(fun() -> ?SAFE(handle_timers, handle_timer(Options, ID)) end)
        end,
        Timers
    ).

-spec handle_timer(options(), mg:id()) ->
    ok.
handle_timer(Options, ID) ->
    {_, StorageMachine} = get_storage_machine(Options, ID),
    case StorageMachine of
        #{status := {waiting, Timestamp, ReqCtx, Timeout}} ->
            handle_timer(Options, ID, Timestamp, ReqCtx, mg_utils:timeout_to_deadline(Timeout));
        #{status := _} ->
            ok
    end.

-spec handle_timer(options(), mg:id(), genlib_time:ts(), mg:request_context(), mg_utils:deadline()) ->
    ok.
handle_timer(Options, ID, Timestamp, ReqCtx, Deadline) ->
    Call = {timeout, Timestamp},
    CallQueue = mg_workers_manager:get_call_queue(manager_options(Options), ID),
    case lists:member(Call, CallQueue) of
        false ->
            case call_(Options, ID, Call, ReqCtx, Deadline) of
                {ok, ok} ->
                    ok;
                {error, Reason} ->
                    ok = emit_log_event(Options, ID, ReqCtx, {timer_handling_failed, Reason})
            end;
        true ->
            ok
    end.

-spec resume_interrupted(options()) ->
    ok.
resume_interrupted(Options) ->
    Interrupted = search(Options, processing),
    resume_interrupted(Options, Interrupted).

-spec resume_interrupted(options(), [mg:id()]) ->
    ok.
resume_interrupted(Options, MachinesIDs) ->
    lists:foreach(
        fun(ID) ->
            erlang:spawn_link(fun() -> ?SAFE(resume_interrupted, resume_interrupted_one(Options, ID)) end)
        end,
        MachinesIDs
    ).

-spec resume_interrupted_one(options(), mg:id()) ->
    ok.
resume_interrupted_one(Options, ID) ->
    case mg_workers_manager:is_alive(manager_options(Options), ID) of
        false ->
            Deadline = mg_utils:timeout_to_deadline(30000),
            {_, StorageMachine} = get_storage_machine(Options, ID),
            case StorageMachine of
                #{status := {processing, ReqCtx}} ->
                    resume_interrupted_one(Options, ID, ReqCtx, Deadline);
                #{status := _} ->
                    ok
            end;
        true ->
            ok
    end.

-spec resume_interrupted_one(options(), mg:id(), mg:request_context(), mg_utils:deadline()) ->
    ok.
resume_interrupted_one(Options, ID, ReqCtx, Deadline) ->
    case call_(Options, ID, resume_interrupted_one, ReqCtx, Deadline) of
        {ok, ok} ->
            ok;
        {error, Reason} ->
            ok = emit_log_event(Options, ID, ReqCtx, {resuming_interrupted_failed, Reason})
    end.

-spec all_statuses() ->
    [atom()].
all_statuses() ->
    [sleeping, waiting, processing, failed].

-spec call_(options(), mg:id(), _, mg:request_context(), mg_utils:deadline()) ->
    ok | {ok, _} | {error, _}.
call_(Options, ID, Call, ReqCtx, Deadline) ->
    mg_workers_manager:call(manager_options(Options), ID, Call, ReqCtx, Deadline).

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
    status => machine_status(),
    state  => machine_state()
}.

-type machine_regular_status() ::
       sleeping
    | {waiting, genlib_time:ts(), mg:request_context(), HandlingTimeout::pos_integer()}
    | {processing, mg:request_context()}
.
-type machine_status() :: machine_regular_status() | {error, _, machine_regular_status()}.

-spec handle_load(_ID, options(), mg:request_context()) ->
    {ok, state()}.
handle_load(ID, Options, ReqCtx) ->
    try
        {StorageContext, StorageMachine} = get_storage_machine(Options, ID),
        State =
            #{
                id              => ID,
                options         => Options,
                storage_machine => StorageMachine,
                storage_context => StorageContext
            },
        {ok, State}
    catch throw:Reason ->
        Exception = {throw, Reason, erlang:get_stacktrace()},
        ok = emit_log_event(Options, ID, ReqCtx, {loading_failed, Exception}),
        {error, Reason}
    end.

-spec handle_call(_Call, mg_worker:call_context(), mg:request_context(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, ReqCtx, S=#{storage_machine:=StorageMachine}) ->
    PCtx = new_processing_context(CallContext),

    % довольно сложное место, тут определяется приоритет реакции на внешние раздражители, нужно быть аккуратнее
    case {Call, StorageMachine} of
        % start
        {{start, Args}, undefined   } -> {noreply, process_start(Args, PCtx, ReqCtx, S)};
        { _           , undefined   } -> {{reply, {error, machine_not_found    }}, S};
        {{start, _   }, #{status:=_}} -> {{reply, {error, machine_already_exist}}, S};

        % fail
        {{fail, Exception}, _} -> {{reply, ok}, handle_exception(Exception, undefined, ReqCtx, S)};

        % сюда мы не должны попадать если машина не падала во время обработки запроса
        % (когда мы переходили в стейт processing)
        {_, #{status := processing}} ->
            handle_call(Call, CallContext, ReqCtx, process(continuation, undefined, ReqCtx, S));

        % ничего не просходит, просто убеждаемся, что машина загружена
        {resume_interrupted_one, _} -> {{reply, {ok, ok}}, S};

        % call
        {{call  , SubCall}, #{status:= sleeping         }} -> {noreply, process({call, SubCall}, PCtx, ReqCtx, S)};
        {{call  , SubCall}, #{status:={waiting, _, _, _}}} -> {noreply, process({call, SubCall}, PCtx, ReqCtx, S)};
        {{call  , _      }, #{status:={error  , _, _   }}} -> {{reply, {error, machine_failed}}, S};

        % repair
        {{repair, Args}, #{status:={error  , _, _}}} -> {noreply, process({repair, Args}, PCtx, ReqCtx, S)};
        {{repair, _   }, #{status:=_              }} -> {{reply, {error, machine_already_working}}, S};

        % simple_repair
        {simple_repair, #{status:={error  , _, _}}} -> {{reply, ok}, process_simple_repair(ReqCtx, S)};
        {simple_repair, #{status:=_              }} -> {{reply, {error, machine_already_working}}, S};

        % timers
        {{timeout, Ts0}, #{status:={waiting, Ts1, _, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, ReqCtx, S)};
        {{timeout, _}, #{status:=_}} ->
            {{reply, {ok, ok}}, S}
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
             sleeping                    ->  1;
            {waiting, TS, ReqCtx, HdlTo} -> [2, TS, ReqCtx, HdlTo];
            {processing, ReqCtx}         -> [3, ReqCtx];
            % TODO подумать как упаковывать reason
            {error, Reason, OldStatus}   -> [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)]
        end,
    Opaque.

-spec opaque_to_machine_status(mg_storage:opaque()) ->
    machine_status().
opaque_to_machine_status(Opaque) ->
    case Opaque of
         1                     ->  sleeping;
        [2, TS               ] -> {waiting, TS, null, 30000}; % совместимость со старой версией
        [2, TS, ReqCtx, HdlTo] -> {waiting, TS, ReqCtx, HdlTo};
         3                     -> {processing, null}; % совместимость со старой версией
        [3, ReqCtx           ] -> {processing, ReqCtx};
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
             sleeping             -> 1;
            {waiting   , _, _, _} -> 2;
            {processing, _      } -> 3;
            {error     , _, _   } -> 4
        end,
    [{?status_idx, StatusInt}].

-spec waiting_date_index(machine_status()) ->
    [mg_storage:index_update()].
waiting_date_index({waiting, Timestamp, _, _}) ->
    [{?waiting_idx, Timestamp}];
waiting_date_index(_) ->
    [].

%%
%% processing
%%
-spec process_start(term(), processing_context(), mg:request_context(), state()) ->
    state().
process_start(Args, ProcessingCtx, ReqCtx, State) ->
    process({init, Args}, ProcessingCtx, ReqCtx, State#{storage_machine := new_storage_machine()}).

-spec process_simple_repair(mg:request_context(), state()) ->
    state().
process_simple_repair(ReqCtx, State = #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}}) ->
    transit_state(
        ReqCtx,
        StorageMachine#{status => OldStatus},
        State
    ).

-spec process(processor_impact(), processing_context(), mg:request_context(), state()) ->
    state().
process(Impact, ProcessingCtx, ReqCtx, State) ->
    try
        process_unsafe(Impact, ProcessingCtx, ReqCtx, State)
    catch Class:Reason ->
        ok = do_reply_action({reply, {error, machine_failed}}, ProcessingCtx),
        handle_exception({Class, Reason, erlang:get_stacktrace()}, Impact, ReqCtx, State)
    end.

-spec handle_exception(mg_utils:exception(), processor_impact() | undefined, mg:request_context(), state()) ->
    state().
handle_exception(Exception, Impact, ReqCtx, State) ->
    #{options := Options, id := ID, storage_machine := StorageMachine} = State,
    ok = emit_log_event(Options, ID, ReqCtx, {machine_failed, Exception}),
    #{status := OldStatus} = StorageMachine,
    case {Impact, OldStatus} of
        {{init, _}, _} ->
            State#{storage_machine := undefined};
        {_, {error, _, _}} ->
            State;
        {_, _} ->
            NewStorageMachine = StorageMachine#{status => {error, Exception, OldStatus}},
            transit_state(ReqCtx, NewStorageMachine, State)
    end.

-spec process_unsafe(processor_impact(), processing_context(), mg:request_context(), state()) ->
    state().
process_unsafe(Impact, ProcessingCtx, ReqCtx, State=#{storage_machine := StorageMachine}) ->
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, ReqCtx, State),
    NewStorageMachine = StorageMachine#{state := NewMachineState},
    NewState =
        case Action of
            {continue, _} ->
                transit_state(ReqCtx, NewStorageMachine#{status := {processing, ReqCtx}}, State);
            sleep ->
                transit_state(ReqCtx, NewStorageMachine#{status := sleeping}, State);
            {wait, Timestamp, HdlReqCtx, HdlTo} ->
                transit_state(ReqCtx, NewStorageMachine#{status := {waiting, Timestamp, HdlReqCtx, HdlTo}}, State);
            remove ->
                remove_from_storage(ReqCtx, State)
        end,
    ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
    case Action of
        {continue, NewProcessingSubState} ->
            process(continuation, ProcessingCtx#{state:=NewProcessingSubState}, ReqCtx, NewState);
        _ ->
            NewState
    end.

-spec call_processor(processor_impact(), processing_context(), mg:request_context(), state()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, ReqCtx, State) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = State,
    F = fun() ->
            processor_process_machine(ID, Impact, ProcessingCtx, ReqCtx, MachineState, Options)
        end,
    RetryStrategy = mg_utils:genlib_retry_new(get_options(processor_retry_policy, Options)),
    do_with_retry(Options, ID, F, RetryStrategy, ReqCtx).

-spec processor_process_machine(mg:id(), processor_impact(), processing_context(), ReqCtx, state(), options()) ->
    _Result
when ReqCtx :: mg:request_context().
processor_process_machine(ID, Impact, ProcessingCtx, ReqCtx, MachineState, Options) ->
    mg_utils:apply_mod_opts(
        get_options(processor, Options),
        process_machine,
        [ID, Impact, ProcessingCtx, ReqCtx, MachineState]
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


-spec transit_state(mg:request_context(), storage_machine(), state()) ->
    state().
transit_state(_ReqCtx, NewStorageMachine, State=#{storage_machine := OldStorageMachine})
    when NewStorageMachine =:= OldStorageMachine
->
    State;
transit_state(ReqCtx, NewStorageMachine, State=#{id:=ID, options:=Options, storage_context := StorageContext}) ->
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
    NewStorageContext  = do_with_retry(Options, ID, F, RetryStrategy, ReqCtx),
    State#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

-spec remove_from_storage(mg:request_context(), state()) ->
    state().
remove_from_storage(ReqCtx, State = #{id := ID, options := Options, storage_context := StorageContext}) ->
    F = fun() ->
            mg_storage:delete(
                storage_options(Options),
                ID,
                StorageContext
            )
        end,
    RetryStrategy = mg_utils:genlib_retry_new(get_options(storage_retry_policy, Options)),
    ok = do_with_retry(Options, ID, F, RetryStrategy, ReqCtx),
    State#{storage_machine := undefined, storage_context := undefined}.

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
        job      => {?MODULE, resume_interrupted, [Options]}
    }.

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
-spec do_with_retry(options(), mg:id(), fun(() -> R), genlib_retry:strategy(), mg:request_context()) ->
    R.
do_with_retry(Options, ID, Fun, RetryStrategy, ReqCtx) ->
    try
        Fun()
    catch throw:(Reason={transient, _}) ->
        ok = emit_log_event(Options, ID, ReqCtx, {transient_error, {throw, Reason, erlang:get_stacktrace()}}),
        case genlib_retry:next_step(RetryStrategy) of
            {wait, Timeout, NewRetryStrategy} ->
                ok = emit_log_event(Options, ID, ReqCtx, {retrying, Timeout}),
                ok = timer:sleep(Timeout),
                do_with_retry(Options, ID, Fun, NewRetryStrategy, ReqCtx);
            finish ->
                throw({timeout, {retry_timeout, Reason}})
        end
    end.

%%
%% logging
%%
-spec emit_log_event(options(), mg:id(), mg:request_context(), mg_machine_logger:sub_event()) ->
    ok.
emit_log_event(#{namespace := NS, logger := Handler}, ID, ReqCtx, Event) ->
    ok = mg_machine_logger:handle_event(Handler, {NS, ID, ReqCtx, Event}).
