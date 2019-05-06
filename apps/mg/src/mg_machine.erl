%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

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
%%   - бизнес-логические -- logic
%%    - машина не найдена -- machine_not_found
%%    - машина уже существует -- machine_already_exist
%%    - машина находится в упавшем состоянии -- machine_failed
%%    - некорректная попытка повторно запланировать обработку события -- invalid_reschedule_request
%%    - что-то ещё?
%%   - временные -- transient
%%    - сервис перегружен —- overload
%%    - хранилище недоступно -- {storage_unavailable, Details}
%%    - процессор недоступен -- {processor_unavailable, Details}
%%    - исчерпаны попытки повтора -- {retries_exhausted, Error}
%%   - таймауты -- {timeout, Details}
%%   - окончательные -- permanent
%%    - исчерпаны попытки повтора обработки таймера -- timer_retries_exhausted
%%  - неожиданные
%%   - что-то пошло не так -- падение с любой другой ошибкой
%%
%% Например: throw:{logic, machine_not_found}, throw:{transient, {storage_unavailable, ...}}, error:badarg
%%
%% Если в процессе обработки внешнего запроса происходит ожидаемая ошибка, она мапится в код ответа,
%%  если неожидаемая ошибка, то запрос падает с internal_error, ошибка пишется в лог.
%%
%% Если в процессе обработки запроса машиной происходит ожидаемая ошибка, то она прокидывается вызывающему коду,
%%  если неожидаемая, то машина переходит в error состояние и в ответ возникает ошибка machine_failed.
%%
%% Хранилище и процессор кидают либо ошибку о недоступности, либо падают.
%%

-include_lib("mg/include/pulse.hrl").

%% API
-export_type([retry_opt             /0]).
-export_type([suicide_probability   /0]).
-export_type([scheduler_opt         /0]).
-export_type([schedulers_opt        /0]).
-export_type([options               /0]).
-export_type([thrown_error          /0]).
-export_type([logic_error           /0]).
-export_type([transient_error       /0]).
-export_type([throws                /0]).
-export_type([machine_state         /0]).
-export_type([machine_status        /0]).
-export_type([processor_impact      /0]).
-export_type([processing_context    /0]).
-export_type([processor_result      /0]).
-export_type([processor_reply_action/0]).
-export_type([processor_flow_action /0]).
-export_type([search_query          /0]).
-export_type([machine_regular_status/0]).

-export([child_spec /2]).
-export([start_link /1]).

-export([start               /5]).
-export([simple_repair       /4]).
-export([repair              /5]).
-export([call                /5]).
-export([send_timeout        /5]).
-export([send_retry_wait     /6]).
-export([resume_interrupted  /4]).
-export([fail                /4]).
-export([fail                /5]).
-export([get                 /2]).
-export([get_status          /2]).
-export([is_exist            /2]).
-export([search              /2]).
-export([search              /3]).
-export([search              /4]).
-export([reply               /2]).
-export([call_with_lazy_start/6]).

%% Internal API
-export([all_statuses          /0]).
-export([manager_options       /1]).
-export([get_storage_machine   /2]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/3, handle_call/5, handle_unload/1]).

-define(TIMERS, <<"timers">>).

%%
%% API
%%
-type scheduler_opt() :: disable | #{
    interval     => pos_integer(),
    no_task_wait => pos_integer(),
    limit        => mg_quota_worker:name(),
    share        => mg_quota:share()
}.
-type retry_subj() :: storage | processor | timers | continuation.
-type retry_opt() :: #{
    retry_subj()   => mg_retry:policy()
}.
-type schedulers_opt() :: #{
    timers           => scheduler_opt(),
    timers_retries   => scheduler_opt(),
    overseer         => scheduler_opt()
}.
-type suicide_probability() :: float() | integer() | undefined. % [0, 1]

-type options() :: #{
    namespace                => mg:ns(),
    storage                  => mg_storage:options(),
    processor                => mg_utils:mod_opts(),
    pulse                    => mg_pulse:handler(),
    retries                  => retry_opt(),
    schedulers               => schedulers_opt(),
    suicide_probability      => suicide_probability(),
    reschedule_timeout       => timeout(),
    timer_processing_timeout => timeout()
}.

-type thrown_error() :: {logic, logic_error()} | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason} | unavailable.

-type throws() :: no_return().
-type machine_state() :: mg_storage:opaque().

-type machine_regular_status() ::
       sleeping
    | {waiting, genlib_time:ts(), request_context(), HandlingTimeout::pos_integer()}
    | {retrying, Target::genlib_time:ts(), Start::genlib_time:ts(), Attempt::non_neg_integer(), request_context()}
    | {processing, request_context()}
.
-type machine_status() :: machine_regular_status() | {error, Reason::term(), machine_regular_status()}.

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
    | {wait, genlib_time:ts(), request_context(), HandlingTimeout::pos_integer()}
    | {continue, processing_state()}
    |  remove
.
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.
-type request_context() :: mg:request_context().

-type processor_retry() :: mg_retry:strategy() | undefined.

-callback processor_child_spec(_Options) ->
    supervisor:child_spec() | undefined.
-callback process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg:id(),
    Impact :: processor_impact(),
    PCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline(),
    MachineState :: machine_state(),
    Result :: processor_result().
-optional_callbacks([processor_child_spec/1]).

-type search_query() ::
       sleeping
    |  waiting
    | {waiting, From::genlib_time:ts(), To::genlib_time:ts()}
    |  retrying
    | {retrying, From::genlib_time:ts(), To::genlib_time:ts()}
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
        mg_utils:lists_compact([
            mg_storage:child_spec(storage_options(Options), storage, storage_reg_name(Options)),
            processor_child_spec(Options),
            mg_workers_manager:child_spec(manager_options(Options), manager),
            scheduler_child_spec(timers        , Options),
            scheduler_child_spec(timers_retries, Options),
            scheduler_child_spec(overseer      , Options)
        ])
    ).

-spec start(options(), mg:id(), term(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
start(Options, ID, Args, ReqCtx, Deadline) ->
    call_(Options, ID, {start, Args}, ReqCtx, Deadline).

-spec simple_repair(options(), mg:id(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
simple_repair(Options, ID, ReqCtx, Deadline) ->
    call_(Options, ID, simple_repair, ReqCtx, Deadline).

-spec repair(options(), mg:id(), term(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
repair(Options, ID, Args, ReqCtx, Deadline) ->
    call_(Options, ID, {repair, Args}, ReqCtx, Deadline).

-spec call(options(), mg:id(), term(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
call(Options, ID, Call, ReqCtx, Deadline) ->
    call_(Options, ID, {call, Call}, ReqCtx, Deadline).

-spec send_timeout(options(), mg:id(), genlib_time:ts(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
send_timeout(Options, ID, Timestamp, ReqCtx, Deadline) ->
    call_(Options, ID, {timeout, Timestamp}, ReqCtx, Deadline).

-spec send_retry_wait(Options, ID, Status, Timestamp, ReqCtx, Deadline) -> _Resp | throws() when
    Options :: options(),
    ID :: mg:id(),
    Status :: waiting | retrying,
    Timestamp :: genlib_time:ts(),
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline().
send_retry_wait(Options, ID, Status, Timestamp, ReqCtx, Deadline) ->
    call_(Options, ID, {retry_wait, {Status, Timestamp}}, ReqCtx, Deadline).

-spec resume_interrupted(options(), mg:id(), request_context(), mg_utils:deadline()) ->
    _Resp | throws().
resume_interrupted(Options, ID, ReqCtx, Deadline) ->
    call_(Options, ID, resume_interrupted_one, ReqCtx, Deadline).

-spec fail(options(), mg:id(), request_context(), mg_utils:deadline()) ->
    ok.
fail(Options, ID, ReqCtx, Deadline) ->
    fail(Options, ID, {error, explicit_fail, []}, ReqCtx, Deadline).

-spec fail(options(), mg:id(), mg_utils:exception(), request_context(), mg_utils:deadline()) ->
    ok.
fail(Options, ID, Exception, ReqCtx, Deadline) ->
    call_(Options, ID, {fail, Exception}, ReqCtx, Deadline).

-spec get(options(), mg:id()) ->
    machine_state() | throws().
get(Options, ID) ->
    {_, #{state := State}} =
        mg_utils:throw_if_undefined(get_storage_machine(Options, ID), {logic, machine_not_found}),
    State.

-spec get_status(options(), mg:id()) ->
    machine_status() | throws().
get_status(Options, ID) ->
    {_, #{status := Status}} =
        mg_utils:throw_if_undefined(get_storage_machine(Options, ID), {logic, machine_not_found}),
    Status.

-spec is_exist(options(), mg:id()) ->
    boolean() | throws().
is_exist(Options, ID) ->
    get_storage_machine(Options, ID) =/= undefined.

-spec search(options(), search_query(), mg_storage:index_limit(), mg_storage:continuation()) ->
    mg_storage:search_result() | throws().
search(Options, Query, Limit, Continuation) ->
    StorageQuery = storage_search_query(Query, Limit, Continuation),
    mg_storage:search(storage_options(Options), storage_ref(Options), StorageQuery).

-spec search(options(), search_query(), mg_storage:index_limit()) ->
    mg_storage:search_result() | throws().
search(Options, Query, Limit) ->
    mg_storage:search(storage_options(Options), storage_ref(Options), storage_search_query(Query, Limit)).

-spec search(options(), search_query()) ->
    mg_storage:search_result() | throws().
search(Options, Query) ->
    % TODO deadline
    mg_storage:search(storage_options(Options), storage_ref(Options), storage_search_query(Query)).

-spec call_with_lazy_start(options(), mg:id(), term(), request_context(), mg_utils:deadline(), term()) ->
    _Resp | throws().
call_with_lazy_start(Options, ID, Call, ReqCtx, Deadline, StartArgs) ->
    try
        call(Options, ID, Call, ReqCtx, Deadline)
    catch throw:{logic, machine_not_found} ->
        try
            _ = start(Options, ID, StartArgs, ReqCtx, Deadline)
        catch throw:{logic, machine_already_exist} ->
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
-define(DEFAULT_RETRY_POLICY, {exponential, infinity, 2, 10, 60 * 1000}).
-define(DEFAULT_TIMER_PROCESSING_TIMEOUT, 60000).
-define(DEFAULT_RESCHEDULING_TIMEOUT, 60000).

-define(can_be_retried(ErrorType), ErrorType =:= transient orelse ErrorType =:= timeout).

%%

-spec all_statuses() ->
    [atom()].
all_statuses() ->
    [sleeping, waiting, retrying, processing, failed].

-spec call_(options(), mg:id(), _, request_context(), mg_utils:deadline()) ->
    _ | no_return().
call_(Options, ID, Call, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, Call, ReqCtx, Deadline)).

%%
%% mg_worker callbacks
%%
-type state() :: #{
    id              => mg:id(),
    namespace       => mg:ns(),
    options         => options(),
    storage_machine => storage_machine() | undefined,
    storage_context => mg_storage:context() | undefined
}.

-type storage_machine() :: #{
    status => machine_status(),
    state  => machine_state()
}.

-spec handle_load(_ID, options(), request_context()) ->
    {ok, state()}.
handle_load(ID, Options, ReqCtx) ->
    Namespace = maps:get(namespace, Options),
    try
        {StorageContext, StorageMachine} =
            case get_storage_machine(Options, ID) of
                undefined -> {undefined, undefined};
                V         -> V
            end,

        State =
            #{
                id              => ID,
                namespace       => Namespace,
                options         => Options,
                storage_machine => StorageMachine,
                storage_context => StorageContext
            },
        ok = emit_machine_load_beat(Options, Namespace, ID, ReqCtx, StorageMachine),
        {ok, State}
    catch throw:Reason:ST ->
        Exception = {throw, Reason, ST},
        ok = emit_beat(Options, #mg_machine_lifecycle_loading_error{
            namespace = Namespace,
            machine_id = ID,
            request_context = ReqCtx,
            exception = Exception
        }),
        {error, Reason}
    end.

-spec handle_call(_Call, mg_worker:call_context(), request_context(), mg_utils:deadline(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, ReqCtx, Deadline, S=#{storage_machine:=StorageMachine}) ->
    PCtx = new_processing_context(CallContext),

    % довольно сложное место, тут определяется приоритет реакции на внешние раздражители, нужно быть аккуратнее
    case {Call, StorageMachine} of
        % start
        {{start, Args}, undefined   } -> {noreply, process({init, Args}, PCtx, ReqCtx, Deadline, S)};
        { _           , undefined   } -> {{reply, {error, {logic, machine_not_found    }}}, S};
        {{start, _   }, #{status:=_}} -> {{reply, {error, {logic, machine_already_exist}}}, S};

        % fail
        {{fail, Exception}, _} -> {{reply, ok}, handle_exception(Exception, undefined, ReqCtx, Deadline, S)};

        % сюда мы не должны попадать если машина не падала во время обработки запроса
        % (когда мы переходили в стейт processing)
        {_, #{status := {processing, ProcessingReqCtx}}} ->
            % обработка машин в стейте processing идёт без дедлайна
            % машина должна либо упасть, либо перейти в другое состояние
            S1 = process(continuation, undefined, ProcessingReqCtx, undefined, S),
            handle_call(Call, CallContext, ReqCtx, Deadline, S1);

        % ничего не просходит, просто убеждаемся, что машина загружена
        {resume_interrupted_one, _} -> {{reply, {ok, ok}}, S};

        % call
        {{call  , SubCall}, #{status:= sleeping         }} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , SubCall}, #{status:={waiting, _, _, _}}} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , SubCall}, #{status:={retrying, _, _, _, _}}} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , _      }, #{status:={error  , _, _   }}} -> {{reply, {error, {logic, machine_failed}}}, S};

        % repair
        {{repair, Args}, #{status:={error  , _, _}}} -> {noreply, process({repair, Args}, PCtx, ReqCtx, Deadline, S)};
        {{repair, _   }, #{status:=_              }} -> {{reply, {error, {logic, machine_already_working}}}, S};

        % simple_repair
        {simple_repair, #{status:={error  , _, _}}} -> {{reply, ok}, process_simple_repair(ReqCtx, Deadline, S)};
        {simple_repair, #{status:=_              }} -> {{reply, {error, {logic, machine_already_working}}}, S};

        % timers
        {{timeout, Ts0}, #{status:={waiting, Ts1, _, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, ReqCtx, Deadline, S)};
        {{timeout, Ts0}, #{status:={retrying, Ts1, _, _, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, ReqCtx, Deadline, S)};
        {{timeout, _}, #{status:=_}} ->
            {{reply, {ok, ok}}, S};

        {{retry_wait, {waiting, Ts0}}, #{status:={waiting, Ts1, _, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, reschedule(PCtx, ReqCtx, Deadline, S)};
        {{retry_wait, {retrying, Ts0}}, #{status:={retrying, Ts1, _, _, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, reschedule(PCtx, ReqCtx, Deadline, S)};
        {{retry_wait, _}, #{status:=_}} ->
            {{reply, {error, {logic, invalid_reschedule_request}}}, S}
    end.

-spec handle_unload(state()) ->
    ok.
handle_unload(State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_machine_lifecycle_unloaded{
        namespace = NS,
        machine_id = ID
    }).

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
    try mg_storage:get(storage_options(Options), storage_ref(Options), ID) of
        undefined ->
            undefined;
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(PackedMachine)}
    catch
        throw:{logic, {invalid_key, _StorageDetails} = Details} ->
            throw({logic, {invalid_machine_id, Details}})
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
            {error, Reason, OldStatus}   -> [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)];
            {retrying, TS, StartTS, Attempt, ReqCtx} -> [5, TS, StartTS, Attempt, ReqCtx]
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
        [4, Reason           ] -> {error, erlang:binary_to_term(Reason), sleeping};
        [5, TS, StartTS, Attempt, ReqCtx] -> {retrying, TS, StartTS, Attempt, ReqCtx}
    end.


%%
%% indexes
%%
-define(status_idx , {integer, <<"status"      >>}).
-define(waiting_idx, {integer, <<"waiting_date">>}).
-define(retrying_idx, {integer, <<"retrying_date">>}).

-spec storage_search_query(search_query(), mg_storage:index_limit(), mg_storage:continuation()) ->
    mg_storage:index_query().
storage_search_query(Query, Limit, Continuation) ->
    erlang:append_element(storage_search_query(Query, Limit), Continuation).

-spec storage_search_query(search_query(), mg_storage:index_limit()) ->
    mg_storage:index_query().
storage_search_query(Query, Limit) ->
    erlang:append_element(storage_search_query(Query), Limit).

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
    {?status_idx, 4};
storage_search_query(retrying) ->
    {?status_idx, 5};
storage_search_query({retrying, FromTs, ToTs}) ->
    {?retrying_idx, {FromTs, ToTs}}.

-spec storage_machine_to_indexes(storage_machine()) ->
    [mg_storage:index_update()].
storage_machine_to_indexes(#{status := Status}) ->
    status_index(Status) ++ status_range_index(Status).

-spec status_index(machine_status()) ->
    [mg_storage:index_update()].
status_index(Status) ->
    StatusInt =
        case Status of
             sleeping                -> 1;
            {waiting   , _, _, _   } -> 2;
            {processing, _         } -> 3;
            {error     , _, _      } -> 4;
            {retrying  , _, _, _, _} -> 5
        end,
    [{?status_idx, StatusInt}].

-spec status_range_index(machine_status()) ->
    [mg_storage:index_update()].
status_range_index({waiting, Timestamp, _, _}) ->
    [{?waiting_idx, Timestamp}];
status_range_index({retrying, Timestamp, _, _, _}) ->
    [{?retrying_idx, Timestamp}];
status_range_index(_) ->
    [].

%%
%% processing
%%

-spec process_simple_repair(request_context(), mg_utils:deadline(), state()) ->
    state().
process_simple_repair(ReqCtx, Deadline, State) ->
    #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}} = State,
    transit_state(
        ReqCtx,
        Deadline,
        StorageMachine#{status => OldStatus},
        State
    ).

-spec process(processor_impact(), processing_context(), request_context(), mg_utils:deadline(), state()) ->
    state().
process(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    RetryStrategy = get_impact_retry_strategy(Impact, Deadline, State),
    try
        process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy)
    catch
        Class:Reason:ST ->
            ok = do_reply_action({reply, {error, {logic, machine_failed}}}, ProcessingCtx),
            handle_exception({Class, Reason, ST}, Impact, ReqCtx, Deadline, State)
    end.

-spec process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, Retry) -> State when
    Impact :: processor_impact(),
    ProcessingCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline(),
    State :: state(),
    Retry :: processor_retry().
process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy) ->
    #{id := ID, namespace := NS, options := Opts} = State,
    try
        process_unsafe(Impact, ProcessingCtx, ReqCtx, Deadline, try_init_state(Impact, State))
    catch
        throw:(Reason=({ErrorType, _Details})):ST when ?can_be_retried(ErrorType) ->
            ok = emit_beat(Opts, #mg_machine_process_transient_error{
                namespace = NS,
                machine_id = ID,
                exception = {throw, Reason, ST},
                request_context = ReqCtx
            }),
            ok = do_reply_action({reply, {error, Reason}}, ProcessingCtx),
            case process_retry_next_step(RetryStrategy) of
                ignore ->
                    State;
                finish ->
                    erlang:throw({permanent, {retries_exhausted, Reason}});
                {wait, Timeout, NewRetryStrategy} ->
                    ok = timer:sleep(Timeout),
                    process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, NewRetryStrategy)
            end
    end.

-spec process_retry_next_step(processor_retry()) ->
    {wait, timeout(), mg_retry:strategy()} | finish | ignore.
process_retry_next_step(undefined) ->
    ignore;
process_retry_next_step(RetryStrategy) ->
    genlib_retry:next_step(RetryStrategy).

-spec get_impact_retry_strategy(processor_impact(), mg_utils:deadline(), state()) ->
    processor_retry().
get_impact_retry_strategy(continuation, Deadline, #{options := Options}) ->
    retry_strategy(continuation, Options, Deadline);
get_impact_retry_strategy(_Impact, _Deadline, _State) ->
    undefined.

-spec try_init_state(processor_impact(), state()) ->
    state().
try_init_state({init, _}, State) ->
    State#{storage_machine := new_storage_machine()};
try_init_state(_Impact, State) ->
    State.

-spec handle_exception(Exception, Impact, ReqCtx, Deadline, state()) -> state() when
    Exception :: mg_utils:exception(),
    Impact :: processor_impact() | undefined,
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline().
handle_exception(Exception, Impact, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, namespace := NS, storage_machine := StorageMachine} = State,
    ok = emit_beat(Options, #mg_machine_lifecycle_failed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        exception = Exception
    }),
    case {Impact, StorageMachine} of
        {_, undefined} ->
            State;
        {_, #{status := {error, _, _}}} ->
            State;
        {_, #{status := OldStatus}} ->
            NewStorageMachine = StorageMachine#{status => {error, Exception, OldStatus}},
            transit_state(ReqCtx, Deadline, NewStorageMachine, State)
    end.

-spec process_unsafe(processor_impact(), processing_context(), request_context(), mg_utils:deadline(), state()) ->
    state().
process_unsafe(Impact, ProcessingCtx, ReqCtx, Deadline, State = #{storage_machine := StorageMachine}) ->
    ok = emit_pre_process_beats(Impact, ReqCtx, Deadline, State),
    ProcessStart = erlang:monotonic_time(),
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State),
    ProcessDuration = erlang:monotonic_time() - ProcessStart,
    ok = emit_post_process_beats(Impact, ReqCtx, Deadline, ProcessDuration, State),
    ok = try_suicide(State, ReqCtx),
    NewStorageMachine0 = StorageMachine#{state := NewMachineState},
    NewState =
        case Action of
            {continue, _} ->
                NewStorageMachine = NewStorageMachine0#{status := {processing, ReqCtx}},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            sleep ->
                NewStorageMachine = NewStorageMachine0#{status := sleeping},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            {wait, Timestamp, HdlReqCtx, HdlTo} ->
                Status = {waiting, Timestamp, HdlReqCtx, HdlTo},
                ok = maybe_force_run_task(Timestamp, State, Status),
                NewStorageMachine = NewStorageMachine0#{status := Status},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            remove ->
                remove_from_storage(ReqCtx, Deadline, State)
        end,
    ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
    case Action of
        {continue, NewProcessingSubState} ->
            % продолжение обработки машины делается без дедлайна
            % предполагается, что машина должна рано или поздно завершить свои дела или упасть
            process(continuation, ProcessingCtx#{state:=NewProcessingSubState}, ReqCtx, undefined, NewState);
        _ ->
            NewState
    end.

-spec(maybe_force_run_task(genlib_time:ts(), state(), machine_regular_status()) -> ok).
maybe_force_run_task(Timestamp, State, Status) ->
    CurrentTimeSec = genlib_time:unow(),
    case Timestamp =< CurrentTimeSec of
        true ->
            Id = maps:get(id, State),
            Ns = maps:get(namespace, State),
            TaskInfo = mg_queue_timer:build_task_info(Id, Timestamp, Status),
            mg_scheduler:add_task(Ns, ?TIMERS, TaskInfo);
        false ->
            ok
    end.

-spec call_processor(processor_impact(), processing_context(), request_context(), mg_utils:deadline(), state()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = State,
    mg_utils:apply_mod_opts(
        get_options(processor, Options),
        process_machine,
        [ID, Impact, ProcessingCtx, ReqCtx, Deadline, MachineState]
    ).

-spec processor_child_spec(options()) ->
    supervisor:child_spec().
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(get_options(processor, Options), processor_child_spec, undefined).

-spec reschedule(ProcessingCtx, ReqCtx, Deadline, state()) -> state() when
    ProcessingCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline().
reschedule(ProcessingCtx, ReqCtx, Deadline, State) ->
    #{id:= ID, options := Options, namespace := NS} = State,
    try
        {ok, NewState, Target, Attempt} = reschedule_unsafe(ReqCtx, Deadline, State),
        ok = emit_beat(Options, #mg_timer_lifecycle_rescheduled{
            namespace = NS,
            machine_id = ID,
            request_context = ReqCtx,
            deadline = Deadline,
            target_timestamp = Target,
            attempt = Attempt
        }),
        ok = do_reply_action({reply, ok}, ProcessingCtx),
        NewState
    catch
        throw:(Reason=({ErrorType, _Details})):ST when ?can_be_retried(ErrorType) ->
            Exception = {throw, Reason, ST},
            ok = emit_beat(Options, #mg_timer_lifecycle_rescheduling_error{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                deadline = Deadline,
                exception = Exception
            }),
            ok = do_reply_action({reply, {error, Reason}}, ProcessingCtx),
            State;
        Class:Reason:ST ->
            Exception = {Class, Reason, ST},
            ok = do_reply_action({reply, {error, {logic, machine_failed}}}, ProcessingCtx),
            handle_exception(Exception, undefined, ReqCtx, Deadline, State)
    end.

-spec reschedule_unsafe(ReqCtx, Deadline, state()) -> Result when
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline(),
    Result :: {ok, state(), genlib_time:ts(), non_neg_integer()}.
reschedule_unsafe(ReqCtx, Deadline, State) ->
    #{storage_machine := #{status := MachineStatus}} = State,
    reschedule_unsafe(MachineStatus, ReqCtx, Deadline, State).

-spec reschedule_unsafe(MachineStatus, ReqCtx, Deadline, state()) -> Result when
    ReqCtx :: request_context(),
    Deadline :: mg_utils:deadline(),
    MachineStatus :: machine_regular_status(),
    Result :: {ok, state(), genlib_time:ts(), non_neg_integer()}.
reschedule_unsafe({waiting, _, _, _}, ReqCtx, Deadline, State) ->
    #{storage_machine := StorageMachine, options := Options} = State,
    RetryStrategy = retry_strategy(timers, Options, undefined),
    case genlib_retry:next_step(RetryStrategy) of
        {wait, Timeout, _NewRetryStrategy} ->
            Now = genlib_time:unow(),
            Target = get_schedule_target(Timeout),
            NewStatus = {retrying, Target, Now, NewAttempt = 0, ReqCtx},
            NewStorageMachine = StorageMachine#{status => NewStatus},
            {ok, transit_state(ReqCtx, Deadline, NewStorageMachine, State), Target, NewAttempt};
        finish ->
            throw({permanent, timer_retries_exhausted})
    end;
reschedule_unsafe({retrying, _, Start, Attempt, _}, ReqCtx, Deadline, State) ->
    #{storage_machine := StorageMachine, options := Options} = State,
    RetryStrategy = retry_strategy(timers, Options, undefined, Start, Attempt),
    case genlib_retry:next_step(RetryStrategy) of
        {wait, Timeout, _NewRetryStrategy} ->
            Target = get_schedule_target(Timeout),
            NewStatus = {retrying, Target, Start, NewAttempt = Attempt + 1, ReqCtx},
            NewStorageMachine = StorageMachine#{status => NewStatus},
            {ok, transit_state(ReqCtx, Deadline, NewStorageMachine, State), Target, NewAttempt};
        finish ->
            throw({permanent, timer_retries_exhausted})
    end.

-spec get_schedule_target(timeout()) ->
    genlib_time:ts().
get_schedule_target(TimeoutMS) ->
    Now = genlib_time:unow(),
    Now + (TimeoutMS div 1000).

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


-spec transit_state(request_context(), mg_utils:deadline(), storage_machine(), state()) ->
    state().
transit_state(_ReqCtx, _Deadline, NewStorageMachine, State=#{storage_machine := OldStorageMachine})
    when NewStorageMachine =:= OldStorageMachine
->
    State;
transit_state(ReqCtx, Deadline, NewStorageMachine, State) ->
    #{id:=ID, options:=Options, storage_context := StorageContext} = State,
    F = fun() ->
            mg_storage:put(
                storage_options(Options),
                storage_ref(Options),
                ID,
                StorageContext,
                storage_machine_to_opaque(NewStorageMachine),
                storage_machine_to_indexes(NewStorageMachine)
            )
        end,
    RS = retry_strategy(storage, Options, Deadline),
    NewStorageContext = do_with_retry(Options, ID, F, RS, ReqCtx, transit),
    State#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

-spec remove_from_storage(request_context(), mg_utils:deadline(), state()) ->
    state().
remove_from_storage(ReqCtx, Deadline, State) ->
    #{namespace := NS, id := ID, options := Options, storage_context := StorageContext} = State,
    F = fun() ->
            mg_storage:delete(
                storage_options(Options),
                storage_ref(Options),
                ID,
                StorageContext
            )
        end,
    RS = retry_strategy(storage, Options, Deadline),
    ok = do_with_retry(Options, ID, F, RS, ReqCtx, remove),
    ok = emit_beat(Options, #mg_machine_lifecycle_removed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx
    }),
    State#{storage_machine := undefined, storage_context := undefined}.

-spec retry_strategy(retry_subj(), options(), mg_utils:deadline()) ->
    mg_retry:strategy().
retry_strategy(Subj, Options, Deadline) ->
    retry_strategy(Subj, Options, Deadline, undefined, undefined).

-spec retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) -> mg_retry:strategy() when
    Subj :: retry_subj(),
    Options :: options(),
    Deadline :: mg_utils:deadline(),
    InitialTs :: genlib_time:ts() | undefined,
    Attempt :: non_neg_integer() | undefined.
retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) ->
    Retries = maps:get(retries, Options, #{}),
    Policy = maps:get(Subj, Retries, ?DEFAULT_RETRY_POLICY),
    constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt).

-spec constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt) -> mg_retry:strategy() when
    Policy :: mg_retry:policy(),
    Deadline :: mg_utils:deadline(),
    InitialTs :: genlib_time:ts() | undefined,
    Attempt :: non_neg_integer() | undefined.
constrain_retry_strategy(Policy, undefined, InitialTs, Attempt) ->
    mg_retry:new_strategy(Policy, InitialTs, Attempt);
constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt) ->
    Timeout = mg_utils:deadline_to_timeout(Deadline),
    mg_retry:new_strategy({timecap, Timeout, Policy}, InitialTs, Attempt).

-spec emit_pre_process_beats(processor_impact(), request_context(), mg_utils:deadline(), state()) ->
    ok.
emit_pre_process_beats(Impact, ReqCtx, Deadline, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_machine_process_started{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }),
    emit_pre_process_timer_beats(Impact, ReqCtx, Deadline, State).

-spec emit_pre_process_timer_beats(processor_impact(), request_context(), mg_utils:deadline(), state()) ->
    ok.
emit_pre_process_timer_beats(timeout, ReqCtx, Deadline, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_timer_process_started{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline
    });
emit_pre_process_timer_beats(_Impact, _ReqCtx, _Deadline, _State) ->
    ok.

-spec emit_post_process_beats(processor_impact(), request_context(), mg_utils:deadline(), integer(), state()) ->
    ok.
emit_post_process_beats(Impact, ReqCtx, Deadline, Duration, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_machine_process_finished{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        duration = Duration
    }),
    emit_post_process_timer_beats(Impact, ReqCtx, Deadline, Duration, State).

-spec emit_post_process_timer_beats(processor_impact(), request_context(), mg_utils:deadline(), integer(), state()) ->
    ok.
emit_post_process_timer_beats(timeout, ReqCtx, Deadline, Duration, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_timer_process_finished{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline,
        duration = Duration
    });
emit_post_process_timer_beats(_Impact, _ReqCtx, _Deadline, _Duration, _State) ->
    ok.

-spec extract_timer_queue_info(machine_status()) ->
    {ok, normal | retries, genlib_time:ts()} | {error, not_timer}.
extract_timer_queue_info({waiting, Timestamp, _, _}) ->
    {ok, normal, Timestamp};
extract_timer_queue_info({retrying, Timestamp, _, _, _}) ->
    {ok, retries, Timestamp};
extract_timer_queue_info(_Other) ->
    {error, not_timer}.

-spec emit_machine_load_beat(options(), mg:ns(), mg:id(), request_context(), storage_machine() | undefined) ->
    ok.
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, undefined) ->
    ok = emit_beat(Options, #mg_machine_lifecycle_created{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    });
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, _StorageMachine) ->
    ok = emit_beat(Options, #mg_machine_lifecycle_loaded{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    }).

%%

-spec manager_options(options()) ->
    mg_workers_manager:options().
manager_options(Options) ->
    #{
        name           => maps:get(namespace, Options),
        pulse          => maps:get(pulse, Options),
        worker_options => #{
            worker => {?MODULE, Options}
        }
    }.

-spec storage_options(options()) ->
    mg_storage:options().
storage_options(#{storage := Storage}) ->
    Storage.

-spec storage_ref(options()) ->
    mg_utils:gen_ref().
storage_ref(Options) ->
    {via, gproc, gproc_key(storage, Options)}.

-spec storage_reg_name(options()) ->
    mg_utils:gen_reg_name().
storage_reg_name(Options) ->
    {via, gproc, gproc_key(storage, Options)}.

-spec gproc_key(atom(), options()) ->
    gproc:key().
gproc_key(Type, #{namespace := Namespace}) ->
    {n, l, {?MODULE, Type, Namespace}}.

-spec scheduler_child_spec(overseer | timers | timers_retries, options()) ->
    supervisor:child_spec() | undefined.
scheduler_child_spec(SchedulerID, Options) ->
    case maps:get(SchedulerID, maps:get(schedulers, Options, #{}), disable) of
        disable ->
            undefined;
        Config ->
            mg_scheduler:child_spec(scheduler_options(SchedulerID, Options, Config), SchedulerID)
    end.

-spec scheduler_options(overseer | timers | timers_retries, options(), scheduler_opt()) ->
    mg_scheduler:options().
scheduler_options(timers, Options, Config) ->
    HandlerOptions = #{
        processing_timeout => maps:get(timer_processing_timeout, Options, undefined),
        reschedule_timeout => maps:get(reschedule_timeout, Options, undefined),
        timer_queue => waiting
    },
    scheduler_options(?TIMERS, mg_queue_timer, Options, HandlerOptions, Config);
scheduler_options(timers_retries, Options, Config) ->
    HandlerOptions = #{
        processing_timeout => maps:get(timer_processing_timeout, Options, undefined),
        reschedule_timeout => maps:get(reschedule_timeout, Options, undefined),
        timer_queue => retrying
    },
    scheduler_options(<<"timers_retries">>, mg_queue_timer, Options, HandlerOptions, Config);
scheduler_options(overseer, Options, Config) ->
    scheduler_options(<<"overseer">>, mg_queue_interrupted, Options, #{}, Config).

-spec scheduler_options(mg_scheduler:name(), module(), options(), map(), scheduler_opt()) ->
    mg_scheduler:options().
scheduler_options(Name, HandlerMod, Options, HandlerOptions, Config) ->
    #{
        namespace := NS,
        pulse := Pulse
    } = Options,
    FullHandlerOptions = genlib_map:compact(maps:merge(
        #{
            namespace => NS,
            scheduler_name => Name,
            pulse => Pulse,
            machine => Options
        },
        HandlerOptions
    )),
    Handler = {HandlerMod, FullHandlerOptions},
    SearchInterval = maps:get(interval, Config, undefined),
    NoTaskWait = maps:get(no_task_wait, Config, SearchInterval),
    genlib_map:compact(#{
        namespace => NS,
        name => Name,
        queue_handler => Handler,
        task_handler => Handler,
        pulse => Pulse,
        quota_name => maps:get(limit, Config, unlimited),
        quota_share => maps:get(share, Config, 1),
        no_task_wait => NoTaskWait,
        search_interval => SearchInterval
    }).

-spec get_options(atom(), options()) ->
    _.
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec try_suicide(state(), request_context()) ->
    ok | no_return().
try_suicide(#{options := Options = #{suicide_probability := Prob}, id := ID, namespace := NS}, ReqCtx) ->
    case (Prob =/= undefined) andalso (rand:uniform() < Prob) of
        true ->
            ok = emit_beat(Options, #mg_machine_lifecycle_committed_suicide{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                suicide_probability = Prob
            }),
            erlang:exit(self(), kill);
        false ->
            ok
    end;
try_suicide(#{}, _) ->
    ok.

%%
%% retrying
%%
-spec do_with_retry(options(), mg:id(), fun(() -> R), mg_retry:strategy(), request_context(), atom()) ->
    R.
do_with_retry(Options = #{namespace := NS}, ID, Fun, RetryStrategy, ReqCtx, BeatCtx) ->
    try
        Fun()
    catch throw:(Reason={transient, _}):ST ->
        NextStep = genlib_retry:next_step(RetryStrategy),
        ok = emit_beat(Options, #mg_machine_lifecycle_transient_error{
            context = BeatCtx,
            namespace = NS,
            machine_id = ID,
            exception = {throw, Reason, ST},
            request_context = ReqCtx,
            retry_strategy = RetryStrategy,
            retry_action = NextStep
        }),
        case NextStep of
            {wait, Timeout, NewRetryStrategy} ->
                ok = timer:sleep(Timeout),
                do_with_retry(Options, ID, Fun, NewRetryStrategy, ReqCtx, BeatCtx);
            finish ->
                throw({transient, {retries_exhausted, Reason}})
        end
    end.

%%
%% logging
%%

-spec emit_beat(options(), mg_pulse:beat()) -> ok.
emit_beat(#{pulse := Handler}, Beat) ->
    ok = mg_pulse:handle_beat(Handler, Beat).
