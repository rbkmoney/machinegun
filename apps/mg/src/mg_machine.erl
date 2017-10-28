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
-export_type([retry_opt             /0]).
-export_type([scheduled_tasks_opt   /0]).
-export_type([suicide_probability   /0]).
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

-export([child_spec /2]).
-export([start_link /1]).

-export([start               /5]).
-export([simple_repair       /4]).
-export([repair              /5]).
-export([call                /5]).
-export([fail                /4]).
-export([fail                /5]).
-export([get                 /2]).
-export([get_status          /2]).
-export([is_exist            /2]).
-export([search              /2]).
-export([call_with_lazy_start/6]).

%% Internal API
-export([handle_timers         /2]).
-export([resume_interrupted    /2]).
-export([resume_interrupted_one/2]).
-export([all_statuses          /0]).
-export([workers_options       /1]).
-export([get_state             /2]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/3, handle_unload/3, handle_call/5]).

%%
%% API
%%
-type scheduled_task_opt() :: disable | #{interval => pos_integer(), limit => mg_storage:index_limit()}.
-type retry_opt() :: #{
    storage   => mg_utils:genlib_retry_policy(),
    processor => mg_utils:genlib_retry_policy()
}.
-type scheduled_tasks_opt() :: #{
    timers   => scheduled_task_opt(),
    overseer => scheduled_task_opt()
}.
-type suicide_probability() :: float() | integer() | undefined. % [0, 1]

-type options() :: #{
    namespace           => mg:ns(),
    storage             => mg_storage:options(),
    processor           => mg_utils:mod_opts(),
    logger              => mg_machine_logger:handler(),
    retries             => retry_opt(),
    scheduled_tasks     => scheduled_tasks_opt(),
    suicide_probability => suicide_probability(),
    raft                => raft:options()
}.

-type thrown_error() :: logic_error() | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason} | unavailable.

-type throws() :: no_return().
-type machine_state() :: mg_storage:opaque().

-type machine_regular_status() ::
       sleeping
    | {waiting, genlib_time:ts(), request_context(), HandlingTimeout::pos_integer()}
    | {processing, request_context()}
.
-type machine_status() :: machine_regular_status() | {error, Reason::term(), machine_regular_status()}.

%%

-type processing_state() :: term().
% контест обработки, сбрасывается при выгрузке машины
-type processing_context() :: #{
    % call_context => mg_worker:call_context(),
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

-callback processor_child_spec(_Options) ->
    supervisor:child_spec() | undefined.
-callback process_machine(_Options, mg:id(), processor_impact(), processing_context(), ReqCtx, machine_state()) ->
    processor_result()
when ReqCtx :: request_context().
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
        mg_utils:lists_compact([
            mg_workers:child_spec(workers_options(Options), manager),
            mg_storage        :child_spec(storage_options(Options), storage, storage_reg_name(Options)),
            scheduler_child_spec(timers  , Options),
            scheduler_child_spec(overseer, Options),
            processor_child_spec(Options)
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
    {_, #{state := State}} = mg_utils:throw_if_undefined(get_state(Options, ID), machine_not_found),
    State.

-spec get_status(options(), mg:id()) ->
    machine_status() | throws().
get_status(Options, ID) ->
    {_, #{status := Status}} = mg_utils:throw_if_undefined(get_state(Options, ID), machine_not_found),
    Status.

-spec is_exist(options(), mg:id()) ->
    boolean() | throws().
is_exist(Options, ID) ->
    get_state(Options, ID) =/= undefined.

-spec search(options(), search_query(), mg_storage:index_limit()) ->
    {[{_TODO, mg:id()}] | [mg:id()], mg_storage:continuation()} | throws().
search(Options, Query, Limit) ->
    mg_storage:search(storage_options(Options), storage_ref(Options), storage_search_query(Query, Limit)).

-spec search(options(), search_query()) ->
    [{_TODO, mg:id()}] | [mg:id()] | throws().
search(Options, Query) ->
    % TODO deadline
    mg_storage:search(storage_options(Options), storage_ref(Options), storage_search_query(Query)).

-spec call_with_lazy_start(options(), mg:id(), term(), request_context(), mg_utils:deadline(), term()) ->
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

%%
%% Internal API
%%
-define(DEFAULT_SCHEDULED_TASK, disable). % {1000, 10}
-define(DEFAULT_RETRY_POLICY, {exponential, infinity, 2, 10, 60 * 1000}).

-define(safe_request(Options, ID, ReqCtx, EventTag, Expr),
    try
        Expr
    catch Class:Reason ->
        Event = {EventTag, {Class, Reason, erlang:get_stacktrace()}},
        ok = emit_log_request_event(Options, ID, ReqCtx, Event)
    end
).

-spec handle_timers(options(), mg_storage:index_limit()) ->
    ok.
handle_timers(Options, Limit) ->
    ?safe_request(
        Options, undefined, null, timer_handling_failed,
        begin
            {Timers, _} = search(Options, {waiting, 1, genlib_time:now()}, Limit),
            lists:foreach(
                % такая схема потенциально опасная, но надо попробовать как она себя будет вести
                fun({_, ID}) ->
                    erlang:spawn_link(fun() -> handle_timer(Options, ID) end)
                end,
                Timers
            )
        end
    ).

-spec handle_timer(options(), mg:id()) ->
    ok.
handle_timer(Options, ID) ->
    ?safe_request(
        Options, ID, null, timer_handling_failed,
        begin
            {_, StorageMachine} = get_state(Options, ID),
            case StorageMachine of
                #{status := {waiting, Timestamp, ReqCtx, Timeout}} ->
                    handle_timer(Options, ID, Timestamp, ReqCtx, mg_utils:timeout_to_deadline(Timeout));
                #{status := _} ->
                    ok
            end
        end
    ).

-spec handle_timer(options(), mg:id(), genlib_time:ts(), request_context(), mg_utils:deadline()) ->
    ok.
handle_timer(Options, ID, Timestamp, ReqCtx, Deadline) ->
    ?safe_request(
        Options, ID, ReqCtx, timer_handling_failed,
        begin
            call_(Options, ID, {timeout, Timestamp}, ReqCtx, Deadline)
        end
    ).

-spec resume_interrupted(options(), mg_storage:index_limit()) ->
    ok.
resume_interrupted(Options, Limit) ->
    ?safe_request(
        Options, undefined, null, resuming_interrupted_failed,
        begin
            {Interrupted, _} = search(Options, processing, Limit),
            lists:foreach(
                fun(ID) ->
                    erlang:spawn_link(fun() -> resume_interrupted_one(Options, ID) end)
                end,
                Interrupted
            )
        end
    ).

-spec resume_interrupted_one(options(), mg:id()) ->
    ok.
resume_interrupted_one(Options, ID) ->
    ?safe_request(
        Options, ID, null, resuming_interrupted_failed,
        begin
            case mg_workers:is_loaded(workers_options(Options), ID) of
                false ->
                    Deadline = mg_utils:timeout_to_deadline(30000),
                    {_, StorageMachine} = get_state(Options, ID),
                    case StorageMachine of
                        #{status := {processing, ReqCtx}} ->
                            resume_interrupted_one(Options, ID, ReqCtx, Deadline);
                        #{status := _} ->
                            ok
                    end;
                true ->
                    ok
            end
        end
    ).

-spec resume_interrupted_one(options(), mg:id(), request_context(), mg_utils:deadline()) ->
    ok.
resume_interrupted_one(Options, ID, ReqCtx, Deadline) ->
    ?safe_request(
        Options, ID, ReqCtx, resuming_interrupted_failed,
        begin
            call_(Options, ID, resume_interrupted_one, ReqCtx, Deadline)
        end
    ).

%%

-spec all_statuses() ->
    [atom()].
all_statuses() ->
    [sleeping, waiting, processing, failed].

-spec call_(options(), mg:id(), _, request_context(), mg_utils:deadline()) ->
    _ | no_return().
call_(Options, ID, Call, ReqCtx, Deadline) ->
    mg_utils:throw_if_error(mg_workers:call(workers_options(Options), ID, Call, ReqCtx, Deadline)).

%%
%% mg_worker callbacks
%%
% handling state
-type hstate() :: #{
    id              => mg:id(),
    options         => options(),
    storage_machine => storage_machine() | undefined,
    storage_context => mg_storage:context() | undefined
}.
-type state() :: {mg_storage:context(), storage_machine()} | undefined.

-type storage_machine() :: #{
    status => machine_status(),
    state  => machine_state()
}.

-spec handle_load(options(), _ID, request_context()) ->
    {ok, mg_storage:opaque()}.
handle_load(Options, ID, ReqCtx) ->
    try
        {ok, state_to_opaque(get_state(Options, ID))}
    catch throw:Reason ->
        Exception = {throw, Reason, erlang:get_stacktrace()},
        ok = emit_log_machine_event(Options, ID, ReqCtx, {loading_failed, Exception}),
        {error, Reason}
    end.

-spec handle_call(options(), _ID, _Call, request_context(), mg_storage:opaque()) ->
    {{reply, _Resp} | noreply, mg_storage:opaque()}.
handle_call(Options, ID, Call, ReqCtx, OpaqueState) ->
    PCtx = new_processing_context(),
    HS = #{storage_machine := StorageMachine} = new_hstate(Options, ID, opaque_to_state(OpaqueState)),

    % довольно сложное место, тут определяется приоритет реакции на внешние раздражители, нужно быть очень аккурантым
    {Reply, NewHState} =
        case {Call, StorageMachine} of
            % start
            {{start, Args}, undefined   } -> process_start(Args, PCtx, ReqCtx, HS);
            { _           , undefined   } -> {{reply, {error, machine_not_found    }}, HS};
            {{start, _   }, #{status:=_}} -> {{reply, {error, machine_already_exist}}, HS};

            % fail
            {{fail, Exception}, _} -> {{reply, ok}, handle_exception(Exception, undefined, ReqCtx, HS)};

            % сюда мы не должны попадать если машина не падала во время обработки запроса
            % (когда мы переходили в стейт processing)
            % TODO починить
            % {_, #{status := {processing, ProcessingReqCtx}}} ->
            %     handle_call(Call, CallContext, ReqCtx, process(continuation, undefined, ProcessingReqCtx, HS));

            % ничего не просходит, просто убеждаемся, что машина загружена
            {resume_interrupted_one, _} -> {{reply, {ok, ok}}, HS};

            % call
            {{call  , SubCall}, #{status:= sleeping         }} -> process({call, SubCall}, PCtx, ReqCtx, HS);
            {{call  , SubCall}, #{status:={waiting, _, _, _}}} -> process({call, SubCall}, PCtx, ReqCtx, HS);
            {{call  , _      }, #{status:={error  , _, _   }}} -> {{reply, {error, machine_failed}}, HS};

            % repair
            {{repair, Args}, #{status:={error  , _, _}}} -> process({repair, Args}, PCtx, ReqCtx, HS);
            {{repair, _   }, #{status:=_              }} -> {{reply, {error, machine_already_working}}, HS};

            % simple_repair
            {simple_repair, #{status:={error  , _, _}}} -> {{reply, ok}, process_simple_repair(ReqCtx, HS)};
            {simple_repair, #{status:=_              }} -> {{reply, {error, machine_already_working}}, HS};

            % timers
            {{timeout, Ts0}, #{status:={waiting, Ts1, _, _}}}
                when Ts0 =:= Ts1 ->
                process(timeout, PCtx, ReqCtx, HS);
            {{timeout, _}, #{status:=_}} ->
                {{reply, {ok, ok}}, HS}
        end,
    {Reply, state_to_opaque(hstate_to_state(NewHState))}.

-spec handle_unload(_, mg:id(), mg_storage:opaque()) ->
    ok.
handle_unload(_, _, _) ->
    ok.

%%

-spec hstate_to_state(hstate()) ->
    state().
hstate_to_state(#{storage_machine := undefined, storage_context := undefined}) ->
    undefined;
hstate_to_state(#{storage_machine := StorageMachine, storage_context := StorageContext}) ->
    {StorageContext, StorageMachine}.

-spec new_hstate(options(), mg:id(), state()) ->
    hstate().
new_hstate(Options, ID, State) ->
    {StorageContext, StorageMachine} =
        case State of
            undefined -> {undefined, undefined};
            {_, _}    -> State
        end,
    #{
        id              => ID,
        options         => Options,
        storage_machine => StorageMachine,
        storage_context => StorageContext
    }.

-spec new_processing_context() ->
    processing_context().
new_processing_context() ->
    #{
        state => undefined
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

-spec get_state(options(), mg:id()) ->
    state().
get_state(Options, ID) ->
    case mg_storage:get(storage_options(Options), storage_ref(Options), ID) of
        undefined ->
            undefined;
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(PackedMachine)}
    end.

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(undefined) ->
    [1, null];
state_to_opaque({StorageContext, StorageMachine}) ->
    [1, storage_context_to_opaque(StorageContext), storage_machine_to_opaque(StorageMachine)].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
opaque_to_state([1, null]) ->
    undefined;
opaque_to_state([1, StorageContext, StorageMachine]) ->
    {opaque_to_storage_context(StorageContext), opaque_to_storage_machine(StorageMachine)}.

-spec storage_context_to_opaque(mg_storage:context()) ->
    mg_storage:opaque().
storage_context_to_opaque(StorageContext) ->
    StorageContext.

-spec opaque_to_storage_context(mg_storage:opaque()) ->
    mg_storage:context().
opaque_to_storage_context(StorageContext) ->
    StorageContext.

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
-spec process_start(term(), processing_context(), request_context(), hstate()) ->
    {processor_reply_action(), hstate()}.
process_start(Args, ProcessingCtx, ReqCtx, HState) ->
    process({init, Args}, ProcessingCtx, ReqCtx, HState#{storage_machine := new_storage_machine()}).

-spec process_simple_repair(request_context(), hstate()) ->
    hstate().
process_simple_repair(ReqCtx, HState = #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}}) ->
    transit_state(
        ReqCtx,
        StorageMachine#{status => OldStatus},
        HState
    ).

-spec process(processor_impact(), processing_context(), request_context(), hstate()) ->
    {processor_reply_action(), hstate()}.
process(Impact, ProcessingCtx, ReqCtx, HState) ->
    try
        process_unsafe(Impact, ProcessingCtx, ReqCtx, HState)
    catch Class:Reason ->
        {
            {reply, {error, machine_failed}},
            handle_exception({Class, Reason, erlang:get_stacktrace()}, Impact, ReqCtx, HState)
        }
    end.

-spec handle_exception(mg_utils:exception(), processor_impact() | undefined, request_context(), hstate()) ->
    hstate().
handle_exception(Exception, Impact, ReqCtx, HState) ->
    #{options := Options, id := ID, storage_machine := StorageMachine} = HState,
    ok = emit_log_machine_event(Options, ID, ReqCtx, {machine_failed, Exception}),
    #{status := OldStatus} = StorageMachine,
    case {Impact, OldStatus} of
        {{init, _}, _} ->
            HState#{storage_machine := undefined};
        {_, {error, _, _}} ->
            HState;
        {_, _} ->
            NewStorageMachine = StorageMachine#{status => {error, Exception, OldStatus}},
            transit_state(ReqCtx, NewStorageMachine, HState)
    end.

-spec process_unsafe(processor_impact(), processing_context(), request_context(), hstate()) ->
    {processor_reply_action(), hstate()}.
process_unsafe(Impact, ProcessingCtx, ReqCtx, HState = #{storage_machine := StorageMachine}) ->
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, ReqCtx, HState),
    ok = try_suicide(HState, ReqCtx),
    NewStorageMachine = StorageMachine#{state := NewMachineState},
    NewHState =
        case Action of
            {continue, _} ->
                transit_state(ReqCtx, NewStorageMachine#{status := {processing, ReqCtx}}, HState);
            sleep ->
                transit_state(ReqCtx, NewStorageMachine#{status := sleeping}, HState);
            {wait, Timestamp, HdlReqCtx, HdlTo} ->
                transit_state(ReqCtx, NewStorageMachine#{status := {waiting, Timestamp, HdlReqCtx, HdlTo}}, HState);
            remove ->
                remove_from_storage(ReqCtx, HState)
        end,
    case Action of
        {continue, NewProcessingSubState} ->
            process(continuation, ProcessingCtx#{state := NewProcessingSubState}, ReqCtx, NewHState);
        _ ->
            {wrap_reply_action(ok, ReplyAction), NewHState}
    end.

-spec call_processor(processor_impact(), processing_context(), request_context(), hstate()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, ReqCtx, HState) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = HState,
    F = fun() ->
            processor_process_machine(ID, Impact, ProcessingCtx, ReqCtx, MachineState, Options)
        end,
    do_with_retry(Options, ID, F, retry_strategy(processor, Options), ReqCtx).

-spec processor_process_machine(mg:id(), processor_impact(), processing_context(), ReqCtx, hstate(), options()) ->
    _Result
when ReqCtx :: request_context().
processor_process_machine(ID, Impact, ProcessingCtx, ReqCtx, MachineState, Options) ->
    mg_utils:apply_mod_opts(
        get_options(processor, Options),
        process_machine,
        [ID, Impact, ProcessingCtx, ReqCtx, MachineState]
    ).

-spec processor_child_spec(options()) ->
    supervisor:child_spec().
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(get_options(processor, Options), processor_child_spec, undefined).

-spec wrap_reply_action(_, processor_reply_action()) ->
    processor_reply_action().
wrap_reply_action(_, noreply) ->
    noreply;
wrap_reply_action(Wrapper, {reply, R}) ->
    {reply, {Wrapper, R}}.

-spec transit_state(request_context(), storage_machine(), hstate()) ->
    hstate().
transit_state(_ReqCtx, NewStorageMachine, HState = #{storage_machine := OldStorageMachine})
    when NewStorageMachine =:= OldStorageMachine
->
    HState;
transit_state(ReqCtx, NewStorageMachine, HState=#{id:=ID, options:=Options, storage_context := StorageContext}) ->
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
    NewStorageContext  = do_with_retry(Options, ID, F, retry_strategy(storage, Options), ReqCtx),
    HState#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

-spec remove_from_storage(request_context(), hstate()) ->
    hstate().
remove_from_storage(ReqCtx, HState = #{id := ID, options := Options, storage_context := StorageContext}) ->
    F = fun() ->
            mg_storage:delete(
                storage_options(Options),
                storage_ref(Options),
                ID,
                StorageContext
            )
        end,
    ok = do_with_retry(Options, ID, F, retry_strategy(storage, Options), ReqCtx),
    HState#{storage_machine := undefined, storage_context := undefined}.

-spec retry_strategy(storage | processor, options()) ->
    genlib_retry:strategy().
retry_strategy(Subj, Options) ->
    mg_utils:genlib_retry_new(maps:get(Subj, maps:get(retries, Options, #{}), ?DEFAULT_RETRY_POLICY)).

%%

-spec workers_options(options()) ->
    mg_workers:options().
workers_options(Options = #{namespace := NS, raft := Raft}) ->
    #{
        namespace      => NS,
        worker_options => #{worker => {?MODULE, Options}},
        raft           => Raft
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

-spec scheduler_child_spec(overseer | timers, options()) ->
    supervisor:child_spec() | undefined.
scheduler_child_spec(Task, Options) ->
    case maps:get(Task, maps:get(scheduled_tasks, Options, #{}), ?DEFAULT_SCHEDULED_TASK) of
        disable ->
            undefined;
        #{interval := Interval, limit := Limit} ->
            F = case Task of
                    timers   -> handle_timers;
                    overseer -> resume_interrupted
                end,
            mg_cron:child_spec(#{interval => Interval, job => {?MODULE, F, [Options, Limit]}}, Task)
    end.

-spec get_options(atom(), options()) ->
    _.
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec try_suicide(hstate(), request_context()) ->
    ok | no_return().
try_suicide(#{options := Options = #{suicide_probability := Prob}, id := ID}, ReqCtx) ->
    case (Prob =/= undefined) andalso (rand:uniform() < Prob) of
        true ->
            ok = emit_log_machine_event(Options, ID, ReqCtx, committed_suicide),
            erlang:exit(self(), kill);
        false ->
            ok
    end;
try_suicide(#{}, _) ->
    ok.

%%
%% retrying
%%
-spec do_with_retry(options(), mg:id(), fun(() -> R), genlib_retry:strategy(), request_context()) ->
    R.
do_with_retry(Options, ID, Fun, RetryStrategy, ReqCtx) ->
    try
        Fun()
    catch throw:(Reason={transient, _}) ->
        ok = emit_log_machine_event(Options, ID, ReqCtx, {transient_error, {throw, Reason, erlang:get_stacktrace()}}),
        case genlib_retry:next_step(RetryStrategy) of
            {wait, Timeout, NewRetryStrategy} ->
                ok = emit_log_machine_event(Options, ID, ReqCtx, {retrying, Timeout}),
                ok = timer:sleep(Timeout),
                do_with_retry(Options, ID, Fun, NewRetryStrategy, ReqCtx);
            finish ->
                throw({timeout, {retry_timeout, Reason}})
        end
    end.

%%
%% logging
%%
-spec emit_log_request_event(options(), mg:id() | undefined, request_context(), mg_machine_logger:request_event()) ->
    ok.
emit_log_request_event(#{logger := Handler}, ID, ReqCtx, RequestEvent) ->
    ok = mg_machine_logger:handle_event(Handler, {request_event, ID, ReqCtx, RequestEvent}).

-spec emit_log_machine_event(options(), mg:id(), request_context(), mg_machine_logger:machine_event()) ->
    ok.
emit_log_machine_event(#{logger := Handler}, ID, ReqCtx, MachineEvent) ->
    ok = mg_machine_logger:handle_event(Handler, {machine_event, ID, ReqCtx, MachineEvent}).
