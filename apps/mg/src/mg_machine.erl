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
-export_type([options           /0]).
-export_type([thrown_error      /0]).
-export_type([logic_error       /0]).
-export_type([transient_error   /0]).
-export_type([throws            /0]).
-export_type([machine_state     /0]).
-export_type([processor_impact  /0]).
-export_type([processing_context/0]).
-export_type([processor_result  /0]).

-export([child_spec /2]).
-export([start_link /1]).

-export([start /3]).
-export([repair/3]).
-export([call  /3]).
-export([get   /2]).

-export([call_with_lazy_start/4]).

%% Internal API
-export([reply/2]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/2, handle_call/3, handle_unload/1]).

%%
%% API
%%
-type options() :: #{
    namespace              => mg:ns(),
    storage                => mg_storage:module           (),
    storage_retry_policy   => mg_utils:genlib_retry_policy(), % optional
    processor              => mg_utils:mod_opts           ()
}.

-type thrown_error() :: logic_error() | {transient, transient_error()} | timeout.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | storage_unavailable | processor_unavailable.

-type throws() :: no_return().
-type machine_state() :: mg_storage:opaque().

%%

-type processing_state() :: term().
-type processing_context() :: #{
    call_context => mg_worker:call_context(),
    state        => processing_state()
} | undefined.
-type processor_impact() ::
      {init  , term()}
    | {repair, term()}
    | {call  , term()}
    |  continuation
.
-type processor_reply_action() :: noreply  | {reply, _}.
-type processor_flow_action() :: wait | {continue, processing_state()}. % TODO {wait, DateTime}
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.

-callback process_machine(_Options, mg:id(), processor_impact(), processing_context(), machine_state()) ->
    processor_result().

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
            mg_workers_manager:child_spec(manager, manager_options(Options)),
            mg_storage:child_spec(storage_options(Options), storage)
        ]
    ).

-spec start(options(), mg:id(), term()) ->
    _Resp | throws().
start(Options, ID, Args) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {start, Args})).

-spec repair(options(), mg:id(), term()) ->
    _Resp | throws().
repair(Options, ID, Args) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {repair, Args})).

-spec call(options(), mg:id(), term()) ->
    _Resp | throws().
call(Options, ID, Call) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {call, Call})).

-spec get(options(), mg:id()) ->
    machine_state() | throws().
get(Options, ID) ->
    #{state:=State} = mg_utils:throw_if_undefined(element(2, get_storage_machine(Options, ID)), machine_not_found),
    State.

%% TODO придумуть имена получше, ревьюверы, есть идеи?
-spec call_with_lazy_start(options(), mg:id(), term(), term()) ->
    _Resp | throws().
call_with_lazy_start(Options, ID, Call, StartArgs) ->
    try
        call(Options, ID, Call)
    catch throw:machine_not_found ->
        try
            _ = start(Options, ID, StartArgs)
        catch throw:machine_already_exist ->
            % вдруг кто-то ещё делает аналогичный процесс
            ok
        end,
        % если к этому моменту машина не создалась, значит она уже не создастся
        % и исключение будет оправданным
        call(Options, ID, Call)
    end.

-spec reply(processing_context(), _) ->
    ok.
reply(undefined, _) ->
    % отвечать уже некому
    ok;
reply(#{call_context := CallContext}, Reply) ->
    ok = mg_worker:reply(CallContext, Reply).

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

-type machine_status() ::
      waiting
    | processing
    | {error, Reason::mg_storage:opaque()}
.


-spec handle_load(_ID, options()) ->
    {ok, state()}.
handle_load(ID, Options) ->
    {StorageContext, StorageMachine} = get_storage_machine(Options, ID),
    State =
        #{
            id              => ID,
            options         => Options,
            storage_machine => StorageMachine,
            storage_context => StorageContext
        },
    {ok, try_finish_processing(State)}.

-spec handle_call(_Call, mg_worker:call_context(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, State=#{storage_machine:=StorageMachine}) ->
    PCtx = new_processing_context(CallContext),
    case {Call, StorageMachine} of
        %% success
        {{start , Args   }, undefined              } -> {noreply, process_start(Args, PCtx, State)};
        {{call  , SubCall}, #{status:= waiting    }} -> {noreply, process({call  , SubCall}, PCtx, State)};
        {{repair, Args   }, #{status:={error  , _}}} -> {noreply, process({repair, Args   }, PCtx, State)};

        %% unsuccess
        {{start , _      }, #{status:=           _}} -> {{reply, {error, machine_already_exist  }}, State};
        {{call  , _      }, #{status:={error  , _}}} -> {{reply, {error, machine_failed         }}, State};
        {{call  , _      }, undefined              } -> {{reply, {error, machine_not_found      }}, State};
        {{repair, _      }, #{status:= waiting    }} -> {{reply, {error, machine_already_working}}, State};
        {{repair, _      }, undefined              } -> {{reply, {error, machine_not_found      }}, State}
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
        status => waiting,
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
             waiting       -> 1;
             processing    -> 2;
            {error, Reason} -> [3, erlang:term_to_binary(Reason)] % TODO подумать как упаковывать reason
        end,
    [1, Opaque].

-spec opaque_to_machine_status(mg_storage:opaque()) ->
    machine_status().
opaque_to_machine_status([1, Opaque]) ->
    case Opaque of
         1          -> waiting;
         2          -> processing;
        [3, Reason] -> {error, erlang:binary_to_term(Reason)}
    end.

%%
%% processing
%%
-spec try_finish_processing(state()) ->
    state().
try_finish_processing(State = #{storage_machine := undefined}) ->
    State;
try_finish_processing(State = #{storage_machine := #{status := processing}}) ->
    process(continuation, undefined, State).

-spec process_start(term(), processing_context(), state()) ->
    state().
process_start(Args, ProcessingCtx, State) ->
    process({init, Args}, ProcessingCtx, State#{storage_machine := new_storage_machine()}).

-spec process(processor_impact(), processing_context(), state()) ->
    state().
process(Impact, ProcessingCtx, State) ->
    try
        process_unsafe(Impact, ProcessingCtx, State)
    catch Class:Reason ->
            ok = do_reply_action({reply, {error, machine_failed}}, ProcessingCtx),
            handle_exception({Class, Reason, erlang:get_stacktrace()}, State)
    end.

-spec handle_exception(mg_utils:exception(), state()) ->
    state().
handle_exception(Exception, State=#{options:=Options, id:=ID, storage_machine := StorageMachine}) ->
    ok = log_machine_error(namespace(Options), ID, Exception),
    NewStorageMachine = StorageMachine#{ status => {error, Exception} },
    transit_state(NewStorageMachine, State).

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
        wait ->
            NewState = transit_state(NewStorageMachine#{status := waiting}, State),
            ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
            NewState
    end.

-spec call_processor(processor_impact(), processing_context(), state()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, #{options := Options, id := ID, storage_machine := #{state := State}}) ->
    % TODO retry processor
    mg_utils:apply_mod_opts(get_options(processor, Options), process_machine, [ID, Impact, ProcessingCtx, State]).

-spec do_reply_action(processor_reply_action(), undefined | processing_context()) ->
    ok.
do_reply_action(noreply, _) ->
    ok;
do_reply_action(_, undefined) ->
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
                storage_machine_to_opaque(NewStorageMachine)
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

-spec namespace(options()) ->
    mg:ns().
namespace(Options) ->
    get_options(namespace, Options).

-spec get_options(atom(), options()) ->
    _.
get_options(Subj=storage_retry_policy, Options) ->
    maps:get(Subj, Options, {exponential, infinity, 2, 10, 60 * 1000});
get_options(Subj, Options) ->
    maps:get(Subj, Options).

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
-spec log_machine_error(mg:ns(), mg:id(), mg_utils:exception()) ->
    ok.
log_machine_error(NS, ID, Exception) ->
    % ok = error_logger:error_msg("[~s:~s] machine failed ~s", [NS, ID, mg_utils:format_exception(Exception)]).
    ok = error_logger:error_msg("[~s:~s] machine failed ~p", [NS, ID, Exception]).

-spec log_transient_exception(mg:ns(), mg:id(), mg_utils:exception()) ->
    ok.
log_transient_exception(NS, ID, Exception) ->
    % ok = error_logger:warning_msg("[~s:~s] transient error ~s", [NS, ID, mg_utils:format_exception(Exception)]).
    ok = error_logger:warning_msg("[~s:~s] transient error ~p", [NS, ID, Exception]).

-spec log_retry(mg:ns(), mg:id(), Timeout::pos_integer()) ->
    ok.
log_retry(NS, ID, RetryTimeout) ->
    ok = error_logger:warning_msg("[~s:~s] retrying in ~p msec", [NS, ID, RetryTimeout]).
