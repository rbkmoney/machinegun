%%%
%%% Оперирующая эвентами машина.
%%% Добавляет понятие эвента, тэга и ссылки(ref).
%%% Отсылает эвенты в event sink (если он указан).
%%%
%%% Эвенты в машине всегда идут в таком порядке, что слева самые старые.
%%%
-module(mg_events_machine).

%% API
-export_type([options         /0]).
-export_type([ref             /0]).
-export_type([machine         /0]).
-export_type([tag_action      /0]).
-export_type([set_timer_action/0]).
-export_type([complex_action  /0]).
-export_type([state_change    /0]).
-export_type([signal          /0]).
-export_type([signal_args     /0]).
-export_type([call_args       /0]).
-export_type([signal_result   /0]).
-export_type([call_result     /0]).
-export_type([request_context /0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([start      /4]).
-export([repair     /5]).
-export([call       /5]).
-export([get_machine/3]).

%% mg_machine handler
-behaviour(mg_machine).
-export([processor_child_spec/1, process_machine/6]).

%%
%% API
%%
-callback processor_child_spec(_Options) ->
    mg_utils_supervisor_wrapper:child_spec().
-callback process_signal(_Options, request_context(), signal_args()) ->
    signal_result().
-callback process_call(_Options, request_context(), call_args()) ->
    call_result().
-optional_callbacks([processor_child_spec/1]).

%% calls, signals, get_gistory
-type signal_args    () :: {signal(), machine()}.
-type call_args      () :: {term(), machine()}.
-type signal_result  () :: {state_change(), complex_action()}.
-type call_result    () :: {term(), state_change(), complex_action()}.
-type state_change   () :: {aux_state(), [mg_events:body()]}.
-type signal         () :: {init, term()} | timeout | {repair, term()}.
-type aux_state      () :: mg_storage:opaque().
-type request_context() :: mg:request_context().

-type machine() :: #{
    ns            => mg:ns(),
    id            => mg:id(),
    history       => [mg_events:event()],
    history_range => mg_events:history_range(),
    aux_state     => aux_state(),
    timer         => int_timer()
}.

-type int_timer() :: {genlib_time:ts(), request_context(), pos_integer()}.

%% actions
-type complex_action  () :: #{
    timer => set_timer_action() | undefined,
    tag   => tag_action      () | undefined
}.
-type tag_action         () :: mg_machine_tags:tag().
-type set_timer_action   () :: timer().
-type timer              () :: {timeout, timeout_()} | {deadline, calendar:datetime()} | cancel.
-type timeout_           () :: non_neg_integer().

-type ref() :: {id, mg:id()} | {tag, mg_machine_tags:tag()}.
-type options() :: #{
    namespace  => mg:ns(),
    storage    => mg_storage:storage(),
    processor  => mg_utils:mod_opts(),
    tagging    => mg_machine_tags:options(),
    logger     => mg_machine_logger:handler(),
    event_sink => {mg:id(), mg_events_sink:options()} % optional
}.


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
            mg_machine     :child_spec(machine_options       (Options), automaton     ),
            mg_machine_tags:child_spec(tags_machine_options  (Options), tags          ),
            mg_storage     :child_spec(events_storage_options(Options), events_storage)
        ]
    ).

-define(default_deadline, mg_utils:timeout_to_deadline(5000)).

-spec start(options(), mg:id(), term(), request_context()) ->
    ok.
start(Options, ID, Args, ReqCtx) ->
    HRange = {undefined, undefined, forward},
    ok = mg_machine:start(
            machine_options(Options),
            ID,
            {Args, HRange},
            ReqCtx,
            mg_utils:default_deadline()
        ).

-spec repair(options(), ref(), term(), mg_events:history_range(), request_context()) ->
    ok.
repair(Options, Ref, Args, HRange, ReqCtx) ->
    ok = mg_machine:repair(
            machine_options(Options),
            ref2id(Options, Ref),
            {Args, HRange},
            ReqCtx,
            mg_utils:default_deadline()
        ).

-spec call(options(), ref(), term(), mg_events:history_range(), request_context()) ->
    _Resp.
call(Options, Ref, Args, HRange, ReqCtx) ->
    mg_machine:call(
        machine_options(Options),
        ref2id(Options, Ref),
        {Args, HRange},
        ReqCtx,
        mg_utils:default_deadline()
    ).

-spec get_machine(options(), ref(), mg_events:history_range()) ->
    machine().
get_machine(Options, Ref, HRange) ->
    % нужно понимать, что эти операции разнесены по времени, и тут могут быть рэйсы
    ID = ref2id(Options, Ref),
    State = opaque_to_state(mg_machine:get(machine_options(Options), ID)),
    machine(Options, ID, State, HRange).

%%

-spec ref2id(options(), ref()) ->
    mg:id().
ref2id(_, {id, ID}) ->
    ID;
ref2id(Options, {tag, Tag}) ->
    case mg_machine_tags:resolve_tag(tags_machine_options(Options), Tag) of
        undefined -> throw(machine_not_found);
        ID        -> ID
    end.

%%
%% mg_processor handler
%%
-type state() :: #{
    events_range    => mg_events:events_range(),
    aux_state       => aux_state(),
    delayed_actions => delayed_actions(),
    timer           => int_timer() | undefined
}.
-type delayed_actions() :: #{
    add_tag           => mg_machine_tags:tag() | undefined,
    new_timer         => int_timer() | undefined | unchanged,
    add_events        => [mg_events:event()],
    new_aux_state     => aux_state(),
    new_events_range  => mg_events:events_range()
} | undefined.

%%

-spec processor_child_spec(options()) ->
    mg_utils_supervisor_wrapper:child_spec().
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(processor_options(Options), processor_child_spec, empty_child_spec).

-spec process_machine(options(), mg:id(), mg_machine:processor_impact(), _, ReqCtx, mg_machine:machine_state()) ->
    mg_machine:processor_result()
when ReqCtx :: request_context().
process_machine(Options, ID, Impact, PCtx, ReqCtx, PackedState) ->
    {ReplyAction, ProcessingFlowAction, NewState} =
        try
            process_machine_(Options, ID, Impact, PCtx, ReqCtx, opaque_to_state(PackedState))
        catch
            throw:{transient, Reason} ->
                erlang:raise(throw, {transient, Reason}, erlang:get_stacktrace());
            throw:Reason ->
                erlang:throw({transient, {processor_unavailable, Reason}})
        end,
    {ReplyAction, ProcessingFlowAction, state_to_opaque(NewState)}.

%%

-spec process_machine_(options(), mg:id(), mg_machine:processor_impact() | {timeout, _}, _, ReqCtx, state()) ->
    _TODO
when ReqCtx :: request_context().
process_machine_(Options, ID, Subj=timeout, PCtx, ReqCtx, State) ->
    process_machine_(Options, ID, {Subj, {undefined, {undefined, undefined, forward}}}, PCtx, ReqCtx, State);
process_machine_(Options, ID, {Subj, {Args, HRange}}, _, ReqCtx, State = #{events_range := EventsRange}) ->
    % обработка стандартных запросов
    Machine = machine(Options, ID, State, HRange),
    {Reply, DelayedActions} =
        case Subj of
            init    -> process_signal(Options, ReqCtx, {init  , Args}, Machine, EventsRange);
            repair  -> process_signal(Options, ReqCtx, {repair, Args}, Machine, EventsRange);
            timeout -> process_signal(Options, ReqCtx,  timeout      , Machine, EventsRange);
            call    -> process_call  (Options, ReqCtx,          Args , Machine, EventsRange)
        end,
    {noreply, {continue, Reply}, State#{delayed_actions := DelayedActions}};
process_machine_(Options, ID, continuation, PCtx, ReqCtx, State = #{delayed_actions := DelayedActions}) ->
    % отложенные действия (эвент синк, тэг)
    %
    % надо понимать, что:
    %  - эвенты сохраняются в сторадж
    %  - создатся тэг
    %  - эвенты добавляются в event sink
    %  - обновится event_range
    %  - отсылается ответ
    % надо быть аккуратнее, мест чтобы накосячить тут вагон и маленькая тележка  :-\
    %
    % действия должны обязательно произойти в конце концов (таймаута нет), либо машина должна упасть
    #{add_tag := Tag, add_events := Events} = DelayedActions,
    ok =                   add_tag(Options, ID, ReqCtx, Tag   ),
    ok =              store_events(Options, ID, ReqCtx, Events),
    ok = push_events_to_event_sink(Options, ID, ReqCtx, Events),

    ReplyAction =
        case PCtx of
            #{state := Reply} ->
                {reply, Reply};
            undefined ->
                noreply
        end,
    NewState = apply_delayed_actions_to_state(DelayedActions, State),
    {ReplyAction, state_to_flow_action(NewState), NewState}.

-spec add_tag(options(), mg:id(), request_context(), undefined | mg_machine_tags:tag()) ->
    ok.
add_tag(_, _, _, undefined) ->
    ok;
add_tag(Options, ID, ReqCtx, Tag) ->
    % TODO retry
    case mg_machine_tags:add_tag(tags_machine_options(Options), Tag, ID, ReqCtx, mg_utils:default_deadline()) of
        ok ->
            ok;
        {already_exists, OtherMachineID} ->
            % была договорённость, что при двойном тэгировании роняем машину
            exit({double_tagging, OtherMachineID})
    end.

-spec store_events(options(), mg:id(), request_context(), [mg_events:event()]) ->
    ok.
store_events(Options, ID, _, Events) ->
    lists:foreach(
        fun({Key, Value}) ->
            _ = mg_storage:put(events_storage_options(Options), Key, undefined, Value, [])
        end,
        events_to_kvs(ID, Events)
    ).


-spec push_events_to_event_sink(options(), mg:id(), request_context(), [mg_events:event()]) ->
    ok.
push_events_to_event_sink(Options, ID, ReqCtx, Events) ->
    Namespace = get_option(namespace, Options),
    case maps:get(event_sink, Options, undefined) of
        {EventSinkID, EventSinkOptions} ->
            ok = mg_events_sink:add_events(
                    EventSinkOptions,
                    EventSinkID,
                    Namespace,
                    ID,
                    Events,
                    ReqCtx,
                    mg_utils:default_deadline()
                );
        undefined ->
            ok
    end.

-spec state_to_flow_action(state()) ->
    mg_machine:processor_flow_action().
state_to_flow_action(#{timer := undefined}) ->
    sleep;
state_to_flow_action(#{timer := {Timestamp, ReqCtx, HandlingTimeout}}) ->
    {wait, Timestamp, ReqCtx, HandlingTimeout}.

-spec apply_delayed_actions_to_state(delayed_actions(), state()) ->
    state().
apply_delayed_actions_to_state(DA = #{new_aux_state := NewAuxState, new_events_range := NewEventsRange}, State) ->
    apply_delayed_timer_actions_to_state(
        DA,
        State#{
            events_range    := NewEventsRange,
            aux_state       := NewAuxState,
            delayed_actions := undefined
        }
    ).

-spec apply_delayed_timer_actions_to_state(delayed_actions(), state()) ->
    state().
apply_delayed_timer_actions_to_state(#{new_timer := unchanged}, State) ->
    State;
apply_delayed_timer_actions_to_state(#{new_timer := Timer}, State) ->
    State#{timer := Timer}.

%%

-spec process_signal(options(), request_context(), signal(), machine(), mg_events:events_range()) ->
    {ok, delayed_actions()}.
process_signal(Options, ReqCtx, Signal, Machine, EventsRange) ->
    {StateChange, ComplexAction} =
        mg_utils:apply_mod_opts(get_option(processor, Options), process_signal, [ReqCtx, {Signal, Machine}]),
    {ok, handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx)}.

-spec process_call(options(), request_context(), term(), machine(), mg_events:events_range()) ->
    {_Resp, delayed_actions()}.
process_call(Options, ReqCtx, Args, Machine, EventsRange) ->
    {Resp, StateChange, ComplexAction} =
        mg_utils:apply_mod_opts(get_option(processor, Options), process_call, [ReqCtx, {Args, Machine}]),
    {Resp, handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx)}.

-spec handle_processing_result(state_change(), complex_action(), mg_events:events_range(), request_context()) ->
    delayed_actions().
handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx) ->
    handle_state_change(StateChange, EventsRange, handle_complex_action(ComplexAction, #{}, ReqCtx)).

-spec handle_state_change(state_change(), mg_events:events_range(), delayed_actions()) ->
    delayed_actions().
handle_state_change({AuxState, EventsBodies}, EventsRange, DelayedActions) ->
    {Events, NewEventsRange} = mg_events:generate_events_with_range(EventsBodies, EventsRange),
    DelayedActions#{
        add_events       => Events,
        new_aux_state    => AuxState,
        new_events_range => NewEventsRange
    }.

-spec handle_complex_action(complex_action(), delayed_actions(), request_context()) ->
    delayed_actions().
handle_complex_action(ComplexAction, DelayedActions, ReqCtx) ->
    DelayedActions#{
        add_tag   => maps:get(tag, ComplexAction, undefined),
        new_timer => get_timer_action(ComplexAction, ReqCtx)
    }.

-spec get_timer_action(complex_action(), request_context()) ->
    genlib_time:ts() | undefined.
get_timer_action(ComplexAction, ReqCtx) ->
    % TODO получать handling timeout свыше!
    HandlingTimeout = 30000,
    case maps:get(timer, ComplexAction, undefined) of
        undefined ->
            unchanged;
        cancel ->
            undefined;
        {timeout, Timeout} ->
            {genlib_time:now() + Timeout, ReqCtx, HandlingTimeout};
        {deadline, Deadline} ->
            {genlib_time:daytime_to_unixtime(Deadline), ReqCtx, HandlingTimeout}
    end.

%%

-spec processor_options(options()) ->
    mg_utils:mod_opts().
processor_options(Options) ->
    maps:get(processor, Options).

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options = #{namespace := Namespace, storage := Storage, logger := Logger}) ->
    #{
        namespace => mg_utils:concatenate_namespaces(Namespace, <<"machines">>),
        processor => {?MODULE, Options},
        storage   => Storage,
        logger    => Logger
    }.

-spec events_storage_options(options()) ->
    mg_storage:options().
events_storage_options(#{namespace := Namespace, storage := Storage}) ->
    #{
        namespace => mg_utils:concatenate_namespaces(Namespace, <<"events">>),
        module    => Storage
    }.

-spec tags_machine_options(options()) ->
    mg_machine_tags:options().
tags_machine_options(#{tagging := Options}) ->
    Options.

-spec get_option(atom(), options()) ->
    _.
get_option(Subj, Options) ->
    maps:get(Subj, Options).

%%

-spec machine(options(), mg:id(), state(), mg_events:history_range()) ->
    machine().
machine(Options = #{namespace := Namespace}, ID, State, HRange) ->
    #{events_range := EventsRange, aux_state := AuxState, timer := Timer} = State,
    #{
        ns            => Namespace,
        id            => ID,
        history       => get_events(Options, ID, EventsRange, HRange),
        history_range => HRange,
        aux_state     => AuxState,
        timer         => Timer
    }.

-spec get_events(options(), mg:id(), mg_events:events_range(), mg_events:history_range()) ->
    [mg_events:event()].
get_events(Options, ID, EventsRange, HRange) ->
    EventsKeys = get_events_keys(ID, EventsRange, HRange),
    kvs_to_events(ID, [
        {Key, Value} ||
        {Key, {_, Value}} <- [{Key, mg_storage:get(events_storage_options(Options), Key)} || Key <- EventsKeys]
    ]).

-spec get_events_keys(mg:id(), mg_events:events_range(), mg_events:history_range()) ->
    [mg_storage:key()].
get_events_keys(ID, EventsRange, HistoryRange) ->
    [
        mg_events:add_machine_id(ID, mg_events:event_id_to_key(EventID))
        ||
        EventID <- mg_events:get_event_ids(EventsRange, HistoryRange)
    ].

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(State) ->
    #{events_range := EventsRange, aux_state := AuxState, delayed_actions := DelayedActions, timer := Timer} = State,
    [2,
        mg_events:events_range_to_opaque(EventsRange),
        AuxState,
        maybe_to_opaque(DelayedActions, fun delayed_actions_to_opaque/1),
        maybe_to_opaque(Timer, fun int_timer_to_opaque/1)
    ].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
%% при создании есть момент (continuation) когда ещё нет стейта
opaque_to_state(null) ->
    #{
        events_range    => undefined,
        aux_state       => <<>>,
        delayed_actions => undefined,
        timer           => undefined
    };
opaque_to_state([1, EventsRange, AuxState, DelayedActions]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => AuxState,
        delayed_actions => maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => undefined
    };
opaque_to_state([2, EventsRange, AuxState, DelayedActions, Timer]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => AuxState,
        delayed_actions => maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    }.


-spec events_to_kvs(mg:id(), [mg_events:event()]) ->
    [mg_storage:kv()].
events_to_kvs(MachineID, Events) ->
    mg_events:add_machine_id(MachineID, mg_events:events_to_kvs(Events)).

-spec kvs_to_events(mg:id(), [mg_storage:kv()]) ->
    [mg_events:event()].
kvs_to_events(MachineID, Kvs) ->
    mg_events:kvs_to_events(mg_events:remove_machine_id(MachineID, Kvs)).

-spec delayed_actions_to_opaque(delayed_actions()) ->
    mg_storage:opaque().
delayed_actions_to_opaque(undefined) ->
    null;
delayed_actions_to_opaque(DelayedActions) ->
    #{
        add_tag          := Tag,
        new_timer        := Timer,
        add_events       := Events,
        new_aux_state    := AuxState,
        new_events_range := EventsRange
    } = DelayedActions,
    [1,
        maybe_to_opaque(Tag, fun identity/1),
        delayed_timer_actions_to_opaque(Timer),
        mg_events:events_to_opaques(Events),
        AuxState,
        mg_events:events_range_to_opaque(EventsRange)
    ].

-spec opaque_to_delayed_actions(mg_storage:opaque()) ->
    delayed_actions().
opaque_to_delayed_actions(null) ->
    undefined;
opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]) ->
    #{
        add_tag          => maybe_from_opaque(Tag, fun identity/1),
        new_timer        => opaque_to_delayed_timer_actions(Timer),
        add_events       => mg_events:opaques_to_events(Events),
        new_aux_state    => AuxState,
        new_events_range => mg_events:opaque_to_events_range(EventsRange)
    }.

-spec delayed_timer_actions_to_opaque(genlib_time:ts() | undefined | unchanged) ->
    mg_storage:opaque().
delayed_timer_actions_to_opaque(undefined) ->
    null;
delayed_timer_actions_to_opaque(unchanged) ->
    <<"unchanged">>;
delayed_timer_actions_to_opaque(Timer) ->
    int_timer_to_opaque(Timer).

-spec opaque_to_delayed_timer_actions(mg_storage:opaque()) ->
    genlib_time:ts() | undefined | unchanged.
opaque_to_delayed_timer_actions(null) ->
    undefined;
opaque_to_delayed_timer_actions(<<"unchanged">>) ->
    unchanged;
opaque_to_delayed_timer_actions(Timer) ->
    opaque_to_int_timer(Timer).

-spec int_timer_to_opaque(int_timer()) ->
    mg_storage:opaque().
int_timer_to_opaque({Timestamp, ReqCtx, HandlingTimeout}) ->
    [1, Timestamp, ReqCtx, HandlingTimeout].

-spec opaque_to_int_timer(mg_storage:opaque()) ->
    int_timer().
opaque_to_int_timer([1, Timestamp, ReqCtx, HandlingTimeout]) ->
    {Timestamp, ReqCtx, HandlingTimeout}.

-spec maybe_to_opaque(undefined | T0, fun((T0) -> T1)) ->
    T1.
maybe_to_opaque(undefined, _) ->
    null;
maybe_to_opaque(T0, ToT1) ->
    ToT1(T0).

-spec maybe_from_opaque(null | T0, fun((T0) -> T1)) ->
    T1.
maybe_from_opaque(null, _) ->
    undefined;
maybe_from_opaque(T0, ToT1) ->
    ToT1(T0).

-spec identity(T) ->
    T.
identity(V) ->
    V.
