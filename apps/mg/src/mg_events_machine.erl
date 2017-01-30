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

-export([child_spec /2]).
-export([start_link /1]).
-export([start      /3]).
-export([repair     /4]).
-export([call       /4]).
-export([get_machine/3]).

%% mg_machine handler
-behaviour(mg_machine).
-export([process_machine/5]).

%%
%% API
%%
-callback process_signal(_Options, signal_args()) ->
    signal_result().
-callback process_call(_Options, call_args()) ->
    call_result().

%% calls, signals, get_gistory
-type signal_args  () :: {signal(), machine()}.
-type call_args    () :: {term(), machine()}.
-type signal_result() :: {state_change(), complex_action()}.
-type call_result  () :: {term(), state_change(), complex_action()}.
-type state_change () :: {aux_state(), [mg_events:body()]}.
-type signal       () :: {init, term()} | timeout | {repair, term()}.
-type aux_state    () :: binary().

-type machine() :: #{
    ns            => mg:ns(),
    id            => mg:id(),
    history       => mg_events:history(),
    history_range => mg_events:history_range(),
    aux_state     => aux_state()
}.

%% actions
-type complex_action  () :: #{
    timer => set_timer_action() | undefined,
    tag   => tag_action      () | undefined
}.
-type tag_action         () :: mg_machine_tags:tag().
-type set_timer_action   () :: timer().
-type timer              () :: {timeout, timeout_()} | {deadline, calendar:datetime()}.
-type timeout_           () :: non_neg_integer().

-type ref() :: {id, mg:id()} | {tag, mg_machine_tags:tag()}.
-type options() :: #{
    namespace  => mg:ns(),
    storage    => mg_storage:storage(),
    processor  => mg_utils:mod_opts(),
    tagging    => mg_machine_tags:options(),
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

-spec start(options(), mg:id(), term()) ->
    ok.
start(Options, ID, Args) ->
    HRange = {undefined, undefined, forward},
    ok = mg_machine:start(machine_options(Options), ID, {Args, HRange}).

-spec repair(options(), ref(), term(), mg_events:history_range()) ->
    ok.
repair(Options, Ref, Args, HRange) ->
    ok = mg_machine:repair(machine_options(Options), ref2id(Options, Ref), {Args, HRange}).

-spec call(options(), ref(), term(), mg_events:history_range()) ->
    _Resp.
call(Options, Ref, Args, HRange) ->
    mg_machine:call(machine_options(Options), ref2id(Options, Ref), {Args, HRange}).

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
    aux_state       => mg_storage:aux_state(),
    delayed_actions => delayed_actions()
}.
-type delayed_actions() :: #{
    add_tag           => mg_machine_tags:tag() | undefined,
    add_events        => [mg_events:event()],
    new_aux_state     => mg_storage:aux_state(),
    new_events_range  => mg_events:events_range()
} | undefined.

%%

-spec process_machine(options(), mg:id(), mg_machine:processor_impact(), _, mg_machine:machine_state()) ->
    mg_machine:processor_result().
process_machine(Options, ID, Impact, PCtx, null) ->
    PackedState =
        state_to_opaque(
            #{
                events_range    => undefined,
                aux_state       => <<>>,
                delayed_actions => undefined
            }
        ),
    process_machine(Options, ID, Impact, PCtx, PackedState);
process_machine(Options, ID, Impact, PCtx, PackedState) ->
    {ReplyAction, ProcessingFlowAction, NewState} =
        process_machine_(Options, ID, Impact, PCtx, opaque_to_state(PackedState)),
    {ReplyAction, ProcessingFlowAction, state_to_opaque(NewState)}.

%%

-spec process_machine_(options(), mg:id(), mg_machine:processor_impact(), _, state()) ->
    _TODO.
process_machine_(Options, ID, {Subj, {Args, HRange}}, _, State = #{events_range := EventsRange}) ->
    % обработка стандартных запросов
    Machine = machine(Options, ID, State, HRange),
    {Reply, DelayedActions} =
        case Subj of
            init   -> process_signal(Options, {init  , Args}, Machine, EventsRange);
            repair -> process_signal(Options, {repair, Args}, Machine, EventsRange);
            call   -> process_call  (Options,          Args , Machine, EventsRange)
        end,
    {noreply, {continue, Reply}, State#{delayed_actions := DelayedActions}};
process_machine_(Options, ID, continuation, #{state := Reply}, State = #{delayed_actions := DelayedActions}) ->
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
    ok = add_tag(Options, Tag, ID),
    ok = store_events(Options, ID, Events),
    ok = push_events_to_event_sink(Options, ID, Events),

    {{reply, Reply}, wait, apply_delayed_actions_to_state(DelayedActions, State)}.

-spec add_tag(options(), undefined | mg_machine_tags:tag(), mg:id()) ->
    ok.
add_tag(_, undefined, _) ->
    ok;
add_tag(Options, Tag, ID) ->
    % TODO retry
    case mg_machine_tags:add_tag(tags_machine_options(Options), Tag, ID) of
        ok ->
            ok;
        {already_exists, OtherMachineID} ->
            % была договорённость, что при двойном тэгировании роняем машину
            exit({double_tagging, OtherMachineID})
    end.

-spec store_events(options(), mg:id(), [mg_events:event()]) ->
    ok.
store_events(Options, MachineID, Events) ->
    lists:foreach(
        fun({Key, Value}) ->
            _ = mg_storage:put(events_storage_options(Options), Key, undefined, Value, [])
        end,
        events_to_kvs(MachineID, Events)
    ).


-spec push_events_to_event_sink(options(), mg:id(), [mg_events:event()]) ->
    ok.
push_events_to_event_sink(Options, ID, Events) ->
    Namespace = get_option(namespace, Options),
    case maps:get(event_sink, Options, undefined) of
        {EventSinkID, EventSinkOptions} ->
            % TODO retry
            ok = mg_events_sink:add_events(EventSinkOptions, EventSinkID, Namespace, ID, Events);
        undefined ->
            ok
    end.

-spec apply_delayed_actions_to_state(delayed_actions(), state()) ->
    state().
apply_delayed_actions_to_state(#{new_aux_state := NewAuxState, new_events_range := NewEventsRange}, State) ->
    State#{
        events_range    := NewEventsRange,
        aux_state       := NewAuxState,
        delayed_actions := undefined
    }.

%%

-spec process_signal(options(), signal(), machine(), mg_events:events_range()) ->
    {ok, delayed_actions()}.
process_signal(Options, Signal, Machine, EventsRange) ->
    {StateChange, ComplexAction} =
        mg_utils:apply_mod_opts(get_option(processor, Options), process_signal, [{Signal, Machine}]),
    {ok, handle_processing_result(StateChange, ComplexAction, EventsRange)}.

-spec process_call(options(), mg:opaque(), machine(), mg_events:events_range()) ->
    {_Resp, delayed_actions()}.
process_call(Options, Args, Machine, EventsRange) ->
    {Resp, StateChange, ComplexAction} =
        mg_utils:apply_mod_opts(get_option(processor, Options), process_call, [{Args, Machine}]),
    {Resp, handle_processing_result(StateChange, ComplexAction, EventsRange)}.

-spec handle_processing_result(state_change(), complex_action(), mg_events:events_range()) ->
    delayed_actions().
handle_processing_result(StateChange, ComplexAction, EventsRange) ->
    handle_state_change(StateChange, EventsRange, handle_complex_action(ComplexAction, #{})).

-spec handle_state_change(state_change(), mg_events:events_range(), delayed_actions()) ->
    delayed_actions().
handle_state_change({AuxState, EventsBodies}, EventsRange, DelayedActions) ->
    {Events, NewEventsRange} = mg_events:generate_events_with_range(EventsBodies, EventsRange),
    DelayedActions#{
        add_events       => Events,
        new_aux_state    => AuxState,
        new_events_range => NewEventsRange
    }.

%% TODO timers
-spec handle_complex_action(complex_action(), delayed_actions()) ->
    delayed_actions().
handle_complex_action(ComplexAction, DelayedActions) ->
    DelayedActions#{
        add_tag => maps:get(tag, ComplexAction, undefined)
    }.

%%

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options = #{namespace := Namespace, storage := Storage}) ->
    #{
        namespace => mg_utils:concatenate_namespaces(Namespace, <<"machines">>),
        processor => {?MODULE, Options},
        storage   => Storage
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
machine(Options = #{namespace := Namespace}, ID, #{events_range := EventsRange, aux_state := AuxState}, HRange) ->
    #{
        ns            => Namespace,
        id            => ID,
        history       => get_events(Options, ID, EventsRange, HRange),
        history_range => HRange,
        aux_state     => AuxState
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
    mg:opaque().
state_to_opaque(#{events_range := EventsRange, aux_state := AuxState, delayed_actions := DelayedActions}) ->
    [1,
        mg_events:events_range_to_opaque(EventsRange),
        AuxState,
        maybe_to_opaque(DelayedActions, fun delayed_actions_to_opaque/1)
    ].

-spec opaque_to_state(mg:opaque()) ->
    state().
opaque_to_state([1, EventsRange, AuxState, DelayedActions]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => AuxState,
        delayed_actions => maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1)
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
    mg:opaque().
delayed_actions_to_opaque(undefined) ->
    null;
delayed_actions_to_opaque(DelayedActions) ->
    #{
        add_tag          := Tag,
        add_events       := Events,
        new_aux_state    := AuxState,
        new_events_range := EventsRange
    } = DelayedActions,
    [1,
        maybe_to_opaque(Tag, fun identity/1),
        mg_events:events_to_opaques(Events),
        AuxState,
        mg_events:events_range_to_opaque(EventsRange)
    ].

-spec opaque_to_delayed_actions(mg:opaque()) ->
    delayed_actions().
opaque_to_delayed_actions(null) ->
    undefined;
opaque_to_delayed_actions([1, Tag, Events, AuxState, EventsRange]) ->
    #{
        add_tag          => maybe_from_opaque(Tag, fun identity/1),
        add_events       => mg_events:opaques_to_events(Events),
        new_aux_state    => AuxState,
        new_events_range => mg_events:opaque_to_events_range(EventsRange)
    }.

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

