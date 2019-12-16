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
%%% Оперирующая эвентами машина.
%%% Добавляет понятие эвента, тэга и ссылки(ref).
%%% Отсылает эвенты в event sink (если он указан).
%%%
%%% Эвенты в машине всегда идут в таком порядке, что слева самые старые.
%%%
-module(mg_events_machine).

-include_lib("mg/include/pulse.hrl").

%% API
-export_type([options         /0]).
-export_type([storage_options /0]).
-export_type([ref             /0]).
-export_type([machine         /0]).
-export_type([tag_action      /0]).
-export_type([timer_action    /0]).
-export_type([complex_action  /0]).
-export_type([state_change    /0]).
-export_type([signal          /0]).
-export_type([signal_args     /0]).
-export_type([call_args       /0]).
-export_type([repair_args     /0]).
-export_type([signal_result   /0]).
-export_type([call_result     /0]).
-export_type([repair_result   /0]).
-export_type([request_context /0]).

-export([child_spec   /2]).
-export([start_link   /1]).
-export([start        /5]).
-export([repair       /6]).
-export([simple_repair/4]).
-export([call         /6]).
-export([get_machine  /3]).
-export([remove       /4]).

%% mg_machine handler
-behaviour(mg_machine).
-export([processor_child_spec/1, process_machine/7]).

%%
%% API
%%
-callback processor_child_spec(_Options) ->
    supervisor:child_spec() | undefined.
-callback process_signal(_Options, request_context(), deadline(), signal_args()) ->
    signal_result().
-callback process_call(_Options, request_context(), deadline(), call_args()) ->
    call_result().
-callback process_repair(_Options, request_context(), deadline(), repair_args()) ->
    repair_result() | no_return().
-optional_callbacks([processor_child_spec/1]).

%% calls, signals, get_gistory
-type signal_args    () :: {signal(), machine()}.
-type call_args      () :: {term(), machine()}.
-type repair_args    () :: {term(), machine()}.
-type signal_result  () :: {state_change(), complex_action()}.
-type call_result    () :: {term(), state_change(), complex_action()}.
-type repair_result  () :: {term(), state_change(), complex_action()}.
-type state_change   () :: {aux_state(), [mg_events:body()]}.
-type signal         () :: {init, term()} | timeout | {repair, term()}.
-type aux_state      () :: mg_events:content().
-type request_context() :: mg:request_context().

-type machine() :: #{
    ns            => mg:ns(),
    id            => mg:id(),
    history       => [mg_events:event()],
    history_range => mg_events:history_range(),
    aux_state     => aux_state(),
    timer         => int_timer()
}.

%% TODO сделать более симпатично
-type int_timer() :: {genlib_time:ts(), request_context(), pos_integer(), mg_events:history_range()}.

%% actions
-type complex_action  () :: #{
    timer  => timer_action() | undefined,
    tag    => tag_action  () | undefined,
    remove => remove         | undefined
}.
-type tag_action  () :: mg_machine_tags:tag().
-type timer_action() ::
      {set_timer, timer(), mg_events:history_range() | undefined, Timeout::pos_integer() | undefined}
    |  unset_timer
.
-type timer       () :: {timeout, timeout_()} | {deadline, calendar:datetime()}.
-type timeout_    () :: non_neg_integer().
-type deadline    () :: mg_deadline:deadline().

-type ref() :: {id, mg:id()} | {tag, mg_machine_tags:tag()}.
-type options() :: #{
    namespace                  => mg:ns(),
    events_storage             => storage_options(),
    processor                  => mg_utils:mod_opts(),
    tagging                    => mg_machine_tags:options(),
    machines                   => mg_machine:options(),
    pulse                      => mg_pulse:handler(),
    event_sinks                => [mg_events_sink:handler()],
    default_processing_timeout => timeout()
}.
-type storage_options() :: mg_utils:mod_opts(map()).  % like mg_storage:options() except `name`


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
            mg_storage     :child_spec(events_storage_options(Options), events_storage),
            mg_machine_tags:child_spec(tags_machine_options  (Options), tags     ),
            mg_machine     :child_spec(machine_options       (Options), automaton)
        ])
    ).

-define(default_deadline, mg_deadline:from_timeout(5000)).

-spec start(options(), mg:id(), term(), request_context(), deadline()) ->
    ok.
start(Options, ID, Args, ReqCtx, Deadline) ->
    HRange = {undefined, undefined, forward},
    ok = mg_machine:start(
            machine_options(Options),
            ID,
            {Args, HRange},
            ReqCtx,
            Deadline
        ).

-spec repair(options(), ref(), term(), mg_events:history_range(), request_context(), deadline()) ->
    _Resp.
repair(Options, Ref, Args, HRange, ReqCtx, Deadline) ->
    ok = mg_machine:repair(
            machine_options(Options),
            ref2id(Options, Ref),
            {Args, HRange},
            ReqCtx,
            Deadline
        ),
    <<"ok">>.

-spec simple_repair(options(), ref(), request_context(), deadline()) ->
    ok.
simple_repair(Options, Ref, ReqCtx, Deadline) ->
    ok = mg_machine:simple_repair(
            machine_options(Options),
            ref2id(Options, Ref),
            ReqCtx,
            Deadline
        ).

-spec call(options(), ref(), term(), mg_events:history_range(), request_context(), deadline()) ->
    _Resp.
call(Options, Ref, Args, HRange, ReqCtx, Deadline) ->
    mg_machine:call(
        machine_options(Options),
        ref2id(Options, Ref),
        {Args, HRange},
        ReqCtx,
        Deadline
    ).

-spec get_machine(options(), ref(), mg_events:history_range()) ->
    machine().
get_machine(Options, Ref, HRange) ->
    % нужно понимать, что эти операции разнесены по времени, и тут могут быть рэйсы
    ID = ref2id(Options, Ref),
    InitialState = opaque_to_state(mg_machine:get(machine_options(Options), ID)),
    {EffectiveState, ExtraEvents} = mg_utils:throw_if_undefined(
        try_apply_delayed_actions(InitialState),
        {logic, machine_not_found}
    ),
    machine(Options, ID, EffectiveState, ExtraEvents, HRange).

-spec remove(options(), mg:id(), request_context(), deadline()) ->
    ok.
remove(Options, ID, ReqCtx, Deadline) ->
    mg_machine:call(machine_options(Options), ID, remove, ReqCtx, Deadline).

%%

-spec ref2id(options(), ref()) ->
    mg:id() | no_return().
ref2id(_, {id, ID}) ->
    ID;
ref2id(Options, {tag, Tag}) ->
    case mg_machine_tags:resolve(tags_machine_options(Options), Tag) of
        undefined -> throw({logic, machine_not_found});
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
    remove            => remove | undefined,
    add_events        => [mg_events:event()],
    new_aux_state     => aux_state(),
    new_events_range  => mg_events:events_range()
} | undefined.

%%

-spec processor_child_spec(options()) ->
    supervisor:child_spec() | undefined.
processor_child_spec(Options) ->
    mg_utils:apply_mod_opts_if_defined(processor_options(Options), processor_child_spec, undefined).

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, PackedState) -> Result when
    Options :: options(),
    ID :: mg:id(),
    Impact :: mg_machine:processor_impact(),
    PCtx :: mg_machine:processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    PackedState :: mg_machine:machine_state(),
    Result :: mg_machine:processor_result().
process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, PackedState) ->
    {ReplyAction, ProcessingFlowAction, NewState} =
        try
            process_machine_(Options, ID, Impact, PCtx, ReqCtx, Deadline, opaque_to_state(PackedState))
        catch
            throw:{transient, Reason}:ST ->
                erlang:raise(throw, {transient, Reason}, ST);
            throw:{business, _} = Error ->
                erlang:throw(Error);
            throw:Reason ->
                erlang:throw({transient, {processor_unavailable, Reason}})
        end,
    {ReplyAction, ProcessingFlowAction, state_to_opaque(NewState)}.

%%

-spec process_machine_(Options, ID, Impact, PCtx, ReqCtx, Deadline, State) -> Result when
    Options :: options(),
    ID :: mg:id(),
    Impact :: mg_machine:processor_impact() | {'timeout', _},
    PCtx :: mg_machine:processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    State :: state(),
    Result :: {mg_machine:processor_reply_action(), mg_machine:processor_flow_action(), state()} | no_return().
process_machine_(Options, ID, Subj=timeout, PCtx, ReqCtx, Deadline, State=#{timer := {_, _, _, HRange}}) ->
    NewState = State#{timer := undefined},
    process_machine_(Options, ID, {Subj, {undefined, HRange}}, PCtx, ReqCtx, Deadline, NewState);
process_machine_(_, _, {call, remove}, _, _, _, State) ->
    % TODO удалить эвенты (?)
    {{reply, ok}, remove, State};
process_machine_(Options, ID, {Subj, {Args, HRange}}, _, ReqCtx, Deadline, State) ->
    % обработка стандартных запросов
    {EffectiveState, ExtraEvents} = try_apply_delayed_actions(State),
    #{events_range := EventsRange} = EffectiveState,
    Machine = machine(Options, ID, EffectiveState, ExtraEvents, HRange),
    {Reply, DelayedActions} =
        case Subj of
            init    -> process_signal(Options, ReqCtx, Deadline, {init  , Args}, Machine, EventsRange);
            timeout -> process_signal(Options, ReqCtx, Deadline,  timeout      , Machine, EventsRange);
            repair  -> process_signal(Options, ReqCtx, Deadline, {repair, Args}, Machine, EventsRange);
            % repair  -> process_repair(Options, ReqCtx, Deadline,          Args , Machine, EventsRange);
            call    -> process_call  (Options, ReqCtx, Deadline,          Args , Machine, EventsRange)
        end,
    NewState = add_delayed_actions(DelayedActions, State),
    {noreply, {continue, Reply}, NewState};
process_machine_(Options, ID, continuation, PCtx, ReqCtx, Deadline, State = #{delayed_actions := DelayedActions}) ->
    % отложенные действия (эвент синк, тэг)
    %
    % надо понимать, что:
    %  - эвенты добавляются в event sink
    %  - создатся тэг
    %  - эвенты сохраняются в сторадж
    %  - обновится event_range
    %  - отсылается ответ
    %  - если есть удаление, то удаляется
    % надо быть аккуратнее, мест чтобы накосячить тут вагон и маленькая тележка  :-\
    %
    % действия должны обязательно произойти в конце концов (таймаута нет), либо машина должна упасть
    #{add_tag := Tag, add_events := Events} = DelayedActions,
    ok = push_events_to_event_sinks(Options, ID, ReqCtx, Deadline, Events),
    ok =                    add_tag(Options, ID, ReqCtx, Deadline, Tag   ),
    ok =               store_events(Options, ID, ReqCtx, Events),

    ReplyAction =
        case PCtx of
            #{state := Reply} ->
                {reply, Reply};
            undefined ->
                noreply
        end,
    {FlowAction, NewState} =
        case apply_delayed_actions_to_state(DelayedActions, State) of
            remove ->
                {remove, State};
            NewState_ ->
                {state_to_flow_action(NewState_), NewState_}
        end,
    ok = emit_action_beats(Options, ID, ReqCtx, DelayedActions),
    {ReplyAction, FlowAction, NewState}.

-spec add_tag(options(), mg:id(), request_context(), deadline(), undefined | mg_machine_tags:tag()) ->
    ok.
add_tag(_, _, _, _, undefined) ->
    ok;
add_tag(Options, ID, ReqCtx, Deadline, Tag) ->
    case mg_machine_tags:add(tags_machine_options(Options), Tag, ID, ReqCtx, Deadline) of
        ok ->
            ok;
        {already_exists, OtherMachineID} ->
            case mg_machine:is_exist(machine_options(Options), OtherMachineID) of
                true ->
                    % была договорённость, что при двойном тэгировании роняем машину
                    exit({double_tagging, OtherMachineID});
                false ->
                    % это забытый после удаления тэг
                    ok = mg_machine_tags:replace(
                            tags_machine_options(Options), Tag, ID, ReqCtx, Deadline
                        )
            end
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


-spec push_events_to_event_sinks(options(), mg:id(), request_context(), deadline(), [mg_events:event()]) ->
    ok.
push_events_to_event_sinks(Options, ID, ReqCtx, Deadline, Events) ->
    Namespace = get_option(namespace, Options),
    EventSinks = maps:get(event_sinks, Options, []),
    lists:foreach(
        fun(EventSinkHandler) ->
            ok = mg_events_sink:add_events(
                EventSinkHandler,
                Namespace,
                ID,
                Events,
                ReqCtx,
                Deadline
            )
        end,
        EventSinks
    ).

-spec state_to_flow_action(state()) ->
    mg_machine:processor_flow_action().
state_to_flow_action(#{timer := undefined}) ->
    sleep;
state_to_flow_action(#{timer := {Timestamp, ReqCtx, HandlingTimeout, _}}) ->
    {wait, Timestamp, ReqCtx, HandlingTimeout}.

-spec apply_delayed_actions_to_state(delayed_actions(), state()) ->
    state() | remove.
apply_delayed_actions_to_state(#{remove := remove}, _) ->
    remove;
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

-spec emit_action_beats(options(), mg:id(), request_context(), delayed_actions()) ->
    ok.
emit_action_beats(Options, ID, ReqCtx, DelayedActions) ->
    ok = emit_timer_action_beats(Options, ID, ReqCtx, DelayedActions),
    ok.

-spec emit_timer_action_beats(options(), mg:id(), request_context(), delayed_actions()) ->
    ok.
emit_timer_action_beats(_Options, _ID, _ReqCtx, #{new_timer := unchanged}) ->
    ok;
emit_timer_action_beats(Options, ID, ReqCtx, #{new_timer := undefined}) ->
    #{namespace := NS, pulse := Pulse} = Options,
    mg_pulse:handle_beat(Pulse, #mg_timer_lifecycle_removed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx
    });
emit_timer_action_beats(Options, ID, ReqCtx, #{new_timer := Timer}) ->
    #{namespace := NS, pulse := Pulse} = Options,
    {Timestamp, _TimerReqCtx, _TimerDeadline, _TimerHistory} = Timer,
    mg_pulse:handle_beat(Pulse, #mg_timer_lifecycle_created{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = Timestamp
    }).

%%

-spec process_signal(options(), request_context(), deadline(), signal(), machine(), mg_events:events_range()) ->
    {ok, delayed_actions()}.
process_signal(#{processor := Processor}, ReqCtx, Deadline, Signal, Machine, EventsRange) ->
    SignalArgs = [ReqCtx, Deadline, {Signal, Machine}],
    {StateChange, ComplexAction} = mg_utils:apply_mod_opts(Processor, process_signal, SignalArgs),
    {ok, handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx)}.

-spec process_call(options(), request_context(), deadline(), term(), machine(), mg_events:events_range()) ->
    {_Resp, delayed_actions()}.
process_call(#{processor := Processor}, ReqCtx, Deadline, Args, Machine, EventsRange) ->
    CallArgs = [ReqCtx, Deadline, {Args, Machine}],
    {Resp, StateChange, ComplexAction} = mg_utils:apply_mod_opts(Processor, process_call, CallArgs),
    {Resp, handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx)}.

% -spec process_repair(options(), request_context(), deadline(), term(), machine(), mg_events:events_range()) ->
%     {_Resp, delayed_actions()}.
% process_repair(#{processor := Processor}, ReqCtx, Deadline, Args, Machine, EventsRange) ->
%     CallArgs = [ReqCtx, Deadline, {Args, Machine}],
%     {Resp, StateChange, ComplexAction} = mg_utils:apply_mod_opts(Processor, process_repair, CallArgs),
%     {Resp, handle_processing_result(StateChange, ComplexAction, EventsRange, ReqCtx)}.

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
        new_timer => get_timer_action(maps:get(timer, ComplexAction, undefined), ReqCtx),
        remove    => maps:get(remove, ComplexAction, undefined)
    }.

-spec get_timer_action(undefined | timer_action(), request_context()) ->
    int_timer() | undefined | unchanged.
get_timer_action(undefined, _) ->
    unchanged;
get_timer_action(unset_timer, _) ->
    undefined;
get_timer_action({set_timer, Timer, undefined, HandlingTimeout}, ReqCtx) ->
    get_timer_action({set_timer, Timer, {undefined, undefined, forward}, HandlingTimeout}, ReqCtx);
get_timer_action({set_timer, Timer, HRange, undefined}, ReqCtx) ->
    get_timer_action({set_timer, Timer, HRange, 30}, ReqCtx);
get_timer_action({set_timer, Timer, HRange, HandlingTimeout}, ReqCtx) ->
    TimerDateTime =
        case Timer of
            {timeout, Timeout} ->
                genlib_time:unow() + Timeout;
            {deadline, Deadline} ->
                genlib_time:daytime_to_unixtime(Deadline)
        end,
    {TimerDateTime, ReqCtx, HandlingTimeout * 1000, HRange}.

%%

-spec processor_options(options()) ->
    mg_utils:mod_opts().
processor_options(Options) ->
    maps:get(processor, Options).

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options = #{machines := MachinesOptions}) ->
    (maps:without([processor], MachinesOptions))#{
        processor => {?MODULE, Options}
    }.

-spec events_storage_options(options()) ->
    mg_storage:options().
events_storage_options(#{namespace := NS, events_storage := StorageOptions}) ->
    {Mod, Options} = mg_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?MODULE, events}}}.

-spec tags_machine_options(options()) ->
    mg_machine_tags:options().
tags_machine_options(#{tagging := Options}) ->
    Options.

-spec get_option(atom(), options()) ->
    _.
get_option(Subj, Options) ->
    maps:get(Subj, Options).

%%

-spec machine(options(), mg:id(), state(), [mg_events:event()], mg_events:history_range()) ->
    machine().
machine(Options = #{namespace := Namespace}, ID, State, ExtraEvents, HRange) ->
    #{events_range := EventsRange, aux_state := AuxState, timer := Timer} = State,
    RangeGetters = [RG ||
        RG = {Range, _Getter} <- [
            {compute_events_range(ExtraEvents) , extra_event_getter(ExtraEvents)},
            {EventsRange                       , storage_event_getter(Options, ID)}
        ],
        Range /= undefined
    ],
    #{
        ns            => Namespace,
        id            => ID,
        history       => get_events(RangeGetters, EventsRange, HRange),
        history_range => HRange,
        aux_state     => AuxState,
        timer         => Timer
    }.

-type event_getter() :: fun((mg_events:id()) -> mg_events:event()).
-type range_getter() :: {mg_events:events_range(), event_getter()}.

-spec get_events([range_getter(), ...], mg_events:events_range(), mg_events:history_range()) ->
    [mg_events:event()].
get_events(RangeGetters, EventsRange, HRange) ->
    EventIDs = mg_events:get_event_ids(EventsRange, HRange),
    genlib_pmap:map(
        fun (EventID) ->
            Getter = find_event_getter(RangeGetters, EventID),
            Getter(EventID)
        end,
        EventIDs
    ).

-spec find_event_getter([range_getter(), ...], mg_events:id()) ->
    event_getter().
find_event_getter([{{First, Last}, Getter} | _], EventID) when First =< EventID, EventID =< Last ->
    Getter;
find_event_getter([_ | [_ | _] = Rest], EventID) ->
    find_event_getter(Rest, EventID);
find_event_getter([{_, Getter}], _) ->
    % сознательно игнорируем последний range
    Getter.

-spec storage_event_getter(options(), mg:id()) ->
    event_getter().
storage_event_getter(Options, ID) ->
    StorageOptions = events_storage_options(Options),
    fun (EventID) ->
        Key = mg_events:add_machine_id(ID, mg_events:event_id_to_key(EventID)),
        {_Context, Value} = mg_storage:get(StorageOptions, Key),
        kv_to_event(ID, {Key, Value})
    end.

-spec extra_event_getter([mg_events:event()]) ->
    event_getter().
extra_event_getter(Events) ->
    fun (EventID) ->
        erlang:hd(lists:dropwhile(
            fun (#{id := ID}) -> ID /= EventID end,
            Events
        ))
    end.

-spec try_apply_delayed_actions(state()) ->
    {state(), [mg_events:event()]} | undefined.
try_apply_delayed_actions(#{delayed_actions := undefined} = State) ->
    {State, []};
try_apply_delayed_actions(#{delayed_actions := DA = #{add_events := NewEvents}} = State) ->
    case apply_delayed_actions_to_state(DA, State) of
        NewState = #{} ->
            {NewState, NewEvents};
        remove ->
            undefined
    end.

-spec add_delayed_actions(delayed_actions(), state()) ->
    state().
add_delayed_actions(DelayedActions, #{delayed_actions := undefined} = State) ->
    State#{delayed_actions => DelayedActions};
add_delayed_actions(NewDelayedActions, #{delayed_actions := OldDelayedActions} = State) ->
    MergedActions = maps:fold(fun add_delayed_action/3, OldDelayedActions, NewDelayedActions),
    State#{delayed_actions => MergedActions}.

-spec add_delayed_action(Field :: atom(), Value :: term(), delayed_actions()) ->
    delayed_actions().
%% Tag
add_delayed_action(add_tag, undefined, DelayedActions) ->
    DelayedActions;
add_delayed_action(add_tag, Tag, DelayedActions) ->
    DelayedActions#{add_tag => Tag};
%% Timer
add_delayed_action(new_timer, unchanged, DelayedActions) ->
    DelayedActions;
add_delayed_action(new_timer, Timer, DelayedActions) ->
    DelayedActions#{new_timer => Timer};
%% Removing
add_delayed_action(remove, undefined, DelayedActions) ->
    DelayedActions;
add_delayed_action(remove, Remove, DelayedActions) ->
    DelayedActions#{remove => Remove};
%% New events
add_delayed_action(add_events, NewEvents, #{add_events := OldEvents} = DelayedActions) ->
    DelayedActions#{add_events => OldEvents ++ NewEvents};
%% AuxState changing
add_delayed_action(new_aux_state, AuxState, DelayedActions) ->
    DelayedActions#{new_aux_state => AuxState};
%% Events range changing
add_delayed_action(new_events_range, Range, DelayedActions) ->
    DelayedActions#{new_events_range => Range}.

-spec compute_events_range([mg_events:event()]) ->
    mg_events:events_range().
compute_events_range([]) ->
    undefined;
compute_events_range([#{id := ID} | _] = Events) ->
    {ID, ID + erlang:length(Events) - 1}.

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(State) ->
    #{events_range := EventsRange, aux_state := AuxState, delayed_actions := DelayedActions, timer := Timer} = State,
    [3,
        mg_events:events_range_to_opaque(EventsRange),
        mg_events:content_to_opaque(AuxState),
        mg_events:maybe_to_opaque(DelayedActions, fun delayed_actions_to_opaque/1),
        mg_events:maybe_to_opaque(Timer, fun int_timer_to_opaque/1)
    ].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
%% при создании есть момент (continuation) когда ещё нет стейта
opaque_to_state(null) ->
    #{
        events_range    => undefined,
        aux_state       => {#{}, <<>>},
        delayed_actions => undefined,
        timer           => undefined
    };
opaque_to_state([1, EventsRange, AuxState, DelayedActions]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => {#{}, AuxState},
        delayed_actions => mg_events:maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => undefined
    };
opaque_to_state([2, EventsRange, AuxState, DelayedActions, Timer]) ->
    State = opaque_to_state([1, EventsRange, AuxState, DelayedActions]),
    State#{
        timer           := mg_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    };
opaque_to_state([3, EventsRange, AuxState, DelayedActions, Timer]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => mg_events:opaque_to_content(AuxState),
        delayed_actions => mg_events:maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => mg_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    }.


-spec events_to_kvs(mg:id(), [mg_events:event()]) ->
    [mg_storage:kv()].
events_to_kvs(MachineID, Events) ->
    mg_events:add_machine_id(MachineID, mg_events:events_to_kvs(Events)).

-spec kv_to_event(mg:id(), mg_storage:kv()) ->
    mg_events:event().
kv_to_event(MachineID, Kv) ->
    mg_events:kv_to_event(mg_events:remove_machine_id(MachineID, Kv)).

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
        new_events_range := EventsRange,
        remove           := Remove
    } = DelayedActions,
    [3,
        mg_events:maybe_to_opaque(Tag   , fun mg_events:identity             /1),
        mg_events:maybe_to_opaque(Timer , fun delayed_timer_actions_to_opaque/1),
        mg_events:maybe_to_opaque(Remove, fun remove_to_opaque               /1),
        mg_events:events_to_opaques(Events),
        mg_events:content_to_opaque(AuxState),
        mg_events:events_range_to_opaque(EventsRange)
    ].

-spec opaque_to_delayed_actions(mg_storage:opaque()) ->
    delayed_actions().
opaque_to_delayed_actions(null) ->
    undefined;
opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]) ->
    #{
        add_tag          => mg_events:maybe_from_opaque(Tag  , fun mg_events:identity             /1),
        new_timer        => mg_events:maybe_from_opaque(Timer, fun opaque_to_delayed_timer_actions/1),
        remove           => undefined,
        add_events       => mg_events:opaques_to_events(Events),
        new_aux_state    => {#{}, AuxState},
        new_events_range => mg_events:opaque_to_events_range(EventsRange)
    };
opaque_to_delayed_actions([2, Tag, Timer, Remove, Events, AuxState, EventsRange]) ->
    DelayedActions = opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]),
    DelayedActions#{
        remove           := mg_events:maybe_from_opaque(Remove, fun opaque_to_remove/1),
        new_aux_state    := {#{}, AuxState}
    };
opaque_to_delayed_actions([3, Tag, Timer, Remove, Events, AuxState, EventsRange]) ->
    DelayedActions = opaque_to_delayed_actions([2, Tag, Timer, Remove, Events, AuxState, EventsRange]),
    DelayedActions#{
        new_aux_state    := mg_events:opaque_to_content(AuxState)
    }.

-spec delayed_timer_actions_to_opaque(genlib_time:ts() | unchanged) ->
    mg_storage:opaque().
delayed_timer_actions_to_opaque(unchanged) ->
    <<"unchanged">>;
delayed_timer_actions_to_opaque(Timer) ->
    int_timer_to_opaque(Timer).

-spec opaque_to_delayed_timer_actions(mg_storage:opaque()) ->
    genlib_time:ts() | undefined | unchanged.
opaque_to_delayed_timer_actions(<<"unchanged">>) ->
    unchanged;
opaque_to_delayed_timer_actions(Timer) ->
    opaque_to_int_timer(Timer).

-spec remove_to_opaque(remove) ->
    mg_storage:opaque().
remove_to_opaque(Value) ->
    enum_to_int(Value, [remove]).

-spec opaque_to_remove(mg_storage:opaque()) ->
    remove.
opaque_to_remove(Value) ->
    int_to_enum(Value, [remove]).

-spec enum_to_int(T, [T]) ->
    pos_integer().
enum_to_int(Value, Enum) ->
    lists_at(Value, Enum).

-spec int_to_enum(pos_integer(), [T]) ->
    T.
int_to_enum(Value, Enum) ->
    lists:nth(Value, Enum).

-spec int_timer_to_opaque(int_timer()) ->
    mg_storage:opaque().
int_timer_to_opaque({Timestamp, ReqCtx, HandlingTimeout, HRange}) ->
    [1, Timestamp, ReqCtx, HandlingTimeout, mg_events:history_range_to_opaque(HRange)].

-spec opaque_to_int_timer(mg_storage:opaque()) ->
    int_timer().
opaque_to_int_timer([1, Timestamp, ReqCtx, HandlingTimeout, HRange]) ->
    {Timestamp, ReqCtx, HandlingTimeout, mg_events:opaque_to_history_range(HRange)}.

%%

-spec lists_at(E, [E]) ->
    pos_integer() | undefined.
lists_at(E, L) ->
    lists_at(E, L, 1).

-spec lists_at(E, [E], pos_integer()) ->
    pos_integer() | undefined.
lists_at(_, [], _) ->
    undefined;
lists_at(E, [H|_], N) when E =:= H ->
    N;
lists_at(E, [_|T], N) ->
    lists_at(E, T, N + 1).
