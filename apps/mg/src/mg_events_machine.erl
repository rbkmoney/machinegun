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
-export_type([ref             /0]).
-export_type([machine         /0]).
-export_type([tag_action      /0]).
-export_type([timer_action    /0]).
-export_type([complex_action  /0]).
-export_type([state_change    /0]).
-export_type([signal          /0]).
-export_type([signal_args     /0]).
-export_type([call_args       /0]).
-export_type([signal_result   /0]).
-export_type([call_result     /0]).
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
-type deadline    () :: mg_utils:deadline().

-type ref() :: {id, mg:id()} | {tag, mg_machine_tags:tag()}.
-type options() :: #{
    namespace                  => mg:ns(),
    events_storage             => mg_storage:options(),
    processor                  => mg_utils:mod_opts(),
    tagging                    => mg_machine_tags:options(),
    machines                   => mg_machine:options(),
    pulse                      => mg_pulse:handler(),
    event_sink                 => {mg:id(), mg_events_sink:options()},
    default_processing_timeout => timeout()
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
    ESRegName = events_storage_reg_name(Options),
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        mg_utils:lists_compact([
            mg_machine     :child_spec(machine_options       (Options), automaton),
            mg_machine_tags:child_spec(tags_machine_options  (Options), tags     ),
            mg_storage     :child_spec(events_storage_options(Options), events_storage, ESRegName)
        ])
    ).

-define(default_deadline, mg_utils:timeout_to_deadline(5000)).

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
    ok.
repair(Options, Ref, Args, HRange, ReqCtx, Deadline) ->
    ok = mg_machine:repair(
            machine_options(Options),
            ref2id(Options, Ref),
            {Args, HRange},
            ReqCtx,
            Deadline
        ).

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
    State = opaque_to_state(mg_machine:get(machine_options(Options), ID)),
    machine(Options, ID, State, HRange).

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
            throw:{transient, Reason} ->
                erlang:raise(throw, {transient, Reason}, erlang:get_stacktrace());
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
process_machine_(Options, ID, {Subj, {Args, HRange}}, _, ReqCtx, Deadline, State = #{events_range := EventsRange}) ->
    % обработка стандартных запросов
    Machine = machine(Options, ID, State, HRange),
    {Reply, DelayedActions} =
        case Subj of
            init    -> process_signal(Options, ReqCtx, Deadline, {init  , Args}, Machine, EventsRange);
            repair  -> process_signal(Options, ReqCtx, Deadline, {repair, Args}, Machine, EventsRange);
            timeout -> process_signal(Options, ReqCtx, Deadline,  timeout      , Machine, EventsRange);
            call    -> process_call  (Options, ReqCtx, Deadline,          Args , Machine, EventsRange)
        end,
    {noreply, {continue, Reply}, State#{delayed_actions := DelayedActions}};
process_machine_(Options, ID, continuation, PCtx, ReqCtx, Deadline, State = #{delayed_actions := DelayedActions}) ->
    % отложенные действия (эвент синк, тэг)
    %
    % надо понимать, что:
    %  - эвенты сохраняются в сторадж
    %  - создатся тэг
    %  - эвенты добавляются в event sink
    %  - обновится event_range
    %  - отсылается ответ
    %  - если есть удаление, то удаляется
    % надо быть аккуратнее, мест чтобы накосячить тут вагон и маленькая тележка  :-\
    %
    % действия должны обязательно произойти в конце концов (таймаута нет), либо машина должна упасть
    #{add_tag := Tag, add_events := Events} = DelayedActions,
    ok =                   add_tag(Options, ID, ReqCtx, Deadline, Tag   ),
    ok =              store_events(Options, ID, ReqCtx, Events),
    ok = push_events_to_event_sink(Options, ID, ReqCtx, Deadline, Events),

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
            _ = mg_storage:put(events_storage_options(Options), events_storage_ref(Options), Key, undefined, Value, [])
        end,
        events_to_kvs(ID, Events)
    ).


-spec push_events_to_event_sink(options(), mg:id(), request_context(), deadline(), [mg_events:event()]) ->
    ok.
push_events_to_event_sink(Options, ID, ReqCtx, Deadline, Events) ->
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
                    Deadline
                );
        undefined ->
            ok
    end.

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
events_storage_options(#{events_storage := EventsStorage}) ->
    EventsStorage.

-spec events_storage_ref(options()) ->
    mg_utils:gen_ref().
events_storage_ref(Options) ->
    {via, gproc, gproc_key(events, Options)}.

-spec events_storage_reg_name(options()) ->
    mg_utils:gen_reg_name().
events_storage_reg_name(Options) ->
    {via, gproc, gproc_key(events, Options)}.

-spec gproc_key(atom(), options()) ->
    gproc:key().
gproc_key(Type, #{namespace := Namespace}) ->
    {n, l, {?MODULE, Type, Namespace}}.

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
        {Key, {_, Value}} <- [
            {Key, mg_storage:get(events_storage_options(Options), events_storage_ref(Options), Key)} ||
            Key <- EventsKeys
        ]
    ]).

-spec get_events_keys(mg:id(), mg_events:events_range(), mg_events:history_range()) ->
    [mg_storage:key()].
get_events_keys(ID, EventsRange, HRange) ->
    [
        mg_events:add_machine_id(ID, mg_events:event_id_to_key(EventID))
        ||
        EventID <- mg_events:get_event_ids(EventsRange, HRange)
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
        mg_events:maybe_to_opaque(DelayedActions, fun delayed_actions_to_opaque/1),
        mg_events:maybe_to_opaque(Timer, fun int_timer_to_opaque/1)
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
        delayed_actions => mg_events:maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => undefined
    };
opaque_to_state([2, EventsRange, AuxState, DelayedActions, Timer]) ->
    #{
        events_range    => mg_events:opaque_to_events_range(EventsRange),
        aux_state       => AuxState,
        delayed_actions => mg_events:maybe_from_opaque(DelayedActions, fun opaque_to_delayed_actions/1),
        timer           => mg_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
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
        new_events_range := EventsRange,
        remove           := Remove
    } = DelayedActions,
    [2,
        mg_events:maybe_to_opaque(Tag   , fun mg_events:identity             /1),
        mg_events:maybe_to_opaque(Timer , fun delayed_timer_actions_to_opaque/1),
        mg_events:maybe_to_opaque(Remove, fun remove_to_opaque               /1),
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
        add_tag          => mg_events:maybe_from_opaque(Tag  , fun mg_events:identity             /1),
        new_timer        => mg_events:maybe_from_opaque(Timer, fun opaque_to_delayed_timer_actions/1),
        remove           => undefined,
        add_events       => mg_events:opaques_to_events(Events),
        new_aux_state    => AuxState,
        new_events_range => mg_events:opaque_to_events_range(EventsRange)
    };
opaque_to_delayed_actions([2, Tag, Timer, Remove, Events, AuxState, EventsRange]) ->
    (opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]))#{
        remove := mg_events:maybe_from_opaque(Remove, fun opaque_to_remove/1)
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
