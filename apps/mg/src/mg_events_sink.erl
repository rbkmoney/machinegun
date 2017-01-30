-module(mg_events_sink).

%% API
-export_type([event_body/0]).
-export_type([options   /0]).
-export([child_spec /2]).
-export([start_link /1]).
-export([add_events /5]).
-export([get_history/3]).
-export([repair     /2]).

%% mg_machine handler
-behaviour(mg_machine).
-export([process_machine/5]).

%%
%% API
%%
-type event_body() :: #{
    source_ns => mg:ns(),
    source_id => mg:id(),
    event     => mg_events:event()
}.
-type event() :: mg_events:event(event_body()).
-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_storage:storage()
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
            mg_machine:child_spec(machine_options       (Options), automaton     ),
            mg_storage:child_spec(events_storage_options(Options), events_storage)
        ]
    ).


-spec add_events(options(), mg:id(), mg:ns(), mg:id(), [mg_events:event()]) ->
    ok.
add_events(Options, EventSinkID, SourceNS, SourceID, Events) ->
    ok = mg_machine:call_with_lazy_start(
            machine_options(Options),
            EventSinkID,
            {add_events, SourceNS, SourceID, Events},
            undefined
        ).

-spec get_history(options(), mg:id(), mg_events:history_range()) ->
    [event()].
get_history(Options, EventSinkID, HistoryRange) ->
    #{events_range := EventsRange} = get_state(Options, EventSinkID),
    EventsKeys = get_events_keys(EventSinkID, EventsRange, HistoryRange),
    kvs_to_sink_events(EventSinkID, [
        {Key, Value} ||
        {Key, {_, Value}} <- [{Key, mg_storage:get(events_storage_options(Options), Key)} || Key <- EventsKeys]
    ]).

-spec repair(options(), mg:id()) ->
    ok.
repair(Options, EventSinkID) ->
    mg_machine:repair(Options, EventSinkID, undefined).

%%
%% mg_processor handler
%%
-type state() :: #{
    events_range => mg_events:events_range()
}.

-spec process_machine(_, mg:id(), mg_machine:processor_impact(), _, mg_machine:machine_state()) ->
    mg_machine:processor_result().
process_machine(Options, EventSinkID, Impact, _, PackedState) ->
    State =
        case {Impact, PackedState} of
            {{init, _}, null} -> new_state();
            {_        , _   } -> opaque_to_state(PackedState)
        end,
    NewState = process_machine_(Options, EventSinkID, Impact, State),
    {{reply, ok}, wait, state_to_opaque(NewState)}.

-spec process_machine_(options(), mg:id(), mg_machine:processor_impact(), state()) ->
    state().
process_machine_(_, _, {init, undefined}, State) ->
    State;
process_machine_(_, _, {repair, undefined}, State) ->
    State;
process_machine_(Options, EventSinkID, {call, {add_events, SourceNS, SourceID, Events}}, State) ->
    {SinkEvents, NewState} = generate_sink_events(SourceNS, SourceID, Events, State),
    ok = store_sink_events(Options, EventSinkID, SinkEvents),
    NewState.

%%

-spec store_sink_events(options(), mg:id(), [event()]) ->
    ok.
store_sink_events(Options, EventSinkID, SinkEvents) ->
    lists:foreach(
        fun({Key, Value}) ->
            _ = mg_storage:put(events_storage_options(Options), Key, undefined, Value, [])
        end,
        sink_events_to_kvs(EventSinkID, SinkEvents)
    ).

-spec get_events_keys(mg:id(), mg_events:events_range(), mg_events:history_range()) ->
    [mg_storage:key()].
get_events_keys(EventSinkID, EventsRange, HistoryRange) ->
    [
        mg_events:add_machine_id(EventSinkID, mg_events:event_id_to_key(EventID))
        ||
        EventID <- mg_events:get_event_ids(EventsRange, HistoryRange)
    ].

-spec get_state(options(), mg:id()) ->
    state().
get_state(Options, EventSinkID) ->
    try
        opaque_to_state(mg_machine:get(machine_options(Options), EventSinkID))
    catch throw:machine_not_found ->
        new_state()
    end.

-spec new_state() ->
    state().
new_state() ->
    #{events_range => undefined}.

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options=#{namespace := Namespace, storage := Storage}) ->
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

-spec generate_sink_events(mg:ns(), mg:id(), [mg_events:event()], state()) ->
    {[event()], state()}.
generate_sink_events(SourceNS, SourceID, Events, State=#{events_range:=EventsRange}) ->
    Bodies = [generate_sink_event_body(SourceNS, SourceID, Event) || Event <- Events],
    {SinkEvents, NewEventsRange} = mg_events:generate_events_with_range(Bodies, EventsRange),
    {SinkEvents, State#{events_range := NewEventsRange}}.

-spec generate_sink_event_body(mg:ns(), mg:id(), mg_events:event()) ->
    event().
generate_sink_event_body(SourceNS, SourceID, Event) ->
    #{
        source_ns => SourceNS,
        source_id => SourceID,
        event     => Event
    }.


%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg:opaque().
state_to_opaque(#{events_range := EventsRange}) ->
    [1, mg_events:events_range_to_opaque(EventsRange)].

-spec opaque_to_state(mg:opaque()) ->
    state().
opaque_to_state([1, EventsRange]) ->
    #{
        events_range => mg_events:opaque_to_events_range(EventsRange)
    }.

-spec sink_event_body_to_opaque(mg_events:event()) ->
    mg_events:body().
sink_event_body_to_opaque(#{source_ns := SourceNS, source_id := SourceID, event := Event}) ->
    [1, SourceNS, SourceID, mg_events:event_to_opaque(Event)].

-spec opaque_to_sink_event_body(mg_events:body()) ->
    mg_events:event().
opaque_to_sink_event_body([1, SourceNS, SourceID, Event]) ->
    #{
        source_ns => SourceNS,
        source_id => SourceID,
        event     => mg_events:opaque_to_event(Event)
    }.

-spec sink_events_to_kvs(mg:id(), [event()]) ->
    [mg_storage:kv()].
sink_events_to_kvs(EventSinkID, Events) ->
    mg_events:add_machine_id(EventSinkID, mg_events:events_to_kvs(Events, fun sink_event_body_to_opaque/1)).

-spec kvs_to_sink_events(mg:id(), [mg_storage:kv()]) ->
    [event()].
kvs_to_sink_events(EventSinkID, Kvs) ->
    mg_events:kvs_to_events(mg_events:remove_machine_id(EventSinkID, Kvs), fun opaque_to_sink_event_body/1).
