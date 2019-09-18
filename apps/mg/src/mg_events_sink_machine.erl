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

-module(mg_events_sink_machine).

%% API
-export_type([event_body/0]).
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([ns_options/0]).
-export([child_spec /2]).
-export([start_link /1]).
-export([get_history/3]).
-export([repair     /4]).

%% mg_events_sink handler
-behaviour(mg_events_sink).
-export([add_events/6]).

%% mg_machine handler
-behaviour(mg_machine).
-export([process_machine/7]).

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
    name                       := atom(),
    namespace                  := mg:ns(),
    machine_id                 := mg:id(),
    storage                    := storage_options(),
    pulse                      := mg_pulse:handler(),
    events_storage             := mg_storage:options(),
    default_processing_timeout := timeout()
}.
-type ns_options() :: #{
    namespace                  := mg:ns(),
    storage                    := storage_options(),
    pulse                      := mg_pulse:handler(),
    events_storage             := storage_options(),
    default_processing_timeout := timeout()
}.
-type storage_options() :: mg_utils:mod_opts(map()).  % like mg_storage:options() except `name`

-spec child_spec(ns_options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id      => ChildID,
        start   => {?MODULE, start_link, [Options]},
        restart => permanent,
        type    => supervisor
    }.


-spec start_link(ns_options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        mg_utils:lists_compact([
            mg_machine:child_spec(machine_options       (Options), automaton),
            mg_storage:child_spec(events_storage_options(Options), events_storage)
        ])
    ).


-spec add_events(options(), mg:ns(), mg:id(), [mg_events:event()], ReqCtx, mg_utils:deadline()) ->
    ok
when
    ReqCtx:: mg:request_context()
.
add_events(#{machine_id := EventSinkID} = Options, SourceNS, SourceMachineID, Events, ReqCtx, Deadline) ->
    NSOptions = maps:without([machine_id, name], Options),
    ok = mg_machine:call_with_lazy_start(
            machine_options(NSOptions),
            EventSinkID,
            {add_events, SourceNS, SourceMachineID, Events},
            ReqCtx,
            Deadline,
            undefined
        ).

-spec get_history(ns_options(), mg:id(), mg_events:history_range()) ->
    [event()].
get_history(Options, EventSinkID, HistoryRange) ->
    #{events_range := EventsRange} = get_state(Options, EventSinkID),
    EventsKeys = get_events_keys(EventSinkID, EventsRange, HistoryRange),
    StorageOptions = events_storage_options(Options),
    Kvs = genlib_pmap:map(
        fun(Key) ->
            {_Context, Value} = mg_storage:get(StorageOptions, Key),
            {Key, Value}
        end,
        EventsKeys
    ),
    kvs_to_sink_events(EventSinkID, Kvs).

-spec repair(ns_options(), mg:id(), mg:request_context(), mg_utils:deadline()) ->
    ok.
repair(Options, EventSinkID, ReqCtx, Deadline) ->
    mg_machine:repair(machine_options(Options), EventSinkID, undefined, ReqCtx, Deadline).

%%
%% mg_processor handler
%%
-type state() :: #{
    events_range => mg_events:events_range()
}.

-spec process_machine(Options, EventSinkID, Impact, PCtx, ReqCtx, Deadline, PackedState) -> Result when
    Options :: ns_options(),
    EventSinkID :: mg:id(),
    Impact :: mg_machine:processor_impact(),
    PCtx :: mg_machine:processing_context(),
    ReqCtx :: mg:request_context(),
    Deadline :: mg_utils:deadline(),
    PackedState :: mg_machine:machine_state(),
    Result :: mg_machine:processor_result().
process_machine(Options, EventSinkID, Impact, _PCtx, _ReqCtx, _Deadline, PackedState) ->
    State =
        case {Impact, PackedState} of
            {{init, _}, null} -> new_state();
            {_        , _   } -> opaque_to_state(PackedState)
        end,
    NewState = process_machine_(Options, EventSinkID, Impact, State),
    {{reply, ok}, sleep, state_to_opaque(NewState)}.

-spec process_machine_(ns_options(), mg:id(), mg_machine:processor_impact(), state()) ->
    state().
process_machine_(_, _, {init, undefined}, State) ->
    State;
process_machine_(_, _, {repair, undefined}, State) ->
    State;
process_machine_(Options, EventSinkID, {call, {add_events, SourceNS, SourceMachineID, Events}}, State) ->
    {SinkEvents, NewState} = generate_sink_events(SourceNS, SourceMachineID, Events, State),
    ok = store_sink_events(Options, EventSinkID, SinkEvents),
    NewState.

%%

-spec store_sink_events(ns_options(), mg:id(), [event()]) ->
    ok.
store_sink_events(Options, EventSinkID, SinkEvents) ->
    lists:foreach(
        fun(SinkEvent) ->
            store_event(Options, EventSinkID, SinkEvent)
        end,
        SinkEvents
    ).

-spec store_event(ns_options(), mg:id(), event()) ->
    ok.
store_event(Options, EventSinkID, SinkEvent) ->
    {Key, Value} = sink_event_to_kv(EventSinkID, SinkEvent),
    _ = mg_storage:put(events_storage_options(Options), Key,
            undefined, Value, []),
    ok.

-spec get_events_keys(mg:id(), mg_events:events_range(), mg_events:history_range()) ->
    [mg_storage:key()].
get_events_keys(EventSinkID, EventsRange, HistoryRange) ->
    [
        mg_events:add_machine_id(EventSinkID, mg_events:event_id_to_key(EventID))
        ||
        EventID <- mg_events:get_event_ids(EventsRange, HistoryRange)
    ].

-spec get_state(ns_options(), mg:id()) ->
    state().
get_state(Options, EventSinkID) ->
    try
        opaque_to_state(mg_machine:get(machine_options(Options), EventSinkID))
    catch throw:{logic, machine_not_found} ->
        new_state()
    end.

-spec new_state() ->
    state().
new_state() ->
    #{events_range => undefined}.

-spec machine_options(ns_options()) ->
    mg_machine:options().
machine_options(Options = #{namespace := Namespace, storage := Storage, pulse := Pulse}) ->
    #{
        namespace       => mg_utils:concatenate_namespaces(Namespace, <<"machines">>),
        processor       => {?MODULE, Options},
        storage         => Storage,
        pulse           => Pulse
    }.

-spec events_storage_options(ns_options()) ->
    mg_storage:options().
events_storage_options(#{namespace := NS, events_storage := StorageOptions}) ->
    {Mod, Options} = mg_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?MODULE, events_storage}}}.

%%

-spec generate_sink_events(mg:ns(), mg:id(), [mg_events:event()], state()) ->
    {[event()], state()}.
generate_sink_events(SourceNS, SourceMachineID, Events, State=#{events_range:=EventsRange}) ->
    Bodies = [generate_sink_event_body(SourceNS, SourceMachineID, Event) || Event <- Events],
    {SinkEvents, NewEventsRange} = mg_events:generate_events_with_range(Bodies, EventsRange),
    {SinkEvents, State#{events_range := NewEventsRange}}.

-spec generate_sink_event_body(mg:ns(), mg:id(), mg_events:event()) ->
    event_body().
generate_sink_event_body(SourceNS, SourceMachineID, Event) ->
    #{
        source_ns => SourceNS,
        source_id => SourceMachineID,
        event     => Event
    }.


%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(#{events_range := EventsRange}) ->
    [1, mg_events:events_range_to_opaque(EventsRange)].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
opaque_to_state([1, EventsRange]) ->
    #{
        events_range => mg_events:opaque_to_events_range(EventsRange)
    }.

-spec sink_event_body_to_opaque(Vsn :: integer(), event_body()) ->
    mg_storage:opaque().
sink_event_body_to_opaque(_Vsn, #{source_ns := SourceNS, source_id := SourceMachineID, event := Event}) ->
    [1, SourceNS, SourceMachineID, mg_events:event_to_opaque(Event)].

-spec opaque_to_sink_event_body(Vsn :: integer(), mg_storage:opaque()) ->
    event_body().
opaque_to_sink_event_body(_Vsn, [1, SourceNS, SourceMachineID, Event]) ->
    #{
        source_ns => SourceNS,
        source_id => SourceMachineID,
        event     => mg_events:opaque_to_event(Event)
    }.

-spec sink_event_to_kv(mg:id(), event()) ->
    mg_storage:kv().
sink_event_to_kv(EventSinkID, Event) ->
    mg_events:add_machine_id(EventSinkID, mg_events:event_to_kv(Event, fun sink_event_body_to_opaque/2)).

-spec kvs_to_sink_events(mg:id(), [mg_storage:kv()]) ->
    [event()].
kvs_to_sink_events(EventSinkID, Kvs) ->
    mg_events:kvs_to_events(mg_events:remove_machine_id(EventSinkID, Kvs), fun opaque_to_sink_event_body/2).
