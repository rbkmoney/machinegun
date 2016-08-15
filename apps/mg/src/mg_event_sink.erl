-module(mg_event_sink).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([options/0]).
-export([child_spec     /2]).
-export([get_history    /2]).
-export([machine_options/1]).


%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/3]).

%% mg_processor handler
-behaviour(mg_observer).
-export([handle_events/3]).


%%
%% API
%%
-define(event_sink_machine_id, <<"event_sink">>).
-type options() :: mg_machine:db_options().

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_machine:child_spec(ChildID, machine_options(Options)).

%% TODO подумать о зацикливании
%% TODO NS
-spec handle_events(options(), mg:id(), [mg:event()]) ->
    ok.
handle_events(Options, SourceID, Events) ->
    try
        {<<"">>, undefined} =
            mg_machine:call(
                machine_options(Options),
                {id, ?event_sink_machine_id},
                {handle_events, SourceID, Events},
                undefined
            ),
        ok
    catch throw:#'MachineNotFound'{} ->
        ok = start(Options),
        handle_events(Options, SourceID, Events)
    end.

-spec get_history(options(), mg:history_range()) ->
    mg:sink_events().
get_history(Options, Range) ->
    try
        [
            SinkEvent
            ||
            #'Event'{event_payload = SinkEvent}
            <-
            mg_machine:get_history(machine_options(Options), {id, ?event_sink_machine_id}, Range)
        ]
    catch throw:#'MachineNotFound'{} ->
        ok = start(Options),
        get_history(Options, Range)
    end.

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(DBMod) ->
    #{
        processor =>?MODULE,
        db        => DBMod
    }.


%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    #'SignalResult'{events = [], action = #'ComplexAction'{}}.

-spec process_call(_, mg:call_args(), mg:call_context()) ->
    mg:call_result().
process_call(_, #'CallArgs'{call = {handle_events, SourceID, Events}}, WoodyContext) ->
    SinkEvents = generate_sink_events(SourceID, Events),
    {#'CallResult'{response = <<"">>, events = SinkEvents, action = #'ComplexAction'{}}, WoodyContext}.

%%
%% local
%%
-spec start(options()) ->
    ok.
start(Options) ->
    mg_machine:start(machine_options(Options), ?event_sink_machine_id, <<"">>).

-spec generate_sink_events(mg:id(), [mg:event()]) ->
    [mg:sink_event()].
generate_sink_events(SourceID, Events) ->
    [generate_sink_event(SourceID, Event) || Event <- Events].

-spec generate_sink_event(mg:id(), mg:event()) ->
    mg:sink_event().
generate_sink_event(SourceID, Event) ->
    #'SinkEvent'{
        source_id = SourceID,
        source_ns = <<"TODO source_ns">>, % TODO
        event     = Event
    }.
