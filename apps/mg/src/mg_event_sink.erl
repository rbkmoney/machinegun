-module(mg_event_sink).

%% API
-export_type([options/0]).
-export_type([id     /0]).
-export([child_spec     /3]).
-export([get_history    /3]).

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/2]).

%% mg_observer handler
-behaviour(mg_observer).
-export([handle_events/3]).

%%
%% API
%%
-type id() :: mg:id().
-type options() :: #{
    storage => mg_storage:storage()
}.

-spec child_spec(options(), id(), atom()) ->
    supervisor:child_spec().
child_spec(Options, EventSinkID, ChildID) ->
    mg_machine:child_spec(ChildID, machine_options(Options, EventSinkID)).

%% TODO подумать о зацикливании
-spec handle_events({options(), mg:ns(), id()}, mg:id(), [mg:event()]) ->
    ok.
handle_events({Options, SourceNS, EventSinkID}, SourceID, Events) ->
    _ = check_namespace_started(Options, EventSinkID),
    try
        ok = mg_machine:call(
                machine_options(Options, EventSinkID),
                {id, EventSinkID},
                {handle_events, SourceNS, SourceID, Events}
            )
    catch throw:machine_not_found ->
        ok = start(Options, EventSinkID),
        handle_events({Options, SourceNS, EventSinkID}, SourceID, Events)
    end.

-spec get_history(options(), id(), mg:history_range()) ->
    mg:sink_history().
get_history(Options, EventSinkID, Range) ->
    _ = check_namespace_started(Options, EventSinkID),
    try
        mg_machine:get_history(machine_options(Options, EventSinkID), {id, EventSinkID}, Range)
    catch throw:machine_not_found ->
        ok = start(Options, EventSinkID),
        get_history(Options, EventSinkID, Range)
    end.

%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    {[], #{timer => undefined, tag => undefined}}.

-spec process_call(_, mg:call_args()) ->
    mg:call_result().
process_call(_, {{handle_events, SourceNS, SourceID, Events}, _}) ->
    SinkEvents = generate_sink_events(SourceNS, SourceID, Events),
    {ok, SinkEvents, #{timer => undefined, tag => undefined}}.

%%
%% local
%%
-spec machine_options(options(), id()) ->
    mg_machine:options().
machine_options(#{storage:=Storage}, EventSinkID) ->
    #{
        namespace => EventSinkID,
        processor => ?MODULE,
        storage   => Storage
    }.

-spec check_namespace_started(options(), id()) ->
    _ | no_return().
check_namespace_started(Options, EventSinkID) ->
    mg_machine:is_started(machine_options(Options, EventSinkID)) orelse throw(event_sink_not_found).

-spec start(options(), id()) ->
    ok.
start(Options, EventSinkID) ->
    mg_machine:start(machine_options(Options, EventSinkID), EventSinkID, <<"">>).

-spec generate_sink_events(mg:ns(), mg:id(), [mg:event()]) ->
    [mg:sink_event()].
generate_sink_events(SourceNS, SourceID, Events) ->
    [generate_sink_event(SourceNS, SourceID, Event) || Event <- Events].

-spec generate_sink_event(mg:ns(), mg:id(), mg:event()) ->
    mg:sink_event().
generate_sink_event(SourceNS, SourceID, Event) ->
    #{
        source_ns => SourceNS,
        source_id => SourceID,
        event     => Event
    }.
