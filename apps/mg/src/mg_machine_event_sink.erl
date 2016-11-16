-module(mg_machine_event_sink).

%% API
-export_type([options/0]).
-export_type([id     /0]).
-export([child_spec /3]).
-export([get_history/3]).

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
    mg_machine:child_spec(machine_options(Options, EventSinkID), ChildID).


-spec handle_events({options(), mg:ns(), id()}, mg:id(), [mg:event()]) ->
    ok.
handle_events({Options, SourceNS, EventSinkID}, SourceID, Events) ->
    % TODO что делать с ошибками тут?
    ok = mg_machine:call_with_lazy_start(
            machine_options(Options, EventSinkID),
            EventSinkID,
            {handle_events, SourceNS, SourceID, Events},
            {undefined, 0, backward},
            undefined
        ).

-spec get_history(options(), id(), mg:history_range()) ->
    mg:sink_history().
get_history(Options, EventSinkID, Range) ->
    #{history:=History} =
        mg_machine:get_machine_with_lazy_start(machine_options(Options, EventSinkID), EventSinkID, Range, undefined),
    History.

%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    {{undefined, []}, #{}}.

-spec process_call(_, mg:call_args()) ->
    mg:call_result().
process_call(_, {{handle_events, SourceNS, SourceID, Events}, #{}}) ->
    SinkEvents = generate_sink_events(SourceNS, SourceID, Events),
    {ok, {undefined, SinkEvents}, #{}}.

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
