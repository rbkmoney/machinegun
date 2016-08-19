-module(mg_event_sink).

%% API
-export_type([options/0]).
-export([child_spec     /2]).
-export([get_history    /2]).
-export([machine_options/1]).

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/2]).

%% mg_processor handler
-behaviour(mg_observer).
-export([handle_events/3]).

%%
%% API
%%
-define(event_sink_machine_id, event_sink).

-type options() :: mg_utils:mod_opts().

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_machine:child_spec(ChildID, machine_options(Options)).

%% TODO подумать о зацикливании
-spec handle_events({options(), mg:ns()}, mg:id(), [mg:event()]) ->
    ok.
handle_events({Options, SourceNS}, SourceID, Events) ->
    try
        ok = mg_machine:call(
                machine_options(Options),
                {id, ?event_sink_machine_id},
                {handle_events, SourceNS, SourceID, Events}
            )
    catch throw:machine_not_found ->
        ok = start(Options),
        handle_events({Options, SourceNS}, SourceID, Events)
    end.

-spec get_history(options(), mg:history_range()) ->
    mg:sink_history().
get_history(Options, Range) ->
    try
        mg_machine:get_history(machine_options(Options), {id, ?event_sink_machine_id}, Range)
    catch throw:machine_not_found ->
        ok = start(Options),
        get_history(Options, Range)
    end.

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options) ->
    #{
        namespace => ?MODULE,
        processor => ?MODULE,
        storage   => Options
    }.


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
-spec start(options()) ->
    ok.
start(Options) ->
    mg_machine:start(machine_options(Options), ?event_sink_machine_id, <<"">>).

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
