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
-type event() :: #{
    source_ns => mg:ns(),
    source_id => mg:id(),
    event     => mg:event()
}.
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
    mg:history().
get_history(Options, EventSinkID, Range) ->
    #{history:=History} =
        mg_machine:get_machine_with_lazy_start(machine_options(Options, EventSinkID), EventSinkID, Range, undefined),
    [Event#{body:=unpack_sink_event(Body)} || Event=#{body:=Body} <- History].

%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    {{null, []}, #{}}.

-spec process_call(_, mg:call_args()) ->
    mg:call_result().
process_call(_, {{handle_events, SourceNS, SourceID, Events}, #{}}) ->
    SinkEvents = generate_sink_events(SourceNS, SourceID, Events),
    {ok, {null, pack_sink_events(SinkEvents)}, #{}}.

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
    [event()].
generate_sink_events(SourceNS, SourceID, Events) ->
    [generate_sink_event(SourceNS, SourceID, Event) || Event <- Events].

-spec generate_sink_event(mg:ns(), mg:id(), mg:event()) ->
    event().
generate_sink_event(SourceNS, SourceID, Event) ->
    #{
        source_ns => SourceNS,
        source_id => SourceID,
        event     => Event
    }.

%%
%% packer to opaque
%%
-spec pack_event(mg:event()) ->
    mg:opaque().
pack_event(#{id := EventID, created_at := Date, body := Body}) ->
    #{
        <<"id">> => EventID,
        <<"ca">> => Date,
        <<"b" >> => Body
    }.
-spec pack_sink_event(event()) ->
    mg:event_body().
pack_sink_event(#{source_ns := SourceNS, source_id := SourceID, event := Event}) ->
    #{
        <<"sn">> => SourceNS,
        <<"si">> => SourceID,
        <<"e" >> => pack_event(Event)
    }.

-spec pack_sink_events([event()]) ->
    [mg:event_body()].
pack_sink_events(Events) ->
    lists:map(fun pack_sink_event/1, Events).



-spec unpack_event(mg:event()) ->
    mg:event().
unpack_event(#{<<"id">> := EventID, <<"ca">> := Date, <<"b" >> := Body}) ->
    #{
        id         => EventID,
        created_at => Date,
        body       => Body
    }.

-spec unpack_sink_event(mg:event_body()) ->
    event().
unpack_sink_event(#{<<"sn">> := SourceNS, <<"si">> := SourceID, <<"e" >> := Event}) ->
    #{
        source_ns => SourceNS,
        source_id => SourceID,
        event     => unpack_event(Event)
    }.
