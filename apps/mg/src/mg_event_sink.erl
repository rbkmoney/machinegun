-module(mg_event_sink).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([options/0]).
-export([child_spec  /2]).
-export([get_history /2]).


%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/3]).

%% mg_processor handler
-behaviour(mg_observer).
-export([handle_event/3]).


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
-spec handle_event(options(), mg:id(), mg:event()) ->
    ok.
handle_event(Options, ID, Event) ->
    try
        mg_machine:call(machine_options(Options), {id, ?event_sink_machine_id}, {handle_event, ID, Event}, undefined)
    catch throw:#'MachineNotFound'{} ->
        ok = start(Options),
        handle_event(Options, ID, Event)
    end.

-spec get_history(options(), mg:history_range()) ->
    mg:sink_events().
get_history(Options, Range) ->
    try
        mg_machine:get_history(machine_options(Options), {id, ?event_sink_machine_id}, Range)
    catch throw:#'MachineNotFound'{} ->
        ok = start(Options),
        get_history(Options, Range)
    end.


%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, {init, #'InitSignal'{}}) ->
    #'SignalResult'{events = [], action = #'ComplexAction'{}};
process_signal(_, {repair, #'RepairSignal'{}}) ->
    #'SignalResult'{events = [], action = #'ComplexAction'{}}.

-spec process_call(_, mg:call_args(), mg:call_context()) ->
    mg:call_result().
process_call(_, #'CallArgs'{call = _, history = _}, WoodyContext) ->
    {#'CallResult'{response = <<"">>, events = [], action = #'ComplexAction'{}}, WoodyContext}.

%%
%% local
%%
-spec start(options()) ->
    ok.
start(Options) ->
    mg_machine:start(machine_options(Options), ?event_sink_machine_id, <<"">>).

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(DBMod) ->
    #{
        processor =>?MODULE,
        db        => DBMod
    }.
