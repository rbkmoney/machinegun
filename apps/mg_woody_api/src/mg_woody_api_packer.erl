-module(mg_woody_api_packer).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export([pack  /2]).
-export([unpack/2]).

%%
%% API
%%
-spec pack
    %% system
    % (_, undefined) ->
    %     undefined;
    (binary, binary()) ->
        binary();
    (integer, integer()) ->
        integer();
    (timestamp, calendar:datetime()) ->
        mg_proto_base_thrift:'Timestamp'();
    ({list, _}, _) ->
        _;

    %% mg base
    (ns, mg_woody_api:ns()) ->
        mg_proto_base_thrift:'Namespace'();
    (id, mg_woody_api:id()) ->
        mg_proto_base_thrift:'ID'();
    (tag, mg_woody_api:tag()) ->
        mg_proto_base_thrift:'Tag'();
    (args, mg_woody_api:args()) ->
        mg_proto_state_processing_thrift:'Args'();
    (timeout, mg:timeout_()) ->
        mg_proto_base_thrift:'Timeout'();
    (timer, mg:timer()) ->
        mg_proto_base_thrift:'Timer'();
    (ref, mg:ref()) ->
        mg_proto_state_processing_thrift:'Reference'();
    (direction, mg:direction()) ->
        mg_proto_state_processing_thrift:'Direction'();

    %% events and history
    (event_id, mg_woody_api:event_id()) ->
        mg_proto_base_thrift:'EventID'();
    (event_body, mg_woody_api:event_body()) ->
        mg_proto_state_processing_thrift:'EventBody'();
    (event, mg:event()) ->
        mg_proto_state_processing_thrift:'Event'();
    (history, mg:history()) ->
        mg_proto_state_processing_thrift:'History'();

    %% actions
    (tag_action, mg:tag_action()) ->
        mg_proto_state_processing_thrift:'TagAction'();
    (set_timer_action, mg:set_timer_action()) ->
        mg_proto_state_processing_thrift:'SetTimerAction'();
    (complex_action, mg:complex_action()) ->
        mg_proto_state_processing_thrift:'ComplexAction'();

    %% calls, signals, get_gistory
    (signal, mg:signal()) ->
        mg_proto_state_processing_thrift:'Signal'();
    (call_response, mg:call_response()) ->
        mg_proto_state_processing_thrift:'CallResponse'();
    (signal_args, mg:signal_args()) ->
        mg_proto_state_processing_thrift:'SignalArgs'();
    (call_args, mg:call_args()) ->
        mg_proto_state_processing_thrift:'CallArgs'();
    (call_result, mg:call_result()) ->
        mg_proto_state_processing_thrift:'CallResult'();
    (history_range, mg:history_range()) ->
        mg_proto_state_processing_thrift:'HistoryRange'();

    %% event sink
    (sink_event, mg:sink_event()) ->
        mg_proto_state_processing_thrift:'SinkEvent'();
    (sink_history, mg:sink_history()) ->
        mg_proto_state_processing_thrift:'SinkHistory'().

    % (_, _) ->
    %     no_return().

%% system
pack(_, undefined) ->
    undefined;
pack(binary, Binary) when is_binary (Binary) ->
    Binary;
pack(integer, Integer) when is_integer(Integer) ->
    Integer; % TODO check size
pack(timestamp, Timestamp) ->
    format_timestamp(Timestamp);
pack({list, T}, Values) ->
    [pack(T, Value) || Value <- Values];

%% mg base
pack(ns , NS) ->
    pack(binary, NS);
pack(id , ID) ->
    pack(binary, ID);
pack(tag, Tag) ->
    pack(binary, Tag);
pack(args, Args) ->
    pack(binary, Args);
pack(timeout, Timeout) ->
    pack(integer, Timeout);
pack(timer, {deadline, Deadline}) ->
    {deadline, pack(timestamp, Deadline)};
pack(timer, {timeout, Timeout}) ->
    {timeout, pack(timeout, Timeout)};
pack(ref, {id , ID}) ->
    {id , pack(id, ID)};
pack(ref, {tag, Tag}) ->
    {tag, pack(tag, Tag)};
pack(direction, Direction) ->
    Direction;

%% events and history
pack(event_id, ID) ->
    pack(integer, ID);
pack(event_body, Body) ->
    pack(binary, Body);
pack(event, #{id := ID, created_at := CreatedAt, body := Body}) ->
    #'Event'{
        id            = pack(event_id  , ID       ),
        created_at    = pack(timestamp , CreatedAt),
        event_payload = pack(event_body, Body     )
    };
pack(history, History) ->
    pack({list, event}, History);

%% actions
pack(complex_action, #{timer := SetTimerAction, tag := TagAction}) ->
    #'ComplexAction'{
        set_timer = pack(set_timer_action, SetTimerAction),
        tag       = pack(tag_action      , TagAction     )
    };
pack(set_timer_action, Timer) ->
    #'SetTimerAction'{timer = pack(timer, Timer)};
pack(tag_action, Tag) ->
    #'TagAction'{tag = pack(tag, Tag)};

%% calls, signals, get_gistory
pack(signal, timeout) ->
    {timeout, #'TimeoutSignal'{}};
pack(signal, {init, ID, Args}) ->
    {init,
        #'InitSignal'{
            id  = pack(id  , ID  ),
            arg = pack(args, Args)
        }
    };
pack(signal, {repair, Args}) ->
    {repair,
        #'RepairSignal'{
            arg = pack(args, Args)
        }
    };
pack(call_response, CallResponse) ->
    pack(binary, CallResponse);
pack(signal_args, {Signal, History}) ->
    #'SignalArgs'{
        signal  = pack(signal , Signal ),
        history = pack(history, History)
    };
pack(call_args, {Args, History}) ->
    #'CallArgs'{
        arg     = pack(args   , Args   ),
        history = pack(history, History)
    };
pack(signal_result, {EventBodies, ComplexAction}) ->
    #'SignalResult'{
        events = pack({list, event_body}, EventBodies  ),
        action = pack(complex_action    , ComplexAction)
    };

pack(call_result, {Response, EventBodies, ComplexAction}) ->
    #'CallResult'{
        response = pack(call_response     , Response     ),
        events   = pack({list, event_body}, EventBodies  ),
        action   = pack(complex_action    , ComplexAction)
    };

pack(history_range, {After, Limit, Direction}) ->
    #'HistoryRange'{
        'after'    = pack(event_id , After    ),
         limit     = pack(integer  , Limit    ),
         direction = pack(direction, Direction)
    };

pack(sink_event, #{id := ID, body := #{ source_ns := SourceNS, source_id := SourceID, event := Event}}) ->
    #'SinkEvent'{
        id        = pack(event_id, ID      ),
        source_id = pack(id      , SourceID),
        source_ns = pack(ns      , SourceNS),
        event     = pack(event   , Event   )
    };

pack(sink_history, SinkHistory) ->
    pack({list, sink_event}, SinkHistory);

pack(Type, Value) ->
    erlang:error(badarg, [Type, Value]).

%%

-spec unpack
    %% system
    % (_, undefined) ->
    %     undefined;
    (binary, binary()) ->
        binary();
    (integer, integer()) ->
        integer();
    (timestamp, mg_proto_base_thrift:'Timestamp'()) ->
        % TODO какой-то пипец с диалайзером, если честно поставить тип, то всё оказывается плохо
        % calendar:datetime();
        binary();
    ({list, _}, _) ->
        _;

    %% mg base
    (ns, mg_proto_base_thrift:'Namespace'()) ->
        mg_woody_api:ns();
    (id, mg_proto_base_thrift:'ID'()) ->
        mg_woody_api:id();
    (tag, mg_proto_base_thrift:'Tag'()) ->
        mg_woody_api:tag();
    (args, mg_proto_state_processing_thrift:'Args'()) ->
        mg_woody_api:args();
    (timeout, mg_proto_base_thrift:'Timeout'()) ->
        mg:timeout_();
    (timer, mg_proto_base_thrift:'Timer'()) ->
        mg:timer();
    (ref, mg_proto_state_processing_thrift:'Reference'()) ->
        mg:ref();
    (direction, mg_proto_state_processing_thrift:'Direction'()) ->
        mg:direction();

    %% events and history
    (event_id, mg_proto_base_thrift:'EventID'()) ->
        mg_woody_api:event_id();
    (event_body, mg_proto_state_processing_thrift:'EventBody'()) ->
        mg_woody_api:event_body();
    (event, mg_proto_state_processing_thrift:'Event'()) ->
        mg:event();
    (history, mg_proto_state_processing_thrift:'History'()) ->
        mg:history();

    %% actions
    (tag_action, mg_proto_state_processing_thrift:'TagAction'()) ->
        mg:tag_action();
    (set_timer_action, mg_proto_state_processing_thrift:'SetTimerAction'()) ->
        mg:set_timer_action();
    (complex_action, mg_proto_state_processing_thrift:'ComplexAction'()) ->
        mg:complex_action();

    %% calls, signals, get_gistory
    (signal, mg_proto_state_processing_thrift:'Signal'()) ->
        mg:signal();
    (call_response, mg_proto_state_processing_thrift:'CallResponse'()) ->
        mg:call_response();
    (signal_args, mg_proto_state_processing_thrift:'SignalArgs'()) ->
        mg:signal_args();
    (call_args, mg_proto_state_processing_thrift:'CallArgs'()) ->
        mg:call_args();
    (signal_result, mg_proto_state_processing_thrift:'SignalResult'()) ->
        mg:signal_result();
    (call_result, mg_proto_state_processing_thrift:'CallResult'()) ->
        mg:call_result();
    (history_range, mg_proto_state_processing_thrift:'HistoryRange'()) ->
        mg:history_range();

    %% event sink
    (sink_event, mg_proto_state_processing_thrift:'SinkEvent'()) ->
        mg:sink_event();
    (sink_history, mg_proto_state_processing_thrift:'SinkHistory'()) ->
        mg:sink_history().

    % (_, _) ->
    %     no_return().

%% system
unpack(_, undefined) ->
    undefined;
unpack(binary, Binary) when is_binary (Binary) ->
    Binary;
unpack(integer, Integer) when is_integer(Integer) ->
    Integer; % TODO check size
unpack(timestamp, Timestamp) ->
    parse_timestamp(Timestamp);
unpack({list, T}, Values) ->
    [unpack(T, Value) || Value <- Values];

%% mg base
unpack(ns , NS) ->
    unpack(binary, NS);
unpack(id , ID) ->
    unpack(binary, ID);
unpack(tag, Tag) ->
    unpack(binary, Tag);
unpack(args, Args) ->
    unpack(binary, Args);
unpack(timeout, Timeout) ->
    unpack(integer, Timeout);
unpack(timer, {deadline, Deadline}) ->
    {deadline, unpack(timestamp, Deadline)};
unpack(timer, {timeout, Timeout}) ->
    {timeout, unpack(timeout, Timeout)};
unpack(ref, {id , ID}) ->
    {id , unpack(id, ID)};
unpack(ref, {tag, Tag}) ->
    {tag, unpack(tag, Tag)};
unpack(direction, Direction) ->
    Direction;

%% events and history
unpack(event_id, ID) ->
    unpack(integer, ID);
unpack(event_body, Body) ->
    unpack(binary, Body);
unpack(event, #'Event'{id = ID, created_at = CreatedAt, event_payload = Body}) ->
    #{
        id         => unpack(event_id  , ID       ),
        created_at => unpack(timestamp , CreatedAt),
        body       => unpack(event_body, Body     )
    };
unpack(history, History) ->
    unpack({list, event}, History);

%% actions
unpack(complex_action, #'ComplexAction'{set_timer = SetTimerAction, tag = TagAction}) ->
    #{
        timer => unpack(set_timer_action, SetTimerAction),
        tag   => unpack(tag_action      , TagAction     )
    };
unpack(set_timer_action, #'SetTimerAction'{timer = Timer}) ->
    unpack(timer, Timer);
unpack(tag_action, #'TagAction'{tag = Tag}) ->
    unpack(tag, Tag);

%% calls, signals, get_gistory
unpack(signal, {timeout, #'TimeoutSignal'{}}) ->
    timeout;
unpack(signal, {init, #'InitSignal'{id = ID, arg = Args}}) ->
    {init, unpack(id, ID), unpack(args, Args)};
unpack(signal, {repair, #'RepairSignal'{arg = Args}}) ->
    {repair, unpack(args, Args)};
unpack(call_response, CallResponse) ->
    unpack(binary, CallResponse);
unpack(signal_args, #'SignalArgs'{signal = Signal, history = History}) ->
    {unpack(signal , Signal ), unpack(history, History)};
unpack(call_args, #'CallArgs'{arg = Args, history = History}) ->
    {unpack(args, Args), unpack(history, History)};
unpack(signal_result, #'SignalResult'{events = EventBodies, action = ComplexAction}) ->
    {unpack({list, event_body}, EventBodies), unpack(complex_action, ComplexAction)};

unpack(call_result, #'CallResult'{response = Response, events = EventBodies, action = ComplexAction}) ->
    {unpack(call_response, Response), unpack({list, event_body}, EventBodies), unpack(complex_action, ComplexAction)};

unpack(history_range, #'HistoryRange'{'after' = After, limit = Limit, direction = Direction}) ->
    {unpack(event_id, After), unpack(integer , Limit), unpack(direction, Direction)};

unpack(sink_event, #'SinkEvent'{id = ID, source_ns = SourceNS, source_id = SourceID, event = Event}) ->
    #{
        id   => pack(id, ID),
        body =>
            #{
                source_ns => unpack(ns   , SourceNS),
                source_id => unpack(id   , SourceID),
                event     => unpack(event, Event   )
            }
    };

unpack(sink_history, SinkHistory) ->
    unpack({list, sink_event}, SinkHistory);

unpack(Type, Value) ->
    erlang:error(badarg, [Type, Value]).

%%

% rfc3339:parse имеет некорретный спек, поэтому диалайзер всегда ругается
-dialyzer({nowarn_function, parse_timestamp/1}).
-spec parse_timestamp(binary()) ->
    calendar:datetime().
parse_timestamp(Timestamp) ->
    {ok, {Date, Time, _, undefined}} = rfc3339:parse(Timestamp),
    {Date, Time}.

-spec format_timestamp(calendar:datetime()) ->
    binary().
format_timestamp(Timestamp) ->
    {ok, TimestampBin} = rfc3339:format(Timestamp),
    TimestampBin.
