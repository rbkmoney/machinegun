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

-module(mg_woody_api_packer).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_msgpack_thrift.hrl").

%% API
-export([pack  /2]).
-export([unpack/2]).

%%
%% API
%%
-spec pack(_, _) ->
    _.

%% system
pack(_, undefined) ->
    undefined;
pack(binary, Binary) when is_binary (Binary) ->
    Binary;
pack(opaque, Opaque) ->
    pack_opaque(Opaque);
pack(integer, Integer) when is_integer(Integer) ->
    Integer; % TODO check size
pack(timestamp, Timestamp) ->
    pack(datetime, genlib_time:unixtime_to_daytime(Timestamp));
pack(datetime, Datetime) ->
    format_datetime(Datetime);
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
    pack(opaque, Args);
pack(timeout, Timeout) ->
    pack(integer, Timeout);
pack(timer, {deadline, Deadline}) ->
    {deadline, pack(datetime, Deadline)};
pack(timer, {timeout, Timeout}) ->
    {timeout, pack(timeout, Timeout)};
pack(ref, {id , ID}) ->
    {id , pack(id, ID)};
pack(ref, {tag, Tag}) ->
    {tag, pack(tag, Tag)};
pack(direction, Direction) ->
    Direction;

%% events and history
pack(aux_state, AuxState) ->
    pack(opaque, AuxState);
pack(event_id, ID) ->
    pack(integer, ID);
pack(event_body, Body) ->
    pack(opaque, Body);
pack(event, #{id := ID, created_at := CreatedAt, body := Body}) ->
    #'Event'{
        id            = pack(event_id  , ID       ),
        created_at    = pack(datetime  , CreatedAt),
        event_payload = pack(event_body, Body     )
    };
pack(history, History) ->
    pack({list, event}, History);
pack(machine, Machine) ->
    #{ns:=NS, id:=ID, history_range:=HRange, history:=History, aux_state:=AuxState, timer:=Timer} = Machine,
    #'Machine'{
        ns            = pack(ns           , NS      ),
        id            = pack(id           , ID      ),
        history       = pack(history      , History ),
        history_range = pack(history_range, HRange  ),
        aux_state     = pack(aux_state    , AuxState),
        timer         = pack(int_timer     , Timer   )
    };
pack(int_timer, {Timestamp, _, _, _}) ->
    % TODO сделать нормально
    pack(timestamp, Timestamp);


%% actions
pack(complex_action, ComplexAction) ->
    #'ComplexAction'{
        timer  = pack(timer_action , maps:get(timer , ComplexAction, undefined)),
        tag    = pack(tag_action   , maps:get(tag   , ComplexAction, undefined)),
        remove = pack(remove_action, maps:get(remove, ComplexAction, undefined))
    };
pack(timer_action, {set_timer, Timer, HRange, HandlingTimeout}) ->
    {set_timer,
        #'SetTimerAction'{
            timer   = pack(timer        , Timer          ),
            range   = pack(history_range, HRange         ),
            timeout = pack(integer      , HandlingTimeout)
        }
    };
pack(timer_action, unset_timer) ->
    {unset_timer, #'UnsetTimerAction'{}};
pack(tag_action, Tag) ->
    #'TagAction'{tag = pack(tag, Tag)};
pack(remove_action, remove) ->
    #'RemoveAction'{};

%% calls, signals, get_gistory
pack(state_change, {AuxState, EventBodies}) ->
    #'MachineStateChange'{
        aux_state = pack( aux_state        , AuxState   ),
        events    = pack({list, event_body}, EventBodies)
    };
pack(signal, timeout) ->
    {timeout, #'TimeoutSignal'{}};
pack(signal, {init, Args}) ->
    {init,
        #'InitSignal'{ arg = pack(args, Args) }
    };
pack(signal, {repair, Args}) ->
    {repair,
        #'RepairSignal'{
            arg = pack(args, Args)
        }
    };
pack(call_response, CallResponse) ->
    pack(opaque, CallResponse);
pack(signal_args, {Signal, Machine}) ->
    #'SignalArgs'{
        signal  = pack(signal , Signal ),
        machine = pack(machine, Machine)
    };
pack(call_args, {Args, Machine}) ->
    #'CallArgs'{
        arg     = pack(args   , Args   ),
        machine = pack(machine, Machine)
    };
pack(signal_result, {StateChange, ComplexAction}) ->
    #'SignalResult'{
        change = pack(state_change  , StateChange  ),
        action = pack(complex_action, ComplexAction)
    };

pack(call_result, {Response, StateChange, ComplexAction}) ->
    #'CallResult'{
        response = pack(call_response , Response     ),
        change   = pack(state_change  , StateChange  ),
        action   = pack(complex_action, ComplexAction)
    };

pack(history_range, {After, Limit, Direction}) ->
    #'HistoryRange'{
        'after'    = pack(event_id , After    ),
         limit     = pack(integer  , Limit    ),
         direction = pack(direction, Direction)
    };

pack(machine_descriptor, {NS, Ref, Range}) ->
    #'MachineDescriptor'{
        ns    = pack(ns           , NS   ),
        ref   = pack(ref          , Ref  ),
        range = pack(history_range, Range)
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

-spec unpack(_, _) ->
    _.
%% system
unpack(_, undefined) ->
    undefined;
unpack(binary, Binary) when is_binary (Binary) ->
    Binary;
unpack(opaque, Opaque) ->
    unpack_opaque(Opaque);
unpack(integer, Integer) when is_integer(Integer) ->
    Integer; % TODO check size
unpack(timestamp, Timestamp) ->
    genlib_time:daytime_to_unixtime(unpack(datetime, Timestamp));
unpack(datetime, Datetime) ->
    parse_datetime(Datetime);
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
    unpack(opaque, Args);
unpack(timeout, Timeout) ->
    unpack(integer, Timeout);
unpack(timer, {deadline, Deadline}) ->
    {deadline, unpack(datetime, Deadline)};
unpack(timer, {timeout, Timeout}) ->
    {timeout, unpack(timeout, Timeout)};
unpack(ref, {id , ID}) ->
    {id , unpack(id, ID)};
unpack(ref, {tag, Tag}) ->
    {tag, unpack(tag, Tag)};
unpack(direction, Direction) ->
    Direction;

%% events and history
unpack(aux_state, AuxState) ->
    unpack(opaque, AuxState);
unpack(event_id, ID) ->
    unpack(integer, ID);
unpack(event_body, Body) ->
    unpack(opaque, Body);
unpack(event, #'Event'{id = ID, created_at = CreatedAt, event_payload = Body}) ->
    #{
        id         => unpack(event_id  , ID       ),
        created_at => unpack(datetime  , CreatedAt),
        body       => unpack(event_body, Body     )
    };
unpack(history, History) ->
    unpack({list, event}, History);
unpack(machine, Machine=#'Machine'{}) ->
    #'Machine'{ns=NS, id=ID, history_range=HRange, history=History, aux_state=AuxState, timer=Timer} = Machine,
    #{
        ns            => unpack(ns           , NS          ),
        id            => unpack(id           , ID          ),
        history_range => unpack(history_range, HRange      ),
        history       => unpack(history      , History     ),
        aux_state     => unpack(aux_state    , AuxState    ),
        timer         => unpack(int_timer    , Timer       )
    };
unpack(int_timer, Timestamp) ->
    % TODO сделать нормально
    {unpack(timestamp, Timestamp), undefined, undefined, undefined};

%% actions
unpack(complex_action, #'ComplexAction'{timer = TimerAction, tag = TagAction, remove = RemoveAction}) ->
    #{
        timer  => unpack(timer_action , TimerAction ),
        tag    => unpack(tag_action   , TagAction   ),
        remove => unpack(remove_action, RemoveAction)
    };
unpack(timer_action, {set_timer, #'SetTimerAction'{timer = Timer, range = HRange, timeout = HandlingTimeout}}) ->
    {set_timer,
        unpack(timer        , Timer          ),
        unpack(history_range, HRange         ),
        unpack(integer      , HandlingTimeout)
    };
unpack(timer_action, {unset_timer, #'UnsetTimerAction'{}}) ->
    unset_timer;
unpack(tag_action, #'TagAction'{tag = Tag}) ->
    unpack(tag, Tag);
unpack(remove_action, #'RemoveAction'{}) ->
    remove;

%% calls, signals, get_gistory
unpack(state_change, #'MachineStateChange'{aux_state=AuxState, events=EventBodies}) ->
    {
        unpack( aux_state        , AuxState   ),
        unpack({list, event_body}, EventBodies)
    };
unpack(signal, {timeout, #'TimeoutSignal'{}}) ->
    timeout;
unpack(signal, {init, #'InitSignal'{arg = Args}}) ->
    {init, unpack(args, Args)};
unpack(signal, {repair, #'RepairSignal'{arg = Args}}) ->
    {repair, unpack(args, Args)};
unpack(call_response, CallResponse) ->
    unpack(opaque, CallResponse);
unpack(signal_args, #'SignalArgs'{signal = Signal, machine = Machine}) ->
    {unpack(signal , Signal), unpack(machine, Machine)};
unpack(call_args, #'CallArgs'{arg = Args, machine = Machine}) ->
    {unpack(args, Args), unpack(machine, Machine)};
unpack(signal_result, #'SignalResult'{change = StateChange, action = ComplexAction}) ->
    {
        unpack(state_change  , StateChange  ),
        unpack(complex_action, ComplexAction)
    };
unpack(call_result, #'CallResult'{response=Response, change = StateChange, action=ComplexAction}) ->
    {
        unpack(call_response , Response     ),
        unpack(state_change  , StateChange  ),
        unpack(complex_action, ComplexAction)
    };

unpack(history_range, #'HistoryRange'{'after' = After, limit = Limit, direction = Direction}) ->
    {unpack(event_id, After), unpack(integer , Limit), unpack(direction, Direction)};

unpack(machine_descriptor, #'MachineDescriptor'{ns=NS, ref=Ref, range=Range}) ->
    {unpack(ns, NS), unpack(ref, Ref), unpack(history_range, Range)};

unpack(sink_event, #'SinkEvent'{id = ID, source_ns = SourceNS, source_id = SourceID, event = Event}) ->
    #{
        id   => unpack(id, ID),
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

-spec pack_opaque(mg_storage:opaque()) ->
    mg_proto_msgpack_thrift:'Value'().
pack_opaque(null) ->
    {nl, #msgpack_Nil{}};
pack_opaque(Boolean) when is_boolean(Boolean) ->
    {b, Boolean};
pack_opaque(Integer) when is_integer(Integer) ->
    {i, Integer};
pack_opaque(Float) when is_float(Float) ->
    {flt, Float};
pack_opaque({string, String}) ->
    {str, unicode:characters_to_binary(String, unicode)};
pack_opaque(Binary) when is_binary(Binary) ->
    {bin, Binary};
pack_opaque(Object) when is_map(Object) ->
    {obj, maps:fold(fun(K, V, Acc) -> maps:put(pack_opaque(K), pack_opaque(V), Acc) end, #{}, Object)};
pack_opaque(Array) when is_list(Array) ->
    {arr, lists:map(fun pack_opaque/1, Array)};
pack_opaque(Arg) ->
    erlang:error(badarg, [Arg]).

-spec unpack_opaque(mg_proto_msgpack_thrift:'Value'()) ->
    mg_storage:opaque().
unpack_opaque({nl, #msgpack_Nil{}}) ->
    null;
unpack_opaque({b, Boolean}) ->
    Boolean;
unpack_opaque({i, Integer}) ->
    Integer;
unpack_opaque({flt, Float}) ->
    Float;
unpack_opaque({str, BString}) ->
    {string, unicode:characters_to_list(BString, unicode)};
unpack_opaque({bin, Binary}) ->
    Binary;
unpack_opaque({obj, Object}) ->
    maps:fold(fun(K, V, Acc) -> maps:put(unpack_opaque(K), unpack_opaque(V), Acc) end, #{}, Object);
unpack_opaque({arr, Array}) ->
    lists:map(fun unpack_opaque/1, Array);
unpack_opaque(Arg) ->
    erlang:error(badarg, [Arg]).


% rfc3339:parse имеет некорретный спек, поэтому диалайзер всегда ругается
-dialyzer({nowarn_function, parse_datetime/1}).
-spec parse_datetime(binary()) ->
    calendar:datetime().
parse_datetime(Datetime) ->
    {ok, {Date, Time, _, undefined}} = rfc3339:parse(Datetime),
    {Date, Time}.

-spec format_datetime(calendar:datetime()) ->
    binary().
format_datetime(Datetime) ->
    {ok, DatetimeBin} = rfc3339:format(Datetime),
    DatetimeBin.
