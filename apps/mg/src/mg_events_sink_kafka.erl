%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_events_sink_kafka).

-include_lib("mg/include/pulse.hrl").

%% mg_events_sink handler
-behaviour(mg_events_sink).
-export([add_events/6]).

%% Types

-type options() :: #{
    name := atom(),
    topic := brod:topic(),
    client := brod:client(),
    pulse := mg_pulse:handler(),
    encoder := encoder()
}.

-type encoder() :: fun((mg:ns(), mg:id(), event()) -> iodata()).

-export_type([options/0]).
-export_type([encoder/0]).

%% Internal types

-type event() :: mg_events:event().
-type req_ctx() :: mg:request_context().
-type deadline() :: mg_utils:deadline().

%% API

-spec add_events(options(), mg:ns(), mg:id(), [event()], req_ctx(), deadline()) ->
    ok.
add_events(Options, NS, MachineID, Events, ReqCtx, Deadline) ->
    #{pulse := Pulse, client := Client, topic := Topic, encoder := Encoder, name := Name} = Options,
    StartTimestamp = erlang:monotonic_time(),
    Batch = encode(Encoder, NS, MachineID, Events),
    EncodeTimestamp = erlang:monotonic_time(),
    ok = produce(Client, Topic, partition_key(MachineID), Batch),
    FinishTimestamp = erlang:monotonic_time(),
    ok = mg_pulse:handle_beat(Pulse, #mg_events_sink_sent{
        name = Name,
        namespace = NS,
        machine_id = MachineID,
        request_context = ReqCtx,
        deadline = Deadline,
        encode_duration = EncodeTimestamp - StartTimestamp,
        send_duration = FinishTimestamp - EncodeTimestamp,
        data_size = batch_size(Batch)
    }).

%% Internals

-spec partition_key(mg:id()) ->
    term().
partition_key(MachineID) ->
    MachineID.

-spec encode(encoder(), mg:ns(), mg:id(), [event()]) ->
    brod:batch_input().
encode(Encoder, NS, MachineID, Events) ->
    [
        #{
            key => partition_key(MachineID),
            value => Encoder(NS, MachineID, Event)
        }
        || Event <- Events
    ].

-spec produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    ok.
produce(Client, Topic, Key, Batch) ->
    case brod:produce_sync(Client, Topic, hash, Key, Batch) of
        ok ->
            ok;
        {error, Reason} when
            Reason =:= leader_not_available orelse
            Reason =:= unknown_topic_or_partition
        ->
            erlang:throw({transient, {event_sink_unavailable, Reason}});
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

-spec batch_size(brod:batch_input()) ->
    non_neg_integer().
batch_size(Batch) ->
    lists:foldl(
        fun(#{value := Value}, Acc) ->
            Acc + erlang:iolist_size(Value)
        end,
        0,
        Batch
    ).
