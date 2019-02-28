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
add_events(Options, NS, ID, Events, _ReqCtx, _Deadline) ->
    #{client := Client, topic := Topic, encoder := Encoder} = Options,
    Batch = [
        #{
            key => partition_key(ID),
            value => Encoder(NS, ID, Event)
        }
        || Event <- Events
    ],
    ok = brod:produce_sync(Client, Topic, hash, partition_key(ID), Batch).

%% Internals

-spec partition_key(mg:id()) ->
    term().
partition_key(ID) ->
    ID.
