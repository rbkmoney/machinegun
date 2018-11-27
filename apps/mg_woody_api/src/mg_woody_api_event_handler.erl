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

-module(mg_woody_api_event_handler).

-include_lib("mg_woody_api/include/pulse.hrl").

%% woody_event_handler callbacks
-behaviour(woody_event_handler).
-export([handle_event/4]).

%%
%% woody_event_handler callbacks
%%
-spec handle_event(Event, RpcID, EventMeta, PulseHandler) -> ok when
    Event :: woody_event_handler:event(),
    RpcID :: woody:rpc_id(),
    EventMeta :: woody_event_handler:event_meta(),
    PulseHandler :: mg_pulse:handler().
handle_event(Event, RpcID, EventMeta, PulseHandler) ->
    mg_pulse:handle_beat(PulseHandler, #woody_event{
        event = Event,
        rpc_id = RpcID,
        event_meta = EventMeta
    }).
