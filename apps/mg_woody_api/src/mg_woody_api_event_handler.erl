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

%% woody_event_handler callbacks
-behaviour(woody_event_handler).
-export([handle_event/4]).

%%
%% woody_event_handler callbacks
%%
-spec handle_event(Event, RpcID, EventMeta, _)
    -> _ when
        Event     :: woody_event_handler:event     (),
        RpcID     :: woody              :rpc_id    (),
        EventMeta :: woody_event_handler:event_meta().
handle_event(Event, RpcID, EventMeta, _) ->
    {Level, Msg} = woody_event_handler:format_event(Event, EventMeta, RpcID),
    mg_woody_api_logger:log({Level, Msg, mg_woody_api_logger:woody_rpc_id_to_meta(RpcID)}).
