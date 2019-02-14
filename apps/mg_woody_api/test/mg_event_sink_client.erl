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

-module(mg_event_sink_client).

%% API
-export_type([options/0]).
-export([get_history/3]).

%%
%% API
%%
-type options() :: URL::string().

-spec get_history(options(), mg:id(), mg_proto_state_processing_thrift:'HistoryRange'()) ->
    mg_proto_state_processing_thrift:'SinkHistory'().
get_history(BaseURL, EventSinkID, Range) ->
    call_service(BaseURL, 'GetHistory', [EventSinkID, Range]).

%%
%% local
%%
-spec call_service(_BaseURL, atom(), [_arg]) ->
    _.
call_service(BaseURL, Function, Args) ->
    WR = woody_client:call(
            {{mg_proto_state_processing_thrift, 'EventSink'}, Function, Args},
            #{
                url           => BaseURL ++ "/v1/event_sink",
                event_handler => {mg_woody_api_event_handler, mg_woody_api_pulse}
            },
            woody_context:new()
        ),
    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    end.
