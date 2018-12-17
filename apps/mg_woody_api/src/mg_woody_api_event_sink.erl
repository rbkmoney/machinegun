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

-module(mg_woody_api_event_sink).

%% API
-export([handler/1]).
-export_type([options/0]).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%%
%% API
%%
-type options() :: {[mg:id()], mg_events_sink:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/event_sink", {{mg_proto_state_processing_thrift, 'EventSink'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('GetHistory', [EventSinkID, Range], WoodyContext, {AvaliableEventSinks, Options}) ->
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    DefaultTimeout = maps:get(default_processing_timeout, Options),
    DefaultDeadline = mg_utils:timeout_to_deadline(DefaultTimeout),
    Deadline = mg_woody_api_utils:get_deadline(WoodyContext, DefaultDeadline),
    SinkHistory =
        mg_woody_api_utils:handle_error(
            #{namespace => undefined, machine_ref => EventSinkID, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                _ = check_event_sink(AvaliableEventSinks, EventSinkID),
                mg_events_sink:get_history(
                    Options,
                    EventSinkID,
                    mg_woody_api_packer:unpack(history_range, Range)
                )
            end,
            pulse(Options)
        ),
    {ok, mg_woody_api_packer:pack(sink_history, SinkHistory)}.

-spec check_event_sink([mg:id()], mg:id()) ->
    ok | no_return().
check_event_sink(AvaliableEventSinks, EventSinkID) ->
    case lists:member(EventSinkID, AvaliableEventSinks) of
        true ->
            ok;
        false ->
            throw({logic, event_sink_not_found})
    end.

-spec pulse(mg_events_sink:options()) ->
    mg_pulse:handler().
pulse(#{pulse := Pulse}) ->
    Pulse.
