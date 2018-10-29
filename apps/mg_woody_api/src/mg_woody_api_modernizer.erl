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

-module(mg_woody_api_modernizer).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-export_type([options/0]).
-export([modernizer_child_spec/1]).
-export([modernize_event/3]).

-type options() :: woody_client:options().

%%

-spec modernizer_child_spec(options()) ->
    supervisor:child_spec().
modernizer_child_spec(Options) ->
    woody_client:child_spec(Options).

-spec modernize_event(options(), woody_context:ctx(), mg_events_modernizer:machine_event()) ->
    mg_events_modernizer:modernized_event_body().
modernize_event(Options, WoodyContext, MachineEvent) ->
    Service = {mg_proto_state_processing_thrift, 'Modernizer'},
    Args = [mg_woody_api_packer:pack(machine_event, MachineEvent)],
    {ok, Result} = woody_client:call({Service, 'ModernizeEvent', Args}, Options, WoodyContext),
    mg_woody_api_packer:unpack(modernize_result, Result).
