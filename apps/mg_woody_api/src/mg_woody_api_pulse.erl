%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_woody_api_pulse).

-include_lib("mg_woody_api/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_pulse).
-export([handle_beat/2]).

%% pulse types
-type beat() ::
      mg_pulse:beat()
    | #woody_event{}
    | #woody_request_handle_error{}.

-export_type([beat   /0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(undefined, beat()) ->
    ok.
handle_beat(Options, Beat) ->
    ok = mg_woody_api_pulse_log:handle_beat(Options, Beat),
    ok = mg_woody_api_pulse_metric:handle_beat(Options, Beat).
