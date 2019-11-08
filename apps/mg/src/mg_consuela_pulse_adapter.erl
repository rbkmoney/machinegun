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

-module(mg_consuela_pulse_adapter).

-export([pulse/2]).

-type producer() ::
      client
    | registry_server
    | session_keeper
    | zombie_reaper
    | discovery_server
    | presence_session
    .

-type beat() ::
    {consuela,
          {client           , consuela_client:beat()}
        | {registry_server  , consuela_registry_server:beat()}
        | {session_keeper   , consuela_session_keeper:beat()}
        | {zombie_reaper    , consuela_zombie_reaper:beat()}
        | {discovery_server , consuela_discovery_server:beat()}
        | {presence_session , consuela_presence_session:beat()}
    }.

-export_type([beat/0]).

-export([handle_beat/2]).

%%

-spec pulse(producer(), mg_pulse:handler()) ->
    {module(), _Opts}.

pulse(Producer, Handler) ->
    {?MODULE, {Producer, Handler}}.

%%

-spec handle_beat
    (consuela_client:beat()            , {client           , mg_pulse:handler()}) -> _;
    (consuela_registry_server:beat()   , {registry_server  , mg_pulse:handler()}) -> _;
    (consuela_session_keeper:beat()    , {session_keeper   , mg_pulse:handler()}) -> _;
    (consuela_zombie_reaper:beat()     , {zombie_reaper    , mg_pulse:handler()}) -> _;
    (consuela_discovery_server:beat()  , {discovery_server , mg_pulse:handler()}) -> _;
    (consuela_presence_session:beat()  , {presence_session , mg_pulse:handler()}) -> _.

handle_beat(Beat, {Producer, Handler}) ->
    mg_pulse:handle_beat(Handler, {consuela, {Producer, Beat}}).
