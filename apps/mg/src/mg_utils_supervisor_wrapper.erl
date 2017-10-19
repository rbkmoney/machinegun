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

-module(mg_utils_supervisor_wrapper).

%% API
-export([start_link/2]).
-export([start_link/3]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% API
-spec start_link(supervisor:sup_flags(), [supervisor:child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

-spec start_link(mg_utils:gen_reg_name(), supervisor:sup_flags(), [supervisor:child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(RegName, Flags, ChildsSpecs) ->
    supervisor:start_link(RegName, ?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({supervisor:sup_flags(), [supervisor:child_spec()]}) ->
    mg_utils:supervisor_ret().
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, ChildsSpecs}}.
