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
-export([child_spec/3]).
-export([child_spec/4]).
-export([start_link/2]).
-export([start_link/3]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% API

-type sup_flags() :: supervisor:sup_flags().
-type child_spec() :: supervisor:child_spec().

-spec child_spec(sup_flags(), [child_spec()], _ChildID) ->
    child_spec().
child_spec(Flags, Childspecs, ChildID) ->
    #{
        id    => ChildID,
        start => {mg_utils_supervisor_wrapper, start_link, [Flags, Childspecs]},
        type  => supervisor
    }.

-spec child_spec(mg_utils:gen_reg_name(), sup_flags(), [child_spec()], _ChildID) ->
    child_spec().
child_spec(RegName, Flags, Childspecs, ChildID) ->
    #{
        id    => ChildID,
        start => {mg_utils_supervisor_wrapper, start_link, [RegName, Flags, Childspecs]},
        type  => supervisor
    }.

-spec start_link(sup_flags(), [child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

-spec start_link(mg_utils:gen_reg_name(), sup_flags(), [child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(RegName, Flags, ChildsSpecs) ->
    supervisor:start_link(RegName, ?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({sup_flags(), [child_spec()]}) ->
    mg_utils:supervisor_ret().
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, ChildsSpecs}}.
