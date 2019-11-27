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

-module(mg_async_action).

%% API
-export([child_spec/2]).
-export([start_child/3]).
-export([start_link/1]).
-export([start_sup/2]).

-callback child_spec(Options :: mg_events_machine:options(), ID :: term()) ->
    supervisor:child_spec().

%% API

-spec child_spec(mg_events_machine:options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ID) ->
    #{
        id       => ID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_child(mg_events_machine:options(), module(), list()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_child(Options, ChildModule, Args) ->
    supervisor:start_child(ref(Options, ChildModule), Args).

-spec start_link(mg_events_machine:options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_one},
        [sup_spec(Options, mg_async_event_sink)]
    ).

-spec start_sup(mg_events_machine:options(), module()) ->
    mg_utils:gen_start_ret().
start_sup(Options, ChildModule) ->
    mg_utils_supervisor_wrapper:start_link(
        ref(Options, ChildModule),
        #{strategy => simple_one_for_one},
        [ChildModule:child_spec(Options, ChildModule)]
    ).

%% Internal functions

-spec sup_spec(mg_events_machine:options(), module()) ->
    supervisor:child_spec().
sup_spec(Options, ChildModule) ->
    #{
        id       => ChildModule,
        start    => {?MODULE, start_sup, [Options, ChildModule]},
        restart  => permanent,
        type     => supervisor
    }.

-spec ref(mg_events_machine:options(), module()) ->
    mg_utils:gen_ref().
ref(Options, ChildModule) ->
    {via, gproc, {n, l, {namespace(Options), ChildModule, supervisor}}}.

-spec namespace(mg_events_machine:options()) ->
    mg:ns().
namespace(#{namespace := Namespace}) ->
    Namespace.
