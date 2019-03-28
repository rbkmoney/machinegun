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

-module(mg_machine_tags_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups        /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([tag           /1]).
-export([idempotent_tag/1]).
-export([double_tag    /1]).
-export([replace       /1]).
-export([resolve       /1]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        {group, main}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {main, [sequence], [
            tag,
            idempotent_tag,
            double_tag,
            replace,
            resolve
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_storage, '_', '_'}, x),
    Apps = mg_ct_helper:start_applications([mg]),
    Pid = start_automaton(automaton_options()),
    true = erlang:unlink(Pid),
    [{apps, Apps}, {pid, Pid}| C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    true = erlang:exit(?config(pid, C), kill),
    mg_ct_helper:stop_applications(?config(apps, C)).


%%
%% tests
%%
-define(ID       , <<"tagged_id">>).
-define(OTHER_ID , <<"other_id" >>).
-define(TAG      , <<"tag"      >>).

-spec tag(config()) ->
    _.
tag(_C) ->
    ok = mg_machine_tags:add(automaton_options(), ?TAG, ?ID, null, mg_utils:default_deadline()).

-spec idempotent_tag(config()) ->
    _.
idempotent_tag(C) ->
    tag(C).

-spec double_tag(config()) ->
    _.
double_tag(_C) ->
    {already_exists, ?ID} =
        mg_machine_tags:add(automaton_options(), ?TAG, ?OTHER_ID, null, mg_utils:default_deadline()).

-spec replace(config()) ->
    _.
replace(_C) ->
    ok = mg_machine_tags:replace(automaton_options(), ?TAG, ?ID, null, mg_utils:default_deadline()).

-spec resolve(config()) ->
    _.
resolve(_C) ->
    ?ID = mg_machine_tags:resolve(automaton_options(), ?TAG).

%%
%% utils
%%
-spec start_automaton(mg_machine_tags:options()) ->
    pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_machine_tags:child_spec(Options, tags)]
        )
    ).

-spec automaton_options() ->
    mg_machine_tags:options().
automaton_options() ->
    #{
        namespace => <<"test_tags">>,
        storage   => mg_storage_memory,
        pulse     => ?MODULE,
        retries   => #{}
    }.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
