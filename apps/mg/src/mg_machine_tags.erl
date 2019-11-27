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

-module(mg_machine_tags).

%% API
-export_type([options/0]).
-export_type([tag    /0]).
-export([child_spec/2]).
-export([add       /5]).
-export([replace   /5]).
-export([resolve   /2]).

%% mg_machine handler
-behaviour(mg_machine).
-export([process_machine/7]).

-type options() :: #{
    namespace => mg:ns(),
    worker    => mg_workers_manager:options(),
    storage   => mg_machine:storage_options(),
    pulse     => mg_pulse:handler(),
    retries   => mg_machine:retry_opt()
}.
-type tag() :: binary().

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_machine:child_spec(machine_options(Options), ChildID).

-spec add(options(), tag(), mg:id(), mg:request_context(), mg_deadline:deadline()) ->
    ok | {already_exists, mg:id()} | no_return().
add(Options, Tag, ID, ReqCtx, Deadline) ->
    mg_machine:call_with_lazy_start(machine_options(Options), Tag, {add, ID}, ReqCtx, Deadline, undefined).

-spec replace(options(), tag(), mg:id(), mg:request_context(), mg_deadline:deadline()) ->
    ok | no_return().
replace(Options, Tag, ID, ReqCtx, Deadline) ->
    mg_machine:call_with_lazy_start(machine_options(Options), Tag, {replace, ID}, ReqCtx, Deadline, undefined).

-spec resolve(options(), tag()) ->
    mg:id() | undefined | no_return().
resolve(Options, Tag) ->
    try
        opaque_to_state(mg_machine:get(machine_options(Options), Tag))
    catch throw:{logic, machine_not_found} ->
        undefined
    end.

%%
%% mg_machine handler
%%
-type state() :: mg:id() | undefined.

-spec process_machine(_, mg:id(), mg_machine:processor_impact(), _, _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result().
process_machine(_, _, {init, undefined}, _, _, _, _) ->
    {{reply, ok}, sleep, [], state_to_opaque(undefined)};
process_machine(_, _, {repair, undefined}, _, _, _, State) ->
    {{reply, ok}, sleep, [], State};
process_machine(_, _, {call, {add, ID}}, _, _, _, PackedState) ->
    case opaque_to_state(PackedState) of
        undefined ->
            {{reply, ok}, sleep, [], state_to_opaque(ID)};
        ID ->
            {{reply, ok}, sleep, [], PackedState};
        OtherID ->
            {{reply, {already_exists, OtherID}}, sleep, [], PackedState}
    end;
process_machine(_, _, {call, {replace, ID}}, _, _, _, _) ->
    {{reply, ok}, sleep, [], state_to_opaque(ID)}.

%%
%% local
%%
-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Opts = #{namespace:=Namespace, storage:=Storage, pulse := Pulse, retries := Retries}) ->
    #{
        namespace => Namespace,
        processor => ?MODULE,
        worker    => maps:get(worker, Opts, #{}),
        storage   => Storage,
        pulse     => Pulse,
        retries   => Retries
    }.

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(undefined) ->
    [1, null];
state_to_opaque(ID) ->
    [1, ID].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
opaque_to_state([1, null]) ->
    undefined;
opaque_to_state([1, ID]) ->
    ID.
