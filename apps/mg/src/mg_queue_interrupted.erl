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

-module(mg_queue_interrupted).

-behaviour(mg_scheduler).
-behaviour(mg_scheduler_worker).

%% mg_scheduler callbacks
-export([init/1]).
-export([search_new_tasks/3]).

%% mg_scheduler_worker callbacks
-export([execute_task/2]).

%% Types

-type options() :: #{
    namespace := mg:ns(),
    scheduler_name := mg_scheduler:name(),
    pulse := mg_pulse:handler(),
    machine := mg_machine:options(),
    processing_timeout => timeout()
}.
-record(state, {
    continuation :: mg_storage:continuation() | obsolete
}).
-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg:id().
-type task_payload() :: #{
    machine_id := mg:id(),
    target_timestamp := timestamp_s()
}.
-type task_info() :: mg_scheduler:task_info(task_id(), task_payload()).
-type timestamp_s() :: genlib_time:ts().  % in seconds
-type continuation() :: mg_storage:continuation().
-type machine_options() :: mg_machine:options().

-define(DEFAULT_PROCESSING_TIMEOUT, 60000).  % 1 minute

%%
%% API
%%

-spec init(options()) ->
    {ok, state()}.
init(_Options) ->
    {ok, #state{continuation = obsolete}}.

-spec search_new_tasks(Options, Limit, State) -> {ok, Result, State} when
    Options :: options(),
    Limit :: non_neg_integer(),
    Result :: [task_info()],
    State :: state().
search_new_tasks(Options, Limit, #state{continuation = Continuation} = State) ->
    MachineOptions = machine_options(Options),
    case search(MachineOptions, Limit, Continuation) of
        {[], _Continuation} ->
            {ok, [], State#state{continuation = obsolete}};
        {IDs, NewContinuation} ->
            CreateTime = erlang:monotonic_time(),
            Tasks = [
                #{
                    id => ID,
                    payload => #{
                        machine_id => ID
                    },
                    created_at => CreateTime,
                    machine_id => ID
                }
                || ID <- IDs
            ],
            {ok, Tasks, State#state{continuation = NewContinuation}}
    end.

-spec execute_task(options(), task_info()) ->
    ok.
execute_task(Options, TaskInfo) ->
    MachineOptions = machine_options(Options),
    #{
        payload := #{
            machine_id := MachineID
        }
    } = TaskInfo,
    case mg_machine:get_status(MachineOptions, MachineID) of
        {processing, ReqCtx} ->
            Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
            Deadline = mg_utils:timeout_to_deadline(Timeout),
            ok = mg_machine:resume_interrupted(MachineOptions, MachineID, ReqCtx, Deadline);
        _Other ->
            ok
    end.

%% Internals

-spec search(machine_options(), non_neg_integer(), continuation() | obsolete) ->
    {[mg:id()], continuation()}.
search(_MachineOptions, _Limit, undefined) ->
    {[], obsolete};
search(MachineOptions, Limit, obsolete) ->
    Query = processing,
    mg_machine:search(MachineOptions, Query, Limit);
search(MachineOptions, Limit, Continuation) ->
    Query = processing,
    mg_machine:search(MachineOptions, Query, Limit, Continuation).

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{machine := MachineOptions}) ->
    MachineOptions.
