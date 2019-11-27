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

-module(mg_queue_async).

-behaviour(mg_queue_scanner).
-export([init/1]).
-export([search_tasks/3]).

-behaviour(mg_scheduler_worker).
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
    continuation :: mg_storage:continuation()
}).
-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg:id().
-type task_payload() :: #{
    machine_id := mg:id()
}.
-type task() :: mg_queue_task:task(task_id(), task_payload()).
-type scan_delay() :: mg_queue_scanner:scan_delay().

-define(DEFAULT_PROCESSING_TIMEOUT, 60000).  % 1 minute

%%
%% API
%%

-spec init(options()) ->
    {ok, state()}.
init(_Options) ->
    {ok, #state{continuation = undefined}}.

-spec search_tasks(options(), _Limit :: non_neg_integer(), state()) ->
    {{scan_delay(), [task()]}, state()}.
search_tasks(Options, Limit, #state{continuation = Continuation} = State) ->
    MachineOptions = machine_options(Options),
    Query = async,
    {IDs, NewContinuation} = mg_machine:search(MachineOptions, Query, Limit, Continuation),
    CreateTime = erlang:monotonic_time(),
    Tasks = [
        #{
            id => {async, ID},
            payload => #{
                machine_id => ID
            },
            created_at => CreateTime,
            machine_id => ID
        }
        || ID <- IDs
    ],
    Delay = get_delay(NewContinuation, Options),
    NewState = State#state{continuation = NewContinuation},
    {{Delay, Tasks}, NewState}.

-spec execute_task(options(), task()) ->
    ok.
execute_task(Options, TaskInfo) ->
    MachineOptions = machine_options(Options),
    #{
        payload := #{
            machine_id := MachineID
        }
    } = TaskInfo,
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_deadline:from_timeout(Timeout),
    ok = mg_machine:resume_async_action(MachineOptions, MachineID, null, Deadline).

%% Internals

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{machine := MachineOptions}) ->
    MachineOptions.

-spec get_delay(mg_storage:continuation(), options()) ->
    scan_delay().
get_delay(undefined, Options) ->
    maps:get(rescan_delay, Options, 10 * 60 * 1000);
get_delay(_Other, Options) ->
    maps:get(min_scan_delay, Options, 1000).
