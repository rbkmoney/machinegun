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

-module(mg_queue_timer).

-behaviour(mg_scheduler).
-behaviour(mg_scheduler_worker).


-export([build_task_info/2]).
-export([build_task_info/3]).

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
    timer_queue := waiting | retrying,
    processing_timeout => timeout(),
    reschedule_timeout => timeout()
}.
-record(state, {
}).
-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg:id().
-type task_payload() :: #{
    machine_id := mg:id(),
    target_timestamp := timestamp_s(),
    status := undefined | mg_machine:machine_regular_status()
}.
-type task_info() :: mg_scheduler:task_info(task_id(), task_payload()).
-type timestamp_s() :: genlib_time:ts().  % in seconds
-type req_ctx() :: mg:request_context().

-define(DEFAULT_PROCESSING_TIMEOUT, 60000).  % 1 minute
-define(DEFAULT_RESCHEDULE_TIMEOUT, 60000).  % 1 minute

%%
%% API
%%

-spec init(options()) ->
    {ok, state()}.
init(_Options) ->
    {ok, #state{}}.

-spec(build_task_info(mg:id(), genlib_time:ts()) -> task_info()).
build_task_info(ID, Timestamp) ->
    build_task_info(ID, Timestamp, undefined).

-spec(build_task_info(mg:id(), genlib_time:ts(), undefined | mg_machine:machine_regular_status()) -> task_info()).
build_task_info(ID, Timestamp, Status) ->
    CreateTime = erlang:monotonic_time(),
    #{
        id => ID,
        payload => #{
            machine_id => ID,
            target_timestamp => Timestamp,
            status => Status
        },
        created_at => CreateTime,
        target_time => Timestamp,
        machine_id => ID
    }.

-spec search_new_tasks(Options, Limit, State) -> {ok, Status, Result, State} when
    Options :: options(),
    Limit :: non_neg_integer(),
    Result :: [task_info()],
    Status :: mg_scheduler:search_status(),
    State :: state().
search_new_tasks(#{timer_queue := TimerMode} = Options, Limit, State) ->
    MachineOptions = machine_options(Options),
    Query = {TimerMode, 1, genlib_time:unow()},
    {Timers, Continuation} = mg_machine:search(MachineOptions, Query, Limit),
    Tasks = [
        build_task_info(ID, Ts) || {Ts, ID} <- Timers
    ],
    {ok, get_status(Continuation), Tasks, State}.

-spec execute_task(options(), task_info()) ->
    ok.
execute_task(#{timer_queue := TimerMode} = Options, #{payload := Payload}) ->
    MachineOptions = machine_options(Options),
    #{
         machine_id := MachineID,
         target_timestamp := TargetTimestamp
    } = Payload,
    Status =
        case maps:get(status, Payload) of
            undefined ->
                mg_machine:get_status(MachineOptions, MachineID);
            S ->
                S
        end,
    case {TimerMode, Status} of
        {waiting, {waiting, Timestamp, ReqCtx, _Timeout}} when Timestamp =:= TargetTimestamp ->
            call_timeout(Options, MachineID, Timestamp, ReqCtx);
        {retrying, {retrying, Timestamp, _Start, _Attempt, ReqCtx}} when Timestamp =:= TargetTimestamp ->
            call_timeout(Options, MachineID, Timestamp, ReqCtx);
        _Other ->
            ok
    end.

%% Internals

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{machine := MachineOptions}) ->
    MachineOptions.

-spec call_timeout(options(), mg:id(), timestamp_s(), req_ctx()) ->
    ok.
call_timeout(Options, MachineID, Timestamp, ReqCtx) ->
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_deadline:from_timeout(Timeout),
    try
        mg_machine:send_timeout(machine_options(Options), MachineID, Timestamp, ReqCtx, Deadline)
    catch
        throw:{ErrorType, _Details} when
            ErrorType =:= transient orelse
            ErrorType =:= timeout
        ->
            call_retry_wait(Options, MachineID, Timestamp, ReqCtx)
    end.

-spec call_retry_wait(options(), mg:id(), timestamp_s(), req_ctx()) ->
    ok.
call_retry_wait(#{timer_queue := TimerMode} = Options, MachineID, Timestamp, ReqCtx) ->
    MachineOptions = machine_options(Options),
    Timeout = maps:get(reschedule_timeout, Options, ?DEFAULT_RESCHEDULE_TIMEOUT),
    Deadline = mg_deadline:from_timeout(Timeout),
    ok = mg_machine:send_retry_wait(MachineOptions, MachineID, TimerMode, Timestamp, ReqCtx, Deadline).

-spec get_status(mg_storage:continuation()) ->
    mg_scheduler:search_status().
get_status(undefined) ->
    completed;
get_status(_Other) ->
    continue.
