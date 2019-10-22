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

-export([build_task/2]).
-export([build_task/3]).

-behaviour(mg_queue_scanner).
-export([init/1]).
-export([search_tasks/3]).

-behaviour(mg_scheduler_worker).
-export([execute_task/2]).

%% Types

-type seconds() :: non_neg_integer().
-type options() :: #{
    namespace := mg:ns(),
    scheduler_name := mg_scheduler:name(),
    pulse := mg_pulse:handler(),
    machine := mg_machine:options(),
    timer_queue := waiting | retrying,
    lookahead => seconds(),
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
    status := undefined | mg_machine:machine_regular_status()
}.
-type target_time() :: mg_queue_task:target_time().
-type task() :: mg_queue_task:task(task_id(), task_payload()).
-type scan_status() :: mg_queue_scanner:scan_status().
-type scan_limit() :: mg_queue_scanner:scan_limit().
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

-spec build_task(mg:id(), target_time()) ->
    task().
build_task(ID, Timestamp) ->
    build_task(ID, Timestamp, undefined).

-spec build_task(mg:id(), target_time(), undefined | mg_machine:machine_regular_status()) ->
    task().
build_task(ID, Timestamp, Status) ->
    CreateTime = erlang:monotonic_time(),
    #{
        id          => ID,
        payload     => #{status => Status},
        created_at  => CreateTime,
        target_time => Timestamp,
        machine_id  => ID
    }.

-spec search_tasks(options(), scan_limit(), state()) ->
    {{scan_status(), [task()]}, state()}.
search_tasks(#{timer_queue := TimerMode} = Options, Limit, State) ->
    Lookahead = maps:get(lookahead, Options, 0),
    MachineOptions = machine_options(Options),
    Query = {TimerMode, 1, mg_queue_task:current_time() + Lookahead},
    {Timers, Continuation} = mg_machine:search(MachineOptions, Query, Limit),
    {Tasks, Count} = lists:foldl(
        fun ({Ts, ID}, {Acc, C}) -> {[build_task(ID, Ts) | Acc], C + 1} end,
        {[], 0},
        Timers
    ),
    {{get_status(Count, Limit, Continuation), Tasks}, State}.

-spec execute_task(options(), task()) ->
    ok.
execute_task(
    #{timer_queue := TimerMode} = Options,
    #{id := MachineID, payload := Payload, target_time := TargetTimestamp}
) ->
    MachineOptions = machine_options(Options),
    Status =
        case maps:get(status, Payload) of
            undefined ->
                mg_machine:get_status(MachineOptions, MachineID);
            S ->
                S
        end,
    % TODO
    % Do we really need to check this _every_ other task?
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

-spec call_timeout(options(), mg:id(), mg_queue_task:target_time(), req_ctx()) ->
    ok.
call_timeout(Options, MachineID, Timestamp, ReqCtx) ->
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_deadline:from_timeout(Timeout),
    ok = mg_machine:send_timeout(machine_options(Options), MachineID, Timestamp, ReqCtx, Deadline).

-spec get_status(non_neg_integer(), scan_limit(), mg_storage:continuation()) ->
    mg_queue_scanner:scan_status().
get_status(_Count, _Limit, undefined) ->
    completed;
get_status(Count, Limit, _Other) when Count < Limit ->
    % NOTE
    % For some reason continuation is not `undefined` even when there are less results than asked
    % for.
    completed;
get_status(_Count, _Limit, _Other) ->
    continue.
