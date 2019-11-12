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

-type search() :: {
    mg_machine:search_query(),
    scan_limit(),
    mg_storage:continuation() | undefined
}.

-record(state, {
    pending :: search() | undefined
}).

-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg:id().
-type task_payload() :: #{}.
-type target_time() :: mg_queue_task:target_time().
-type task() :: mg_queue_task:task(task_id(), task_payload()).
-type scan_status() :: mg_queue_scanner:scan_status().
-type scan_limit() :: mg_queue_scanner:scan_limit().

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
    CreateTime = erlang:monotonic_time(),
    #{
        id          => ID,
        payload     => #{},
        created_at  => CreateTime,
        target_time => Timestamp,
        machine_id  => ID
    }.

-spec search_tasks(options(), scan_limit(), state()) ->
    {{scan_status(), [task()]}, state()}.
search_tasks(Options, Limit, State = #state{pending = undefined}) ->
    search_tasks(Options, Limit, State#state{pending = construct_search(Options, Limit)});
search_tasks(Options, _Limit, State = #state{pending = Search}) ->
    {Timers, SearchNext} = execute_search(Options, Search),
    Status = get_status(SearchNext),
    Tasks = [build_task(ID, Ts) || {Ts, ID} <- Timers],
    {{Status, Tasks}, State#state{pending = SearchNext}}.

-spec construct_search(options(), scan_limit()) ->
    search().
construct_search(#{timer_queue := TimerMode} = Options, Limit) ->
    Lookahead = maps:get(lookahead, Options, 0),
    Query = {TimerMode, 1, mg_queue_task:current_time() + Lookahead},
    {Query, Limit, undefined}.

-spec execute_search(options(), search()) ->
    {[_Result], search() | undefined}.
execute_search(Options, Search = {Query, Limit, undefined}) ->
    handle_search_result(mg_machine:search(machine_options(Options), Query, Limit), Search);
execute_search(Options, Search = {Query, Limit, Continuation}) ->
    handle_search_result(mg_machine:search(machine_options(Options), Query, Limit, Continuation), Search).

-spec handle_search_result(mg_storage:search_result(), search()) ->
    {[_Result], search() | undefined}.
handle_search_result({Results, undefined}, _Search) ->
    {Results, undefined}; % search is finished
handle_search_result({Results, Continuation}, {Query, Limit, _}) ->
    {Results, {Query, Limit, Continuation}}.

-spec execute_task(options(), task()) ->
    ok.
execute_task(Options, #{id := MachineID, target_time := Timestamp}) ->
    call_timeout(Options, MachineID, Timestamp).

%% Internals

-spec call_timeout(options(), mg:id(), mg_queue_task:target_time()) ->
    ok.
call_timeout(Options, MachineID, Timestamp) ->
    %% NOTE
    %% Machine identified by `MachineID` may in fact already have processed timeout signal so that
    % the task we're in is already stale, and we could shed it by reading the machine status. But we
    % expect that most tasks are not stale yet and most of the time machine is online, therefore
    % it's very likely that reading machine status is just unnecessary.
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_deadline:from_timeout(Timeout),
    ok = mg_machine:send_timeout(machine_options(Options), MachineID, Timestamp, Deadline).

-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{machine := MachineOptions}) ->
    MachineOptions.

-spec get_status(search() | undefined) ->
    mg_queue_scanner:scan_status().
get_status(undefined) ->
    completed;
get_status(_Search) ->
    continue.
