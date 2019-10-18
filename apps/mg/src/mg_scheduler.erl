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

-module(mg_scheduler).

-export([child_spec/3]).
-export([start_link/2]).
-export([where_is/1]).

-export([send_task/2]).
-export([distribute_tasks/2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1]).
-export([handle_info/2]).
-export([handle_cast/2]).
-export([handle_call/3]).

%% Types
-type options() :: #{
    start_interval => non_neg_integer(),
    quota_name     := mg_quota_worker:name(),
    quota_share    => mg_quota:share(),
    pulse          => mg_pulse:handler()
}.

-type name() :: atom().
-type id() :: {name(), mg:ns()}.

-type task_id() :: mg_queue_task:id().
-type task() :: mg_queue_task:task().
-type target_time() :: mg_queue_task:target_time().

-export_type([id/0]).
-export_type([name/0]).
-export_type([options/0]).

%% Internal types
-record(state, {
    id :: id(),
    pulse :: mg_pulse:handler(),
    quota_name :: mg_quota_worker:name(),
    quota_share :: mg_quota:share(),
    quota_reserved :: mg_quota:resource() | undefined,
    timer :: timer:tref(),
    waiting_tasks :: waiting_tasks(),
    active_tasks :: #{task_id() => pid()},
    task_monitors :: #{monitor() => task_id()}
}).
-type state() :: #state{}.
-type monitor() :: reference().

-type task_set() :: #{task_id() => task()}.
-type task_rank() :: {target_time(), integer()}.
-type task_queue() :: gb_trees:tree(task_rank(), task_id()).
-type waiting_tasks() :: {task_set(), task_queue(), integer()}.

%%
%% API
%%

-spec child_spec(id(), options(), _ChildID) ->
    supervisor:child_spec().
child_spec(ID, Options, ChildID) ->
    #{
        id    => ChildID,
        start => {?MODULE, start_link, [ID, Options]},
        type  => worker
    }.

-spec start_link(id(), options()) ->
    mg_utils:gen_start_ret().
start_link(ID, Options) ->
    gen_server:start_link(self_reg_name(ID), ?MODULE, {ID, Options}, []).

-spec where_is(id()) ->
    pid() | undefined.
where_is(ID) ->
    mg_utils:gen_ref_to_pid(self_ref(ID)).

-spec send_task(id(), task()) ->
    ok.
send_task(ID, Task) ->
    gen_server:cast(self_ref(ID), {tasks, [Task]}).

-spec distribute_tasks(pid(), [task()]) ->
    ok.
distribute_tasks(Pid, Tasks) when is_pid(Pid) ->
    gen_server:cast(Pid, {tasks, Tasks}).

%% gen_server callbacks

-spec init({id(), options()}) ->
    mg_utils:gen_server_init_ret(state()).
init({ID, Options}) ->
    {ok, TimerRef} = timer:send_interval(maps:get(start_interval, Options, 1000), start),
    {ok, #state{
        id = ID,
        pulse = maps:get(pulse, Options, undefined),
        quota_name = maps:get(quota_name, Options),
        quota_share = maps:get(quota_share, Options, 1),
        quota_reserved = undefined,
        active_tasks = #{},
        task_monitors = #{},
        waiting_tasks = {#{}, gb_trees:empty(), 0},
        timer = TimerRef
    }}.

-spec handle_call(Call :: any(), mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-type cast() ::
    {tasks, [task()]}.

-spec handle_cast(cast(), state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast({tasks, Tasks}, State0) ->
    State1 = add_tasks(Tasks, State0),
    State2 = maybe_update_reserved(State1),
    State3 = start_new_tasks(State2),
    {noreply, State3};
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

-type info() ::
    {'DOWN', monitor(), process, pid(), _Info} |
    start.

-spec handle_info(info(), state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info({'DOWN', Monitor, process, _Object, _Info}, State0) ->
    State1 = forget_about_task(Monitor, State0),
    State2 = start_new_tasks(State1),
    {noreply, State2};
handle_info(start, State0) ->
    State1 = update_reserved(State0),
    State2 = start_new_tasks(State1),
    {noreply, State2};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info received: ~p", [Info]),
    {noreply, State}.

% Process registration

-spec self_reg_name(id()) ->
    mg_procreg:reg_name().
self_reg_name(ID) ->
    mg_procreg:reg_name(mg_procreg_gproc, {?MODULE, ID}).

-spec self_ref(id()) ->
    mg_procreg:ref().
self_ref(ID) ->
    mg_procreg:ref(mg_procreg_gproc, {?MODULE, ID}).

% Helpers

-spec forget_about_task(monitor(), state()) ->
    state().
forget_about_task(Monitor, State) ->
    #state{active_tasks = Tasks, task_monitors = Monitors} = State,
    case maps:find(Monitor, Monitors) of
        {ok, TaskID} ->
            State#state{
                active_tasks = maps:remove(TaskID, Tasks),
                task_monitors = maps:remove(Monitor, Monitors)
            };
        error ->
            State
    end.

-spec add_tasks([task()], state()) ->
    state().
add_tasks(Tasks, State = #state{waiting_tasks = WaitingTasks}) ->
    {NewTasksCount, NewWaitingTasks} = lists:foldl(fun enqueue_task/2, {0, WaitingTasks}, Tasks),
    ok = emit_new_tasks_beat(NewTasksCount, State),
    State#state{waiting_tasks = NewWaitingTasks}.

-spec enqueue_task(task(), {non_neg_integer(), waiting_tasks()}) ->
    {non_neg_integer(), waiting_tasks()}.
enqueue_task(Task = #{id := TaskID}, {N, {TaskSet, Queue, Counter}}) ->
    % TODO
    % Blindly overwriting a task with same ID here if there's one. This is not the best strategy out
    % there but sufficient enough. For example we could overwrite most recent legit task with an
    % outdated one appointed a bit late by some remote queue scanner.
    {NewN, NewTaskSet} = case maps:is_key(TaskID, TaskSet) of
        true  -> {N    , TaskSet#{TaskID := Task}};
        false -> {N + 1, TaskSet#{TaskID => Task}}
    end,
    TargetTime = maps:get(target_time, Task, 0),
    % NOTE
    % Inclusion of the unique counter value here helps to ensure FIFO semantics among tasks with the
    % same target timestamp.
    NewQueue = gb_trees:insert({TargetTime, Counter}, TaskID, Queue),
    {NewN, {NewTaskSet, NewQueue, Counter + 1}}.

-spec start_new_tasks(state()) ->
    state().
start_new_tasks(State = #state{quota_reserved = Reserved, waiting_tasks = WaitingTasks}) ->
    TotalActiveTasks = get_active_task_count(State),
    NewTasksNumber = erlang:max(Reserved - TotalActiveTasks, 0),
    CurrentTime = genlib_time:unow(),
    Iterator = make_iterator(CurrentTime, WaitingTasks),
    start_multiple_tasks(NewTasksNumber, Iterator, State).

-spec start_multiple_tasks(non_neg_integer(), waiting_tasks_iterator(), state()) ->
    state().
start_multiple_tasks(0, _Iterator, State) ->
    State;
start_multiple_tasks(N, Iterator, State) when N > 0 ->
    #state{
        id = ID,
        waiting_tasks = WaitingTasks,
        active_tasks = ActiveTasks,
        task_monitors = Monitors
    } = State,
    case next_task(Iterator) of
        {Rank, TaskID, IteratorNext} when not is_map_key(TaskID, ActiveTasks) ->
            % Task appears not to be running on the scheduler...
            case dequeue_task(Rank, WaitingTasks) of
                {{ok, Task}, NewWaitingTasks} ->
                    % ...so let's start it.
                    {ok, Pid, Monitor} = mg_scheduler_worker:start_task(ID, Task),
                    NewState = State#state{
                        waiting_tasks = NewWaitingTasks,
                        active_tasks = ActiveTasks#{TaskID => Pid},
                        task_monitors = Monitors#{Monitor => TaskID}
                    },
                    start_multiple_tasks(N - 1, IteratorNext, NewState);
                {outdated, NewWaitingTasks} ->
                    % ...but the queue entry seems outdated, let's skip.
                    NewState = State#state{waiting_tasks = NewWaitingTasks},
                    start_multiple_tasks(N, IteratorNext, NewState)
            end;
        {_Rank, _TaskID, IteratorNext} ->
            % Task is running already, possibly with earlier target time, let's leave it for later.
            start_multiple_tasks(N, IteratorNext, State);
        none ->
            State
    end.

-type waiting_tasks_iterator() ::
    {gb_trees:iter(task_rank(), task_id()), target_time()}.

-spec make_iterator(target_time(), waiting_tasks()) ->
    waiting_tasks_iterator().
make_iterator(TargetTimeCutoff, {_, Queue, _}) ->
    {gb_trees:iterator(Queue), TargetTimeCutoff}.

-spec next_task(waiting_tasks_iterator()) ->
    {task_rank(), task_id(), waiting_tasks_iterator()} | none.
next_task({Iterator, TargetTimeCutoff}) ->
    case gb_trees:next(Iterator) of
        {{TargetTime, _} = Rank, TaskID, IteratorNext} when TargetTime =< TargetTimeCutoff ->
            {Rank, TaskID, {IteratorNext, TargetTimeCutoff}};
        {{TargetTime, _}, _, _} when TargetTime > TargetTimeCutoff ->
            none;
        none ->
            none
    end.

-spec dequeue_task(task_rank(), waiting_tasks()) ->
    {{ok, task()} | outdated, waiting_tasks()}.
dequeue_task(Rank = {TargetTime, _}, {TaskSet, Queue, Counter}) ->
    {TaskID, QueueLeft} = gb_trees:take(Rank, Queue),
    {Task, TaskSetLeft} = maps:take(TaskID, TaskSet),
    case maps:get(target_time, Task, 0) of
        TargetTime ->
            {{ok, Task}, {TaskSetLeft, QueueLeft, Counter}};
        _Different ->
            {outdated, {TaskSet, QueueLeft, Counter}}
    end.

-spec update_reserved(state()) ->
    state().
update_reserved(State = #state{id = ID, quota_name = Quota, quota_share = QuotaShare}) ->
    TotalActiveTasks = get_active_task_count(State),
    TotalKnownTasks = TotalActiveTasks + get_waiting_task_count(State),
    ClientOptions = #{
        client_id => ID,
        share => QuotaShare
    },
    Reserved = mg_quota_worker:reserve(ClientOptions, TotalActiveTasks, TotalKnownTasks, Quota),
    NewState = State#state{quota_reserved = Reserved},
    ok = emit_reserved_beat(TotalActiveTasks, TotalKnownTasks, Reserved, NewState),
    NewState.

-spec get_active_task_count(state()) ->
    non_neg_integer().
get_active_task_count(#state{active_tasks = ActiveTasks}) ->
    maps:size(ActiveTasks).

-spec get_waiting_task_count(state()) ->
    non_neg_integer().
get_waiting_task_count(#state{waiting_tasks = {TaskSet, _, _}}) ->
    maps:size(TaskSet).

%% logging

-include_lib("mg/include/pulse.hrl").

-spec emit_beat(mg_pulse:handler(), mg_pulse:beat()) -> ok.
emit_beat(Handler, Beat) ->
    ok = mg_pulse:handle_beat(Handler, Beat).

-spec emit_new_tasks_beat(non_neg_integer(), state()) ->
    ok.
emit_new_tasks_beat(NewTasksCount, #state{pulse = Pulse, id = {Name, NS}}) ->
    emit_beat(Pulse, #mg_scheduler_new_tasks{
        namespace = NS,
        scheduler_name = Name,
        new_tasks_count = NewTasksCount
    }).

-spec emit_reserved_beat(non_neg_integer(), non_neg_integer(), mg_quota:resource(), state()) ->
    ok.
emit_reserved_beat(Active, Total, Reserved, State) ->
    #state{pulse = Pulse, id = {Name, NS}, quota_name = Quota} = State,
    emit_beat(Pulse, #mg_scheduler_quota_reserved{
        namespace = NS,
        scheduler_name = Name,
        active_tasks = Active,
        waiting_tasks = Total - Active,
        quota_name = Quota,
        quota_reserved = Reserved
    }).

-spec maybe_update_reserved(state()) ->
    state().
maybe_update_reserved(#state{quota_reserved = undefined} = State) ->
    update_reserved(State);
maybe_update_reserved(State) ->
    State.
