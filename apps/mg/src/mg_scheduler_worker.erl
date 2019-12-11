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

-module(mg_scheduler_worker).

-include_lib("mg/include/pulse.hrl").

-export([child_spec/3]).
-export([start_link/2]).

-export([start_task/2]).

%% Internal API
-export([do_start_task/3]).
-export([execute/3]).

-callback execute_task(Options :: any(), task()) -> ok.

%% Internal types
-type scheduler_id() :: mg_scheduler:id().
-type task() :: mg_queue_task:task().

-type options() :: #{
    task_handler := mg_utils:mod_opts(),
    pulse        => mg_pulse:handler()
}.

-type monitor() :: reference().

%%
%% API
%%

-spec child_spec(scheduler_id(), options(), _ChildID) ->
    supervisor:child_spec().
child_spec(SchedulerID, Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [SchedulerID, Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(scheduler_id(), options()) ->
    mg_utils:gen_start_ret().
start_link(SchedulerID, Options) ->
    mg_utils_supervisor_wrapper:start_link(
        self_reg_name(SchedulerID),
        #{strategy => simple_one_for_one},
        [
            #{
                id       => tasks,
                start    => {?MODULE, do_start_task, [SchedulerID, Options]},
                restart  => temporary
            }
        ]
    ).

-spec start_task(scheduler_id(), task()) ->
    {ok, pid(), monitor()} | {error, _}.
start_task(SchedulerID, Task) ->
    case supervisor:start_child(self_ref(SchedulerID), [Task]) of
        {ok, Pid} ->
            Monitor = erlang:monitor(process, Pid),
            {ok, Pid, Monitor};
        Error ->
            Error
    end.

-spec do_start_task(scheduler_id(), options(), task()) ->
    mg_utils:gen_start_ret().
do_start_task(SchedulerID, Options, Task) ->
    proc_lib:start_link(?MODULE, execute, [SchedulerID, Options, Task]).

-spec execute(scheduler_id(), options(), task()) ->
    ok.
execute(SchedulerID, #{task_handler := Handler} = Options, Task) ->
    ok = proc_lib:init_ack({ok, self()}),
    Start = erlang:monotonic_time(),
    ok = emit_start_beat(Task, SchedulerID, Options),
    ok = try
        ok = mg_utils:apply_mod_opts(Handler, execute_task, [Task]),
        End = erlang:monotonic_time(),
        ok = emit_finish_beat(Task, Start, End, SchedulerID, Options)
    catch
        Class:Reason:ST ->
            Exception = {Class, Reason, ST},
            ok = emit_error_beat(Task, Exception, SchedulerID, Options)
    end.

%% Internlas

% Process registration

-spec self_ref(scheduler_id()) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    mg_procreg:ref(mg_procreg_gproc, wrap_id(ID)).

-spec self_reg_name(scheduler_id()) ->
    mg_utils:gen_reg_name().
self_reg_name(ID) ->
    mg_procreg:reg_name(mg_procreg_gproc, wrap_id(ID)).

-spec wrap_id(scheduler_id()) ->
    term().
wrap_id(ID) ->
    {?MODULE, ID}.

%% logging

-spec emit_beat(options(), mg_pulse:beat()) ->
    ok.
emit_beat(Options, Beat) ->
    ok = mg_pulse:handle_beat(maps:get(pulse, Options, undefined), Beat).

-spec get_delay(task()) ->
    timeout().
get_delay(#{target_time := Target}) ->
    TargetMS = Target * 1000,
    os:system_time(millisecond) - TargetMS;
get_delay(#{}) ->
    undefined.

-spec emit_start_beat(task(), scheduler_id(), options()) ->
    ok.
emit_start_beat(Task, {Name, NS}, Options) ->
    emit_beat(Options, #mg_scheduler_task_started{
        namespace = NS,
        scheduler_name = Name,
        task_delay = get_delay(Task),
        machine_id = maps:get(machine_id, Task, undefined)
    }).

-spec emit_finish_beat(task(), integer(), integer(), scheduler_id(), options()) ->
    ok.
emit_finish_beat(#{target_time := TargetTime} = Task, StartedAt, FinishedAt, {Name, NS}, Options) ->
    ScheduledOn = erlang:convert_time_unit(TargetTime, second, native),
    emit_beat(Options, #mg_scheduler_task_finished{
        namespace = NS,
        scheduler_name = Name,
        task_delay = get_delay(Task),
        machine_id = maps:get(machine_id, Task, undefined),
        waiting_in_queue = StartedAt - ScheduledOn, % in native units
        process_duration = FinishedAt - StartedAt  % in native units
    }).

-spec emit_error_beat(task(), mg_utils:exception(), scheduler_id(), options()) ->
    ok.
emit_error_beat(Task, Exception, {Name, NS}, Options) ->
    emit_beat(Options, #mg_scheduler_task_error{
        namespace = NS,
        scheduler_name = Name,
        exception = Exception,
        machine_id = maps:get(machine_id, Task, undefined)
    }).
