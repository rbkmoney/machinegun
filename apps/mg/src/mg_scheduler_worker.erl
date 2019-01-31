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

-export([child_spec/2]).
-export([start_link/1]).

-export([start_task/3]).

%% Internal API
-export([do_start_task/2]).
-export([execute/2]).

-callback execute_task(Options :: any(), task_info()) -> ok.

%% Internal types
-type options() :: mg_scheduler:options().
-type scheduler_id() :: {mg:ns(), name()}.
-type name() :: mg_scheduler:name().
-type task_info() :: mg_scheduler:task_info().

%%
%% API
%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(#{name := Name, namespace := NS} = Options) ->
    mg_utils_supervisor_wrapper:start_link(
        self_reg_name({NS, Name}),
        #{strategy => simple_one_for_one},
        [
            #{
                id       => tasks,
                start    => {?MODULE, do_start_task, [Options]},
                restart  => temporary
            }
        ]
    ).

-spec start_task(mg:ns(), name(), task_info()) ->
    mg_utils:gen_start_ret().
start_task(NS, Name, TaskInfo) ->
    supervisor:start_child(self_ref({NS, Name}), [TaskInfo]).

-spec do_start_task(options(), task_info()) ->
    mg_utils:gen_start_ret().
do_start_task(Options, TaskInfo) ->
    proc_lib:start_link(?MODULE, execute, [Options, TaskInfo]).

-spec execute(options(), task_info()) ->
    ok.
execute(#{task_handler := Handler} = Options, TaskInfo) ->
    ok = proc_lib:init_ack({ok, self()}),
    Start = erlang:monotonic_time(),
    ok = emit_start_beat(TaskInfo, Options),
    ok = try
        ok = mg_utils:apply_mod_opts(Handler, execute_task, [TaskInfo]),
        End = erlang:monotonic_time(),
        ok = emit_finish_beat(TaskInfo, Start, End, Options)
    catch
        Class:Reason ->
            Exception = {Class, Reason, erlang:get_stacktrace()},
            ok = emit_error_beat(TaskInfo, Exception, Options)
    end.

%% Internlas

% Process registration

-spec self_ref(scheduler_id()) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec self_reg_name(scheduler_id()) ->
    mg_utils:gen_reg_name().
self_reg_name(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec wrap_id(scheduler_id()) ->
    term().
wrap_id(ID) ->
    {?MODULE, ID}.

%% logging

-spec emit_beat(options(), mg_pulse:beat()) -> ok.
emit_beat(#{pulse := Handler}, Beat) ->
    ok = mg_pulse:handle_beat(Handler, Beat).

-spec get_delay(task_info()) ->
    timeout().
get_delay(#{target_time := Target}) ->
    TargetMS = Target * 1000,
    os:system_time(millisecond) - TargetMS;
get_delay(#{}) ->
    undefined.

-spec emit_start_beat(task_info(), options()) ->
    ok.
emit_start_beat(Task, #{namespace := NS, name := Name} = Options) ->
    emit_beat(Options, #mg_scheduler_task_started{
        namespace = NS,
        scheduler_name = Name,
        task_delay = get_delay(Task),
        machine_id = maps:get(machine_id, Task, undefined)
    }).

-spec emit_finish_beat(task_info(), integer(), integer(), options()) ->
    ok.
emit_finish_beat(#{created_at := Create} = Task, Start, End, #{namespace := NS, name := Name} = Options) ->
    emit_beat(Options, #mg_scheduler_task_finished{
        namespace = NS,
        scheduler_name = Name,
        task_delay = get_delay(Task),
        machine_id = maps:get(machine_id, Task, undefined),
        waiting_in_queue = Start - Create,  % in native units
        process_duration = End - Start  % in native units
    }).

-spec emit_error_beat(task_info(), mg_utils:exception(), options()) ->
    ok.
emit_error_beat(Task, Exception, #{namespace := NS, name := Name} = Options) ->
    emit_beat(Options, #mg_scheduler_task_error{
        namespace = NS,
        scheduler_name = Name,
        exception = Exception,
        machine_id = maps:get(machine_id, Task, undefined)
    }).
