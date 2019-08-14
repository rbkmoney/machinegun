%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_woody_api_pulse_log).

-include_lib("mg/include/pulse.hrl").
-include_lib("mg_woody_api/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_pulse).
-export([handle_beat/2]).

%% internal types
-type log_msg() :: mg_woody_api_log:log_msg().
-type meta() :: mg_woody_api_log:meta().
-type beat() :: mg_woody_api_pulse:beat().

%%
%% mg_pulse handler
%%

-spec handle_beat(undefined, beat()) ->
    ok.
handle_beat(undefined, Beat) ->
    ok = mg_woody_api_log:log(format_beat(Beat)).

%% Internals

-define(beat_to_meta(RecordName, Record),
    [{mg_pulse_event_id, RecordName} | lists:flatten([
        extract_meta(FieldName, Value) ||
        {FieldName, Value} <- lists:zip(
            record_info(fields, RecordName),
            erlang:tl(erlang:tuple_to_list(Record))
        )
    ])]
).

-spec format_beat(beat()) ->
    log_msg() | undefined.
format_beat(#woody_request_handle_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(woody_request_handle_error, Beat),
    LogLevel = case Reason of
        {logic, _Details} ->
            % бизнес ошибки это не warning
            info;
        _OtherReason ->
            warning
    end,
    {LogLevel, {"request handling failed ~p", [Reason]}, Context};
format_beat(#woody_event{event = Event, rpc_id = RPCID, event_meta = EventMeta}) ->
    WoodyMetaFields = [event, service, function, type, metadata, url, deadline, role, execution_duration_ms],
    {Level, Msg, WoodyMeta} = woody_event_handler:format_event_and_meta(Event, EventMeta, RPCID, WoodyMetaFields),
    Meta = lists:flatten([extract_woody_meta(WoodyMeta), extract_meta(rpc_id, RPCID)]),
    {Level, Msg, Meta};
format_beat(#mg_scheduler_task_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_scheduler_task_error, Beat),
    {warning, {"scheduler task ~p failed ~p", [Name, Reason]}, Context};
format_beat(#mg_scheduler_task_add_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_scheduler_task_add_error, Beat),
    {warning, {"scheduler task ~p add failed ~p", [Name, Reason]}, Context};
format_beat(#mg_scheduler_search_error{scheduler_name = Name, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_scheduler_search_error, Beat),
    {warning, {"scheduler search ~p failed ~p", [Name, Reason]}, Context};
format_beat(#mg_machine_process_transient_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_process_transient_error, Beat),
    {warning, {"transient error ~p", [Reason]}, Context};
format_beat(#mg_machine_lifecycle_failed{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_failed, Beat),
    {error, {"machine failed ~p", [Reason]}, Context};
format_beat(#mg_machine_lifecycle_loading_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_loading_error, Beat),
    {error, {"loading failed ~p", [Reason]}, Context};
format_beat(#mg_machine_lifecycle_committed_suicide{} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_committed_suicide, Beat),
    {info, {"machine has committed suicide", []}, Context};
format_beat(#mg_machine_lifecycle_transient_error{context = Ctx, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_transient_error, Beat),
    case Beat#mg_machine_lifecycle_transient_error.retry_action of
        {wait, Timeout, _} ->
            {warning, {"transient error ~p during ~p, retrying in ~p msec", [Ctx, Reason, Timeout]}, Context};
        finish ->
            {warning, {"transient error ~p during ~p, retires exhausted", [Ctx, Reason]}, Context}
    end;
format_beat(#mg_timer_lifecycle_rescheduled{target_timestamp = TS, attempt = Attempt} = Beat) ->
    Context = ?beat_to_meta(mg_timer_lifecycle_rescheduled, Beat),
    {info, {"machine rescheduled to ~s, attempt ~p", [format_timestamp(TS), Attempt]}, Context};
format_beat(#mg_timer_lifecycle_rescheduling_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_timer_lifecycle_rescheduling_error, Beat),
    {info, {"machine rescheduling failed ~p", [Reason]}, Context};

format_beat({consuela, Beat = {Producer, _}}) ->
    {Level, Format, Context} = format_consuela_beat(Beat),
    {Level, Format, [{consuela_producer, Producer} | Context]};

format_beat(_Beat) ->
    undefined.

%% consuela
-spec format_consuela_beat(mg_consuela_pulse_adapter:beat()) ->
    log_msg() | undefined.

%% consul client
format_consuela_beat({client, {request, Request = {Method, Url, _Headers, Body}}}) ->
    _ = erlang:put({?MODULE, consuela_request}, Request),
    {debug, {"consul request: ~s ~s ~p", [Method, Url, Body]}, [
        {mg_pulse_event_id, consuela_client_request}
    ]};
format_consuela_beat({client, {result, Response = {ok, Status, _Headers, _Body}}}) ->
    {Method, Url, _, Body} = erlang:get({?MODULE, consuela_request}),
    Level = case Status of
        S when S < 400 -> debug;
        S when S < 500 -> info;
        _              -> warning
    end,
    {Level, {"consul response: ~p for: ~s ~s ~p", [Response, Method, Url, Body]}, [
        {mg_pulse_event_id, consuela_client_response},
        {status, Status}
    ]};
format_consuela_beat({client, {result, Error = {error, Reason}}}) ->
    {Method, Url, _, Body} = erlang:get({?MODULE, consuela_request}),
    {warning, {"consul request failed: ~p for: ~s ~s ~p", [Error, Method, Url, Body]}, [
        {mg_pulse_event_id, consuela_client_request_failed},
        {error, [{reason, genlib:print(Reason, 500)}]}
    ]};

%% registry
format_consuela_beat({registry_server, {{register, {Name, Pid}}, Status}}) ->
    case Status of
        started ->
            {debug, {"registering ~p as ~p ...", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_registration_started}
            ]};
        {finished, ok} ->
            {debug, {"registered ~p as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_registration_succeeded}
            ]};
        {finished, Error} ->
            {info, {"did not register ~p as ~p: ~p", [Pid, Name, Error]}, [
                {mg_pulse_event_id, consuela_registration_did_not_succeed}
            ]};
        {failed, Reason} ->
            {warning, {"failed to register ~p as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_registration_failed},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end;
format_consuela_beat({registry_server, {{unregister, Reg}, Status}}) ->
    {Name, Pid} = case Reg of
        {_, N, P} -> {N, P};
        {N, P}    -> {N, P}
    end,
    case Status of
        started ->
            {debug, {"unregistering ~p known as ~p ...", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_unregistration_started}
            ]};
        {finished, ok} ->
            {debug, {"unregistered ~p known as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_unregistration_succeeded}
            ]};
        {finished, Error} ->
            {info, {"did not unregister ~p known as ~p: ~p", [Pid, Name, Error]}, [
                {mg_pulse_event_id, consuela_registration_did_not_succeed}
            ]};
        {failed, Reason} ->
            {warning, {"failed to unregister ~p known as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_unregistration_failed},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end;

%% session keeper
format_consuela_beat({session_keeper, {session, {renewal, Status}}}) ->
    case Status of
        {succeeded, Session, Deadline} ->
            {info, {"session renewal succeeded: ~p", [Session]}, [
                {mg_pulse_event_id, consuela_session_renewal_succeeded},
                {time_left, Deadline - os:system_time(second)}
            ]};
        {failed, Reason} ->
            {error, {"session renewal failed", []}, [
                {mg_pulse_event_id, consuela_session_renewal_failed},
                {error, [
                    {reason, genlib:print(Reason, 500)}
                ]}
            ]}
    end;
format_consuela_beat({session_keeper, {session, expired}}) ->
    {error, {"session expired", []}, [{mg_pulse_event_id, consuela_session_expired}]};
format_consuela_beat({session_keeper, {session, destroyed}}) ->
    {info, {"session destroyed", []}, [{mg_pulse_event_id, consuela_session_destroyed}]};

%% zombie reaper
format_consuela_beat({zombie_reaper, {{zombie, {Rid, Name, Pid}}, Status}}) ->
    {Level, Format, Context} = case Status of
        enqueued ->
            {info, {"enqueued zombie registration ~p as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_zombie_enqueued}
            ]};
        {reaping, succeeded} ->
            {info, {"reaped zombie registration ~p as ~p", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_zombie_reaped}
            ]};
        {reaping, {failed, Reason}} ->
            {warning, {"reaped zombie registration ~p as ~p failed", [Pid, Name]}, [
                {mg_pulse_event_id, consuela_zombie_failed},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end,
    {Level, Format, [{registration_id, Rid} | Context]};

%% leader sup
format_consuela_beat({leader, {{leader, Name}, Status}}) ->
    case Status of
        {started, Pid} ->
            {info, {"leader ~p started", [Name]}, [
                {mg_pulse_event_id, consuela_leader_sup_started},
                {leader_pid, Pid}
            ]};
        {down, Pid, Reason} ->
            {info, {"leader ~p looks down from here", [Name]}, [
                {mg_pulse_event_id, consuela_leader_sup_down},
                {leader_pid, Pid},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end;
format_consuela_beat({leader, {{warden, Name}, {started, Pid}}}) ->
    {info, {"leader ~p warden started", [Name]}, [
        {mg_pulse_event_id, consuela_leader_warden_started},
        {warden_pid, Pid}
    ]};
format_consuela_beat({leader, {{warden, Name}, {stopped, Pid}}}) ->
    {info, {"leader ~p warden stopped", [Name]}, [
        {mg_pulse_event_id, consuela_leader_warden_stopped},
        {warden_pid, Pid}
    ]};

%% discovery
format_consuela_beat({discovery_server, {discovery, Status}}) ->
    case Status of
        started ->
            {debug, {"discovery started ...", []}, [
                {mg_pulse_event_id, consuela_discovery_started}
            ]};
        {succeeded, Nodes} ->
            {info, {"discovery succeeded, found ~p nodes: ~p", [length(Nodes), Nodes]}, [
                {mg_pulse_event_id, consuela_discovery_succeeded}
            ]};
        {failed, Reason} ->
            {error, {"discovery failed", []}, [
                {mg_pulse_event_id, consuela_discovery_failed},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end;
format_consuela_beat({discovery_server, {{connect, Node}, Status}}) ->
    case Status of
        started ->
            {debug, {"trying to connect to ~p ...", [Node]}, [
                {mg_pulse_event_id, consuela_distnode_connect_started}
            ]};
        {finished, true} ->
            {info, {"connection to ~p estabilished", [Node]}, [
                {mg_pulse_event_id, consuela_distnode_connect_succeeded}
            ]};
        {finished, false} ->
            {error, {"connect to ~p did not succeed", [Node]}, [
                {mg_pulse_event_id, consuela_distnode_connect_failed}
            ]}
    end;
format_consuela_beat({discovery_server, {{node, Node}, Status}}) ->
    case Status of
        up ->
            {info, {"~p is online now", [Node]}, [
                {mg_pulse_event_id, consuela_distnode_online}
            ]};
        {down, Reason} ->
            {error, {"~p gone offline", [Node]}, [
                {mg_pulse_event_id, consuela_distnode_offline},
                {error, [{reason, genlib:print(Reason, 500)}]}
            ]}
    end;

%% presence
format_consuela_beat({presence_session, {{presence, Name}, Status}}) ->
    case Status of
        started ->
            {info, {"started '~s' presence session", [Name]}, [
                {mg_pulse_event_id, consuela_presence_session_started}
            ]};
        {stopped, Reason} ->
            {info, {"stoped '~s' presence session: ~p", [Name, Reason]}, [
                {mg_pulse_event_id, consuela_presence_session_stopped}
            ]}
    end;

%% kinda generic beats
format_consuela_beat({_Producer, {{deadline_call, Deadline, Call}, Status}}) ->
    TimeLeft = Deadline - erlang:monotonic_time(millisecond),
    case Status of
        accepted ->
            {debug, {"accepted call ~p with ~p ms time left", [Call, TimeLeft]}, [
                {mg_pulse_event_id, consuela_deadline_call_accepted},
                {time_left, TimeLeft}
            ]};
        rejected ->
            {warning, {"rejected stale call ~p (~p ms late)", [Call, -TimeLeft]}, [
                {mg_pulse_event_id, consuela_deadline_call_rejected},
                {time_left, TimeLeft}
            ]}
    end;
format_consuela_beat({_Producer, {{timer, TRef}, Status}}) ->
    case Status of
        {started, Timeout} ->
            {debug, {"timer ~p armed to fire after ~p ms", [TRef, Timeout]}, [
                {mg_pulse_event_id, consuela_timer_started},
                {timeout, Timeout}
            ]};
        {started, Msg, Timeout} ->
            {debug, {"timer ~p armed to fire ~p after ~p ms", [TRef, Msg, Timeout]}, [
                {mg_pulse_event_id, consuela_timer_started},
                {timeout, Timeout}
            ]};
        fired ->
            {debug, {"timer ~p fired", [TRef]}, [{mg_pulse_event_id, consuela_timer_fired}]};
        reset ->
            {debug, {"timer ~p reset", [TRef]}, [{mg_pulse_event_id, consuela_timer_reset}]}
    end;
format_consuela_beat({_Producer, {{monitor, MRef}, Status}}) ->
    case Status of
        set ->
            {debug, {"monitor ~p set", [MRef]}, [{mg_pulse_event_id, consuela_monitor_set}]};
        fired ->
            {debug, {"monitor ~p fired", [MRef]}, [{mg_pulse_event_id, consuela_monitor_fired}]}
    end;
format_consuela_beat({_Producer, {unexpected, {Type, Message}}}) ->
    case Type of
        {call, From} ->
            {warning, {"received unexpected call from ~p: ~p", [From, Message]}, [
                {mg_pulse_event_id, consuela_unexpected_call}
            ]};
        cast ->
            {warning, {"received unexpected cast: ~p", [Message]}, [
                {mg_pulse_event_id, consuela_unexpected_cast}
            ]};
        info ->
            {warning, {"received unexpected info: ~p", [Message]}, [
                {mg_pulse_event_id, consuela_unexpected_info}
            ]}
    end;
format_consuela_beat({_Producer, Beat}) ->
    {warning, {"unknown or mishandled consuela beat: ~p", [Beat]}, []}.

-spec extract_meta(atom(), any()) ->
    [meta()] | meta().
extract_meta(_Name, undefined) ->
    [];
extract_meta(request_context, null) ->
    [];
extract_meta(request_context, ReqCtx) ->
    #{rpc_id := RPCID} = mg_woody_api_utils:opaque_to_woody_context(ReqCtx),
    extract_meta(rpc_id, RPCID);
extract_meta(rpc_id, RPCID) ->
    maps:to_list(RPCID);
extract_meta(deadline, Deadline) when is_integer(Deadline) ->
    {deadline, format_timestamp(Deadline div 1000)};  % Deadline measured in millisecond
extract_meta(target_timestamp, Timestamp) ->
    {target_timestamp, format_timestamp(Timestamp)};
extract_meta(exception, {Class, Reason, StackStrace}) ->
    [
        {error, [
            {class, genlib:to_binary(Class)},
            {reason, genlib:format(Reason)},
            {stack_trace, genlib_format:format_stacktrace(StackStrace)}
        ]}
    ];
extract_meta(retry_action, {wait, Timeout, NextStrategy}) ->
    [
        {wait_timeout, Timeout},
        {next_retry_strategy, genlib:format(NextStrategy)}
    ];
extract_meta(retry_action, _Other) ->
    [];
extract_meta(machine_ref, {id, MachineID}) ->
    {machine_id, MachineID};
extract_meta(machine_ref, {tag, MachineTag}) ->
    {machine_tag, MachineTag};
extract_meta(namespace, NS) ->
    {machine_ns, NS};
extract_meta(Name, Value) ->
    {Name, Value}.

-spec extract_woody_meta(woody_event_handler:event_meta()) ->
    meta().
extract_woody_meta(#{role := server} = Meta) ->
    [{'rpc.server', Meta}];
extract_woody_meta(#{role := client} = Meta) ->
    [{'rpc.client', Meta}];
extract_woody_meta(Meta) ->
    [{rpc, Meta}].

-spec format_timestamp(genlib_time:ts()) ->
    binary().
format_timestamp(TS) ->
    genlib_format:format_timestamp_iso8601(TS).
