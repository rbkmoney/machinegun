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
    lists:flatten([
        extract_meta(FiledName, Value) ||
        {FiledName, Value} <- lists:zip(
            record_info(fields, RecordName),
            erlang:tl(erlang:tuple_to_list(Record))
        )
    ])
).

-spec format_beat(beat()) ->
    log_msg() | undefined.
format_beat(#woody_request_handle_error{exception = {_, Reason, _}, error_reaction = Reaction} = Beat) ->
    Context = ?beat_to_meta(woody_request_handle_error, Beat),
    LogLevel = case Reason of
        {logic, _Details} ->
            % бизнес ошибки это не warning
            info;
        _OtherReason ->
            warning
    end,
    case Reaction of
        {retry_in, Timeout, _NextStrategy} ->
            {LogLevel, {"request handling failed ~p, retrying in ~p msec", [Reason, Timeout]}, Context};
        _OtherReaction ->
            {LogLevel, {"request handling failed ~p", [Reason]}, Context}
    end;
format_beat(#woody_internal_event{event = Event, rpc_id = RPCID, event_meta = EventMeta}) ->
    WoodyMetaFields = [event, service, function, type, metadata, url, deadline],
    {Level, Msg, Meta} = woody_event_handler:format_event_and_meta(Event, EventMeta, RPCID, WoodyMetaFields),
    {Level, Msg, maps:to_list(Meta)};
format_beat(#mg_scheduler_error{tag = Tag, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_scheduler_error, Beat),
    {warning, {"sheduler task ~p failed ~p", [Tag, Reason]}, Context};
format_beat(#mg_machine_process_transient_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_process_transient_error, Beat),
    case Beat#mg_machine_process_transient_error.retry_action of
        {wait, Timeout, _} ->
            {warning, {"transient error ~p, retrying in ~p msec", [Reason, Timeout]}, Context};
        finish ->
            {warning, {"transient error ~p, retires exhausted", [Reason]}, Context}
    end;
format_beat(#mg_machine_lifecycle_failed{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_failed, Beat),
    {error, {"machine failed ~p", [Reason]}, Context};
format_beat(#mg_machine_lifecycle_loading_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_loading_error, Beat),
    {error, {"loading failed ~p", [Reason]}, Context};
format_beat(#mg_machine_lifecycle_committed_suicide{} = Beat) ->
    Context = ?beat_to_meta(mg_machine_lifecycle_committed_suicide, Beat),
    {info, {"machine has committed suicide", []}, Context};
format_beat(#mg_timer_lifecycle_rescheduled{target_timestamp = TS, attempt = Attempt} = Beat) ->
    Context = ?beat_to_meta(mg_timer_lifecycle_rescheduled, Beat),
    {info, {"machine rescheduled to ~s, attempt ~p", [format_timestamp(TS), Attempt]}, Context};
format_beat(#mg_timer_lifecycle_rescheduling_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_timer_lifecycle_rescheduling_error, Beat),
    {info, {"machine rescheduling failed ~p", [Reason]}, Context};
format_beat(_Beat) ->
    undefined.

-spec extract_meta(atom(), any()) ->
    [meta()] | meta().
extract_meta(_Name, undefined) ->
    [];
extract_meta(request_context, null) ->
    [];
extract_meta(request_context, ReqCtx) ->
    #{rpc_id := RPCID} = mg_woody_api_utils:opaque_to_woody_context(ReqCtx),
    maps:to_list(RPCID);
extract_meta(deadline, Deadline) when is_integer(Deadline) ->
    {deadline, format_timestamp(Deadline div 1000)};  % Deadline measured in millisecond
extract_meta(target_timestamp, Timestamp) ->
    {target_timestamp, format_timestamp(Timestamp)};
extract_meta(exception, {Class, Reason, StackStrace}) ->
    [
        {error_class, genlib:to_binary(Class)},
        {error_reason, genlib:format(Reason)},
        {error_stack_trace, genlib_format:format_stacktrace(StackStrace)}
    ];
extract_meta(retry_action, {wait, Timeout, NextStrategy}) ->
    [
        {wait_timeout, Timeout},
        {next_retry_strategy, genlib:format(NextStrategy)}
    ];
extract_meta(retry_action, _Other) ->
    [];
extract_meta(error_reaction, {retry_in, Timeout, NextStrategy}) ->
    [
        {wait_timeout, Timeout},
        {next_retry_strategy, genlib:format(NextStrategy)}
    ];
extract_meta(error_reaction, _Other) ->
    [];
extract_meta(machine_ref, {id, MachineID}) ->
    {machine_id, MachineID};
extract_meta(machine_ref, {tag, MachineTag}) ->
    {machine_tag, MachineTag};
extract_meta(namespace, NS) ->
    {machine_ns, NS};
extract_meta(Name, Value) when is_number(Value) orelse is_binary(Value) ->
    {Name, Value};
extract_meta(Name, Value) ->
    {Name, genlib:format(Value)}.

-spec format_timestamp(genlib_time:ts()) ->
    binary().
format_timestamp(TS) ->
    genlib_format:format_timestamp_iso8601(TS).
