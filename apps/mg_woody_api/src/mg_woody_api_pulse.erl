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

-module(mg_woody_api_pulse).

-include_lib("mg/include/pulse.hrl").
-include_lib("mg_woody_api/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_pulse).
-export_type([beat/0]).
-export([handle_beat/2]).

%% logging API
-export_type([log_msg/0]).
-export_type([msg    /0]).
-export_type([meta   /0]).
-export_type([level  /0]).
-export([log/1]).

%%
%% mg_pulse handler
%%
-type beat() ::
      mg_pulse:beat()
    | #woody_request_handle_error{}
    | #woody_request_handle_retry{}.

-spec handle_beat(undefined, beat()) ->
    ok.
handle_beat(undefined, Beat) ->
    log(format_beat(Beat)).

%%
%% logging API
%%
-type log_msg() :: {level(), msg(), meta()}.
-type msg() :: expanded_msg() | string().
-type expanded_msg() :: {Format::string(), Args::list()}.
-type meta() :: [{atom(), binary() | integer()}]. % there is no such exported type in lager
-type level() :: lager:log_level().

-spec log(undefined | log_msg()) ->
    ok.
log(undefined) ->
    ok;
log({Level, Msg, Meta}) ->
    {MsgFormat, MsgArgs} = expand_msg(Msg),
    ok = lager:log(Level, [{pid, erlang:self()} | Meta], MsgFormat, MsgArgs).

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
format_beat(#woody_request_handle_error{exception = {_, {logic, _} = Reason, _}} = Beat) ->
    % бизнес ошибки это не warning
    Context = ?beat_to_meta(woody_request_handle_error, Beat),
    {info, {"request handling failed ~p", [Reason]}, Context};
format_beat(#woody_request_handle_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(woody_request_handle_error, Beat),
    {warning, {"request handling failed ~p", [Reason]}, Context};
format_beat(#mg_scheduler_error{tag = Tag, exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_scheduler_error, Beat),
    {warning, {"sheduler task ~p failed ~p", [Tag, Reason]}, Context};
format_beat(#mg_machine_process_transient_error{exception = {_, Reason, _}} = Beat) ->
    Context = ?beat_to_meta(mg_machine_process_transient_error, Beat),
    {warning, {"transient error ~p", [Reason]}, Context};
format_beat(#mg_machine_process_retry{wait_timeout = Timeout} = Beat) ->
    Context = ?beat_to_meta(mg_machine_process_retry, Beat),
    {warning, {"retrying in ~p msec", [Timeout]}, Context};
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
extract_meta(deadline, Deadline) ->
    {deadline, format_timestamp(Deadline div 1000)};  % Deadline measured in millisecond
extract_meta(target_timestamp, Timestamp) ->
    {target_timestamp, format_timestamp(Timestamp)};
extract_meta(exception, {Class, Reason, StackStrace}) ->
    [
        {error_class, genlib:to_binary(Class)},
        {error_reason, genlib:format(Reason)},
        {error_stack_trace, genlib_format:format_stacktrace(StackStrace)}
    ];
extract_meta(machine_ref, {id, MachineID}) ->
    {machine_id, MachineID};
extract_meta(machine_ref, {tag, MachineTag}) ->
    {machine_tag, MachineTag};
extract_meta(namespace, NS) ->
    {machine_ns, NS};
extract_meta(Name, Value) when is_integer(Value) orelse is_binary(Value) ->
    {Name, Value};
extract_meta(Name, Value) when is_atom(Value) orelse is_float(Value) ->
    {Name, genlib:to_binary(Value)};
extract_meta(Name, Value) ->
    {Name, genlib:format(Value)}.

-spec format_timestamp(genlib_time:ts()) ->
    binary().
format_timestamp(TS) ->
    genlib_format:format_timestamp_iso8601(TS).

-spec expand_msg(msg()) ->
    expanded_msg().
expand_msg(Msg={_, _}) ->
    Msg;
expand_msg(Str) ->
    {Str, []}.
