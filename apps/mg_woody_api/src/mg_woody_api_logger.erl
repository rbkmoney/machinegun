%%%
%%% Copyright 2017 RBKmoney
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

-module(mg_woody_api_logger).

%% mg_machine_logger handler
-behaviour(mg_machine_logger).
-export([handle_machine_logging_event/2]).

%% machine logging API
-export_type([subj   /0]).
-export_type([subj_id/0]).
-export([woody_rpc_id_to_meta/1]).

%% logging API
-export_type([log_msg/0]).
-export_type([msg    /0]).
-export_type([meta   /0]).
-export_type([level  /0]).
-export([log/1]).

%%
%% mg_machine_logger handler
%%
-type subj() :: {machine, NS} | event_sink | {machine_tags, NS}.
-type subj_id() :: _.

-spec handle_machine_logging_event(subj(), mg_machine_logger:event()) ->
    ok.
handle_machine_logging_event(Subj, Event) ->
    ok = log(format_event(Subj, Event)).

-spec format_event(subj(), mg_machine_logger:event()) ->
    log_msg().
format_event(Subj, {request_event, SubjID, ReqCtx, RequestEvent}) ->
    append_subj_info(Subj, SubjID, add_request_context(ReqCtx, format_request_event(RequestEvent)));
format_event(Subj, {machine_event, SubjID, ReqCtx, MachineEvent}) ->
    append_subj_info(Subj, SubjID, add_request_context(ReqCtx, format_machine_event(MachineEvent))).

-spec format_request_event(mg_machine_logger:request_event()) ->
    log_msg().
format_request_event({request_failed, Exception={throw, {logic, _}, _}}) ->
    % бизнес ошибки это не warning
    {info, {"request handling failed ~p", [Exception]}, []};
format_request_event({request_failed, Exception}) ->
    {warning, {"request handling failed ~p", [Exception]}, []};
format_request_event({timer_handling_failed, Exception}) ->
    {warning, {"timer handling failed ~p", [Exception]}, []};
format_request_event({timer_retry_handling_failed, Exception}) ->
    {warning, {"timer retry handling failed ~p", [Exception]}, []};
format_request_event({resuming_interrupted_failed, Exception}) ->
    {warning, {"resuming interrupted failed ~p", [Exception]}, []};
format_request_event({retrying, RetryTimeout}) ->
    {warning, {"retrying in ~p msec", [RetryTimeout]}, []}.

-spec format_machine_event(mg_machine_logger:machine_event()) ->
    log_msg().
format_machine_event({loading_failed, Exception}) ->
    {error, {"loading failed ~p", [Exception]}, []};
format_machine_event({machine_failed, Exception}) ->
    {error, {"machine failed ~p", [Exception]}, []};
format_machine_event({transient_error, Exception}) ->
    {warning, {"transient error ~p", [Exception]}, []};
format_machine_event({retrying, RetryTimeout}) ->
    {warning, {"retrying in ~p msec", [RetryTimeout]}, []};
format_machine_event({machine_resheduled, TS, Attempt}) ->
    PrettyDate = genlib_time:unixtime_to_daytime(TS),
    {warning, {"machine resheduled to ~p, attempt ~p", [PrettyDate, Attempt]}, []};
format_machine_event({machine_resheduling_failed, Exception}) ->
    {warning, {"machine resheduling failed ~p", [Exception]}, []};
format_machine_event(committed_suicide) ->
    {warning, {"machine has committed suicide", []}, []};
format_machine_event(UnknownLogEvent) ->
    {warning, {"unknown log event ~p", [UnknownLogEvent]}, []}.

%%
%% machine logging API
%%
-spec add_request_context(mg:request_context(), log_msg()) ->
    log_msg().
add_request_context(null, LogMsg) ->
    LogMsg;
add_request_context(ReqCtx, LogMsg) ->
    #{rpc_id := RPCID} = mg_woody_api_utils:opaque_to_woody_context(ReqCtx),
    add_meta(woody_rpc_id_to_meta(RPCID), LogMsg).

-spec woody_rpc_id_to_meta(woody:rpc_id()) ->
    meta().
woody_rpc_id_to_meta(RPCID) ->
    maps:to_list(RPCID).

-spec append_subj_info(subj(), subj_id() | undefined, log_msg()) ->
    log_msg().
append_subj_info({machine, NS}, ID, LogMsg) ->
    append_machine_info(NS, ID, LogMsg);
append_subj_info(event_sink, ESID, LogMsg) ->
    append_event_sink_info(ESID, LogMsg);
append_subj_info({machine_tags, NS}, Tag, LogMsg) ->
    append_machine_tags_info(NS, Tag, LogMsg).

-spec append_machine_info(mg:ns(), mg:id() | mg_events_machine:ref() | undefined, log_msg()) ->
    log_msg().
append_machine_info(NS, undefined, LogMsg) ->
    add_meta([{machine_ns, NS}], LogMsg);
append_machine_info(NS, {tag, Tag}, LogMsg) ->
    add_meta([{machine_ns, NS}, {machine_tag, Tag}], LogMsg);
append_machine_info(NS, {id, ID}, LogMsg) ->
    append_machine_info(NS, ID, LogMsg);
append_machine_info(NS, ID, LogMsg) ->
    add_meta([{machine_ns, NS}, {machine_id, ID}], LogMsg).

-spec append_event_sink_info(mg:id() | undefined, log_msg()) ->
    log_msg().
append_event_sink_info(undefined, LogMsg) ->
    LogMsg;
append_event_sink_info(ESID, LogMsg) ->
    add_meta([{event_sink_id, ESID}], LogMsg).

-spec append_machine_tags_info(mg:ns(), mg_machine_tags:tag() | undefined, log_msg()) ->
    log_msg().
append_machine_tags_info(NS, undefined, LogMsg) ->
    add_meta([{machine_ns, NS}], LogMsg);
append_machine_tags_info(NS, Tag, LogMsg) ->
    add_meta([{machine_ns, NS}, {machine_tags_id, Tag}], LogMsg).

%%
%% logging API
%%
-type log_msg() :: {level(), msg(), meta()}.
-type msg() :: expanded_msg() | string().
-type expanded_msg() :: {Format::string(), Args::list()}.
-type meta() :: [{atom(), _}]. % there is no such exported type in lager
-type level() :: lager:log_level().

-spec log(log_msg()) ->
    ok.
log({Level, Msg, Meta}) ->
    {MsgFormat, MsgArgs} = expand_msg(Msg),
    ok = lager:log(Level, [{pid, erlang:self()} | Meta], MsgFormat, MsgArgs).

-spec add_meta(meta(), log_msg()) ->
    log_msg().
add_meta(AdditionalMeta, {Level, Msg, Meta}) ->
    {Level, Msg, Meta ++ AdditionalMeta}.

-spec expand_msg(msg()) ->
    expanded_msg().
expand_msg(Msg={_, _}) ->
    Msg;
expand_msg(Str) ->
    {Str, []}.
