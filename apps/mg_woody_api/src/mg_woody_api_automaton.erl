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

-module(mg_woody_api_automaton).

%% API
-export_type([options/0]).
-export([handler/1]).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: #{mg:ns() => mg_events_machine:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/automaton", {{mg_proto_state_processing_thrift, 'Automaton'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('Start', [NS, ID_, Args], WoodyContext, Options) ->
    ID = unpack(id, ID_),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    Deadline = mg_utils:default_deadline(),
    ok = mg_woody_api_utils:handle_safe_with_retry(
            {id, ID}, ReqCtx,
            fun() ->
                mg_events_machine:start(
                    get_ns_options(NS, Options), ID, unpack(args, Args), ReqCtx, Deadline
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, ok};

handle_function('Repair', [MachineDesc, Args], WoodyContext, Options) ->
    Deadline = mg_utils:default_deadline(),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    ok = mg_woody_api_utils:handle_safe_with_retry(
            Ref, ReqCtx,
            fun() ->
                mg_events_machine:repair(
                    get_ns_options(NS, Options), Ref, unpack(args, Args), Range, ReqCtx, Deadline
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, ok};

handle_function('SimpleRepair', [NS, Ref_], WoodyContext, Options) ->
    Deadline = mg_utils:default_deadline(),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    Ref = unpack(ref, Ref_),
    ok = mg_woody_api_utils:handle_safe_with_retry(
            Ref, ReqCtx,
            fun() ->
                mg_events_machine:simple_repair(
                    get_ns_options(NS, Options), Ref, ReqCtx, Deadline
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, ok};

handle_function('Call', [MachineDesc, Args], WoodyContext, Options) ->
    Deadline = mg_utils:default_deadline(),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Response =
        mg_woody_api_utils:handle_safe_with_retry(
            Ref, ReqCtx,
            % блин, как же в эрланге не хватает каррирования... :-\
            fun() ->
                mg_events_machine:call(
                    get_ns_options(NS, Options), Ref, unpack(args, Args), Range, ReqCtx, Deadline
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, pack(call_response, Response)};

handle_function('GetMachine', [MachineDesc], WoodyContext, Options) ->
    Deadline = mg_utils:default_deadline(),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    History =
        mg_woody_api_utils:handle_safe_with_retry(
            Ref, mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
            fun() ->
                mg_events_machine:get_machine(
                    get_ns_options(NS, Options), Ref, Range
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, pack(machine, History)};

handle_function('Remove', [NS, ID_], WoodyContext, Options) ->
    ID = unpack(id, ID_),
    Deadline = mg_utils:default_deadline(),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    ok = mg_woody_api_utils:handle_safe_with_retry(
            ID, ReqCtx,
            fun() ->
                mg_events_machine:remove(
                    get_ns_options(NS, Options), ID, ReqCtx, Deadline
                )
            end,
            Deadline, logger(NS, Options)
        ),
    {ok, ok}.

%%
%% local
%%
-spec get_ns_options(mg:ns(), options()) ->
    mg_events_machine:options().
get_ns_options(Namespace, Options) ->
    try
        maps:get(Namespace, Options)
    catch
        error:{badkey, Namespace} ->
            throw({logic, namespace_not_found})
    end.

-spec logger(mg:ns(), options()) ->
    mg_machine_logger:handler().
logger(NS, Options) ->
    case maps:get(NS, Options, undefined) of
        undefined                          -> undefined;
        #{machines := #{logger := Logger}} -> Logger
    end.
