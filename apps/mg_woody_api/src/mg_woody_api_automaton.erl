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
-type options() :: #{mg:ns() => ns_options()}.
-type ns_options() :: #{
    machine    := mg_events_machine:options(),
    modernizer => mg_events_modernizer:options()
}.

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
    Deadline = get_deadline(NS, WoodyContext, Options),
    ok = mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => {id, ID}, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:start(
                    get_machine_options(NS, Options), ID, unpack(args, Args), ReqCtx, Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, ok};

handle_function('Repair', [MachineDesc, Args], WoodyContext, Options) ->
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    ok = mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => Ref, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:repair(
                    get_machine_options(NS, Options), Ref, unpack(args, Args), Range, ReqCtx, Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, ok};

handle_function('SimpleRepair', [NS, Ref_], WoodyContext, Options) ->
    Deadline = get_deadline(NS, WoodyContext, Options),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    Ref = unpack(ref, Ref_),
    ok = mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => Ref, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:simple_repair(
                    get_machine_options(NS, Options), Ref, ReqCtx, Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, ok};

handle_function('Call', [MachineDesc, Args], WoodyContext, Options) ->
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    Response =
        mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => Ref, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:call(
                    get_machine_options(NS, Options), Ref, unpack(args, Args), Range, ReqCtx, Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, pack(call_response, Response)};

handle_function('GetMachine', [MachineDesc], WoodyContext, Options) ->
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    History =
        mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => Ref, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:get_machine(
                    get_machine_options(NS, Options), Ref, Range
                )
            end,
            pulse(NS, Options)
        ),
    {ok, pack(machine, History)};

handle_function('Remove', [NS, ID_], WoodyContext, Options) ->
    ID = unpack(id, ID_),
    Deadline = get_deadline(NS, WoodyContext, Options),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    ok = mg_woody_api_utils:handle_error(
            #{namespace => NS, machine_ref => ID, request_context => ReqCtx, deadline => Deadline},
            fun() ->
                mg_events_machine:remove(
                    get_machine_options(NS, Options), ID, ReqCtx, Deadline
                )
            end,
            pulse(NS, Options)
        ),
    {ok, ok};

handle_function('Modernize', [MachineDesc], WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Deadline = get_deadline(NS, WoodyContext, Options),
    ReqCtx = mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
    mg_woody_api_utils:handle_error(
        #{namespace => NS, machine_ref => Ref, request_context => ReqCtx, deadline => Deadline},
        fun () ->
            case get_ns_options(NS, Options) of
                #{modernizer := ModernizerOptions, machine := MachineOptions} ->
                    {ok, mg_events_modernizer:modernize_machine(
                        ModernizerOptions, MachineOptions, WoodyContext, Ref, Range
                    )};
                #{} ->
                    % TODO
                    % Тут нужно отдельное исключение конечно.
                    erlang:throw({logic, namespace_not_found})
            end
        end,
        pulse(NS, Options)
    ).

%%
%% local
%%
-spec get_machine_options(mg:ns(), options()) ->
    mg_events_machine:options().
get_machine_options(Namespace, Options) ->
    maps:get(machine, get_ns_options(Namespace, Options)).

-spec get_ns_options(mg:ns(), options()) ->
    ns_options().
get_ns_options(Namespace, Options) ->
    try
        maps:get(Namespace, Options)
    catch
        error:{badkey, Namespace} ->
            throw({logic, namespace_not_found})
    end.

-spec pulse(mg:ns(), options()) ->
    mg_pulse:handler().
pulse(Namespace, Options) ->
    try get_machine_options(Namespace, Options) of
        #{machines := #{pulse := Pulse}} ->
            Pulse
    catch throw:{logic, namespace_not_found} ->
        undefined
    end.

-spec get_deadline(mg:ns(), woody_context:ctx(), options()) ->
    mg_deadline:deadline().
get_deadline(Namespace, WoodyContext, Options) ->
    DefaultTimeout = default_processing_timeout(Namespace, Options),
    mg_woody_api_utils:get_deadline(WoodyContext, mg_deadline:from_timeout(DefaultTimeout)).

-spec default_processing_timeout(mg:ns(), options()) ->
    timeout().
default_processing_timeout(Namespace, Options) ->
    try get_machine_options(Namespace, Options) of
        #{default_processing_timeout := V} -> V
    catch
        throw:{logic, namespace_not_found} -> 0
    end.
