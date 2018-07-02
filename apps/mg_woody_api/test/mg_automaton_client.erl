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

-module(mg_automaton_client).

%% API
-export_type([options/0]).
-export([start        /3]).
-export([start        /4]).
-export([repair       /3]).
-export([repair       /4]).
-export([simple_repair/2]).
-export([simple_repair/3]).
-export([remove       /2]).
-export([remove       /3]).
-export([call         /3]).
-export([call         /4]).
-export([get_machine  /3]).
-export([get_machine  /4]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: #{
    url := URL::string(),
    ns  := mg:ns(),
    retry_strategy := genlib_retry:strategy() | undefined,
    transport_opts => woody_client_thrift_http_transport:options()
}.

-spec start(options(), mg:id(), mg_events_machine:args()) -> ok.
start(Options, ID, Args) ->
    start(Options, ID, Args, mg_utils:default_deadline()).

-spec start(options(), mg:id(), mg_events_machine:args(), mg_utils:deadline()) -> ok.
start(#{ns := NS} = Options, ID, Args, Deadline) ->
    ok = call_service(Options, 'Start', [pack(ns, NS), pack(id, ID), pack(args, Args)], Deadline).

-spec repair(options(), mg_events_machine:ref(), mg_events_machine:args()) -> ok.
repair(Options, Ref, Args) ->
    repair(Options, Ref, Args, mg_utils:default_deadline()).

-spec repair(options(), mg_events_machine:ref(), mg_events_machine:args(), mg_utils:deadline()) -> ok.
repair(#{ns := NS} = Options, Ref, Args, Deadline) ->
    ok = call_service(Options, 'Repair', [machine_desc(NS, Ref), pack(args, Args)], Deadline).

-spec simple_repair(options(), mg_events_machine:ref()) -> ok.
simple_repair(Options, Ref) ->
    simple_repair(Options, Ref, mg_utils:default_deadline()).

-spec simple_repair(options(), mg_events_machine:ref(), mg_utils:deadline()) -> ok.
simple_repair(#{ns := NS} = Options, Ref, Deadline) ->
    ok = call_service(Options, 'SimpleRepair', [pack(ns, NS), pack(ref, Ref)], Deadline).

-spec remove(options(), mg:id()) -> ok.
remove(Options, ID) ->
    remove(Options, ID, mg_utils:default_deadline()).

-spec remove(options(), mg:id(), mg_utils:deadline()) -> ok.
remove(#{ns := NS} = Options, ID, Deadline) ->
    ok = call_service(Options, 'Remove', [pack(ns, NS), pack(id, ID)], Deadline).

-spec call(options(), mg_events_machine:ref(), mg_events_machine:args()) -> mg:call_resp().
call(Options, Ref, Args) ->
    call(Options, Ref, Args, mg_utils:default_deadline()).

-spec call(options(), mg_events_machine:ref(), mg_events_machine:args(), mg_utils:deadline()) ->
    mg:call_resp().
call(#{ns := NS} = Options, Ref, Args, Deadline) ->
    unpack(
        call_response,
        call_service(Options, 'Call', [machine_desc(NS, Ref), pack(args, Args)], Deadline)
    ).

-spec get_machine(options(), mg_events_machine:ref(), mg_events:history_range()) ->
    mg_events_machine:machine().
get_machine(Options, Ref, Range) ->
    get_machine(Options, Ref, Range, mg_utils:default_deadline()).

-spec get_machine(options(), mg_events_machine:ref(), mg_events:history_range(), mg_utils:deadline()) ->
    mg_events_machine:machine().
get_machine(#{ns := NS} = Options, Ref, Range, Deadline) ->
    unpack(
        machine,
        call_service(Options, 'GetMachine', [machine_desc(NS, Ref, Range)], Deadline)
    ).

%%
%% local
%%
-spec machine_desc(mg:ns(), mg_events_machine:ref()) ->
    _.
machine_desc(NS, Ref) ->
    machine_desc(NS, Ref, {undefined, undefined, forward}).

-spec machine_desc(mg:ns(), mg_events_machine:ref(), mg_events:history_range()) ->
    _.
machine_desc(NS, Ref, HRange) ->
    pack(machine_descriptor, {NS, Ref, HRange}).

-spec call_service(options(), atom(), [_Arg], mg_utils:deadline()) -> any().
call_service(#{retry_strategy := undefined} = Options, Function, Args, Deadline) ->
    WR = woody_call(Options, Function, Args, Deadline),
    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    end;
call_service(#{retry_strategy := Strategy} = Options, Function, Args, Deadline) ->
    WR = woody_call(Options, Function, Args, Deadline),

    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NewStrategy} ->
                    ok = timer:sleep(Timeout),
                    call_service(Options#{retry_strategy := NewStrategy}, Function, Args, Deadline);
                finish ->
                    erlang:throw(Exception)
            end
    end.

-spec woody_call(options(), atom(), [_Arg], mg_utils:deadline()) -> any().
woody_call(#{url := BaseURL} = Options, Function, Args, Deadline) ->
    TransportOptions = maps:get(transport_opts, Options, []),
    Context = mg_woody_api_utils:set_deadline(Deadline, woody_context:new()),
    woody_client:call(
            {{mg_proto_state_processing_thrift, 'Automaton'}, Function, Args},
            #{
                url            => BaseURL ++ "/v1/automaton",
                event_handler  => {mg_woody_api_event_handler, undefined},
                transport_opts => TransportOptions
            },
            Context
    ).
