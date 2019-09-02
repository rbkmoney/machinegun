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

-module(mg_events_machine_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("ct_helper.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([continuation_repair_test/1]).

%% mg_events_machine handler
-behaviour(mg_events_machine).
-export_type([options/0]).
-export([process_signal/4, process_call/4]).

%% mg_events_sink handler
-behaviour(mg_events_sink).
-export([add_events/6]).

%% Pulse
-export([handle_beat/2]).

%% Internal types

-type call() :: term().
-type signal() :: mg_events_machine:signal().
-type machine() :: mg_events_machine:machine().
-type signal_result() :: mg_events_machine:signal_result().
-type call_result() :: mg_events_machine:call_result().
-type action() :: mg_events_machine:complex_action().
-type event() :: term().
-type aux_state() :: term().
-type req_ctx() :: mg:request_context().
-type deadline() :: mg_deadline:deadline().

-type options() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun((call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}),
    sink_handler => fun(([event()]) -> ok)
}.

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%% Common test handlers

-spec all() ->
    [test_name()].
all() ->
    [
       continuation_repair_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec continuation_repair_test(config()) -> any().
continuation_repair_test(_C) ->
    NS = <<"test">>,
    MachineID = <<"machine">>,
    TestRunner = self(),
    Options = #{
        signal_handler => fun
            ({init, <<>>}, AuxState, []) -> {AuxState, [1], #{}};
            ({repair, <<>>}, AuxState, [1, 2]) -> {AuxState, [3], #{}}
        end,
        call_handler => fun
            (raise, AuxState, [1]) -> {ok, AuxState, [2], #{}}
        end,
        sink_handler => fun
            ([2]) -> erlang:error(test_error);
            (Events) -> TestRunner ! {sink_events, Events}, ok
        end
    },
    _Pid = start_automaton(Options, NS),
    ok = start(Options, NS, MachineID, <<>>),
    ?assertReceive({sink_events, [1]}),
    ?assertException(throw, {logic, machine_failed}, call(Options, NS, MachineID, raise)),
    ok = repair(Options, NS, MachineID, <<>>),
    ?assertReceive({sink_events, [2, 3]}),
    ?assertEqual([1, 2, 3], get_history(Options, NS, MachineID)).

%% Processor handlers

-spec process_signal(options(), req_ctx(), deadline(), mg_events_machine:signal_args()) -> signal_result().
process_signal(Options, _ReqCtx, _Deadline, {EncodedSignal, Machine}) ->
    Handler = maps:get(signal_handler, Options, fun dummy_signal_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Signal = decode_signal(EncodedSignal),
    ct:pal("call signal handler ~p with [~p, ~p, ~p]", [Handler, Signal, AuxState, History]),
    {NewAuxState, NewEvents, ComplexAction} = Handler(Signal, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {StateChange, ComplexAction}.

-spec process_call(options(), req_ctx(), deadline(), mg_events_machine:call_args()) -> call_result().
process_call(Options, _ReqCtx, _Deadline, {EncodedCall, Machine}) ->
    Handler = maps:get(call_handler, Options, fun dummy_call_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Call = decode(EncodedCall),
    ct:pal("call call handler ~p with [~p, ~p, ~p]", [Handler, Call, AuxState, History]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Call, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {encode(Result), StateChange, ComplexAction}.

-spec add_events(options(), mg:ns(), mg:id(), [event()], req_ctx(), deadline()) ->
    ok.
add_events(Options, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Options, fun dummy_sink_handler/1),
    ct:pal("call sink handler ~p with [~p]", [Handler, Events]),
    ok = Handler(decode_events(Events)).

-spec dummy_signal_handler(signal(), aux_state(), [event()]) ->
    {aux_state(), [event()], action()}.
dummy_signal_handler(_Signal, AuxState, _Events) ->
    {AuxState, [], #{}}.

-spec dummy_call_handler(signal(), aux_state(), [event()]) ->
    {aux_state(), [event()], action()}.
dummy_call_handler(_Signal, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_sink_handler([event()]) ->
    ok.
dummy_sink_handler(_Events) ->
    ok.

%% Utils

-spec start_automaton(options(), mg:ns()) ->
    pid().
start_automaton(Options, NS) ->
    mg_utils:throw_if_error(mg_events_machine:start_link(events_machine_options(Options, NS))).

-spec events_machine_options(options(), mg:ns()) ->
    mg_events_machine:options().
events_machine_options(Options, NS) ->
    Scheduler = #{registry => mg_procreg_gproc, interval => 100},
    #{
        namespace => NS,
        processor => {?MODULE, Options},
        tagging => #{
            namespace => <<NS/binary, "_tags">>,
            storage => mg_storage_memory,
            worker => #{
                registry => mg_procreg_gproc
            },
            pulse => ?MODULE,
            retries => #{}
        },
        machines => #{
            namespace => NS,
            storage => mg_storage_memory,
            worker => #{
                registry => mg_procreg_gproc
            },
            pulse => ?MODULE,
            schedulers => #{
                timers         => Scheduler,
                timers_retries => Scheduler,
                overseer       => Scheduler
            }
        },
        events_storage => mg_storage_memory,
        event_sinks => [
            {?MODULE, Options}
        ],
        pulse => ?MODULE,
        default_processing_timeout => timer:seconds(10)
    }.

-spec start(options(), mg:ns(), mg:id(), term()) ->
    ok.
start(Options, NS, MachineID, Args) ->
    Deadline = mg_deadline:from_timeout(3000),
    MgOptions = events_machine_options(Options, NS),
    mg_events_machine:start(MgOptions, MachineID, encode(Args), <<>>, Deadline).

-spec call(options(), mg:ns(), mg:id(), term()) ->
    term().
call(Options, NS, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_deadline:from_timeout(3000),
    MgOptions = events_machine_options(Options, NS),
    mg_events_machine:call(MgOptions, {id, MachineID}, encode(Args), HRange, <<>>, Deadline).

-spec repair(options(), mg:ns(), mg:id(), term()) ->
    ok.
repair(Options, NS, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_deadline:from_timeout(3000),
    MgOptions = events_machine_options(Options, NS),
    mg_events_machine:repair(MgOptions, {id, MachineID}, encode(Args), HRange, <<>>, Deadline).

-spec get_history(options(), mg:ns(), mg:id()) ->
    ok.
get_history(Options, NS, MachineID) ->
    HRange = {undefined, undefined, forward},
    MgOptions = events_machine_options(Options, NS),
    Machine = mg_events_machine:get_machine(MgOptions, {id, MachineID}, HRange),
    {_AuxState, History} = decode_machine(Machine),
    History.

%% Codecs

-spec decode_machine(machine()) ->
    {aux_state(), History :: [event()]}.
decode_machine(#{aux_state := EncodedAuxState, history := EncodedHistory}) ->
    {decode_aux_state(EncodedAuxState), decode_events(EncodedHistory)}.

-spec decode_aux_state(mg_events_machine:aux_state()) ->
    aux_state().
decode_aux_state({#{format_version := 1}, EncodedAuxState}) ->
    decode(EncodedAuxState);
decode_aux_state({#{}, <<>>}) ->
    <<>>.

-spec decode_events([mg_events:event()]) ->
    [event()].
decode_events(Events) ->
    [
        decode(EncodedEvent)
        || #{body := {#{}, EncodedEvent}} <- Events
    ].

-spec decode_signal(signal()) ->
    signal().
decode_signal(timeout) ->
    timeout;
decode_signal({init, Args}) ->
    {init, decode(Args)};
decode_signal({repair, Args}) ->
    {repair, decode(Args)}.

-spec encode(term()) ->
    binary().
encode(Value) ->
    erlang:term_to_binary(Value).

-spec decode(binary()) ->
    term().
decode(Value) ->
    erlang:binary_to_term(Value, [safe]).

%% Pulse handler

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
