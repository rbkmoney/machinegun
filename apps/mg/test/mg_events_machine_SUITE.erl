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
-export([get_events_test/1]).
-export([continuation_repair_test/1]).

%% mg_events_machine handler
-behaviour(mg_events_machine).
-export_type([options/0]).
-export([process_signal/4, process_call/4, process_repair/4]).

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
-type repair_result() :: mg_events_machine:repair_result().
-type action() :: mg_events_machine:complex_action().
-type event() :: term().
-type history() :: [{mg_events:id(), event()}].
-type aux_state() :: term().
-type req_ctx() :: mg:request_context().
-type deadline() :: mg_deadline:deadline().

-type options() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun((call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}),
    sink_handler => fun((history()) -> ok)
}.

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%% Common test handlers

-spec all() ->
    [test_name()].
all() ->
    [
       get_events_test,
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

-spec get_events_test(config()) -> any().
get_events_test(_C) ->
    NS = <<"events">>,
    ProcessorOpts = #{
        signal_handler => fun ({init, <<>>}, AuxState, []) ->
            {AuxState, [], #{}}
        end,
        call_handler => fun ({emit, N}, AuxState, _) ->
            {ok, AuxState, [{} || _ <- lists:seq(1, N)], #{}}
        end,
        sink_handler => fun (_Events) ->
            % NOTE
            % Inducing random delay so that readers would hit delayed
            % events reading logic more frequently.
            Spread = 100,
            Baseline = 20,
            timer:sleep(rand:uniform(Spread) + Baseline)
        end
    },
    N = 10,
    ok = lists:foreach(
        fun (I) ->
            ct:pal("Complexity = ~p", [I]),
            BaseOpts = #{
                pulse => {?MODULE, quiet},
                event_stash_size => rand:uniform(2 * I)
            },
            StorageOpts = #{
                batching => #{concurrency_limit => rand:uniform(5 * I)}
            },
            Options = events_machine_options(BaseOpts, StorageOpts, ProcessorOpts, NS),
            ct:pal("Options = ~p", [Options]),
            {Pid, Options} = start_automaton(Options),
            MachineID = genlib:to_binary(I),
            ok = start(Options, MachineID, <<>>),
            NEmits = rand:uniform(2 * I),
            NGets = rand:uniform(10 * I),
            Delay = rand:uniform(400) + 100,
            Size = rand:uniform(5 * I),
            THRange = t_history_range(),
            _ = genlib_pmap:map(
                fun
                    (emit) ->
                        ok = timer:sleep(rand:uniform(Delay)),
                        ok = call(Options, MachineID, {emit, rand:uniform(2 * I)});
                    (get) ->
                        ok = timer:sleep(rand:uniform(Delay)),
                        {ok, HRange} = proper_gen:pick(THRange, Size),
                        History = get_history(Options, MachineID, HRange),
                        assert_history_consistent(History, HRange)
                end,
                lists:duplicate(NEmits, emit) ++
                lists:duplicate(NGets, get)
            ),
            ok = stop_automaton(Pid)
        end,
        lists:seq(1, N)
    ).

-spec assert_history_consistent
    (history(), mg_events:history_range()) -> ok;
    (history(), _Assertion :: {from | limit | direction, _}) -> ok.
assert_history_consistent(History, HRange = {From, Limit, Direction}) ->
    Result = lists:all(fun (Assert) -> assert_history_consistent(History, Assert) end, [
        {from, {From, Direction}},
        {limit, Limit},
        {direction, Direction}
    ]),
    ?assertMatch({true, _, _}, {Result, History, HRange});
assert_history_consistent([{ID, _} | _], {from, {From, forward}}) when From /= undefined ->
    From < ID;
assert_history_consistent([{ID, _} | _], {from, {From, backward}}) when From /= undefined ->
    From > ID;
assert_history_consistent(History, {limit, Limit}) ->
    length(History) =< Limit;
assert_history_consistent(History = [{ID, _} | _], {direction, Direction}) ->
    Step = case Direction of
        forward  -> +1;
        backward -> -1
    end,
    Expected = lists:seq(ID, ID + (length(History) - 1) * Step, Step),
    Expected =:= [ID1 || {ID1, _} <- History];
assert_history_consistent(_History, _Assertion) ->
    true.

-spec t_history_range() ->
    proper_types:raw_type().
t_history_range() ->
    {t_from_event(), t_limit(), t_direction()}.

-spec t_from_event() ->
    proper_types:raw_type().
t_from_event() ->
    proper_types:frequency([
        {4, proper_types:non_neg_integer()},
        {1, proper_types:exactly(undefined)}
    ]).

-spec t_limit() ->
    proper_types:raw_type().
t_limit() ->
    proper_types:frequency([
        {4, proper_types:pos_integer()},
        {1, proper_types:exactly(undefined)}
    ]).

-spec t_direction() ->
    proper_types:raw_type().
t_direction() ->
    proper_types:oneof([
        proper_types:exactly(forward),
        proper_types:exactly(backward)
    ]).

-spec continuation_repair_test(config()) -> any().
continuation_repair_test(_C) ->
    NS = <<"test">>,
    MachineID = <<"machine">>,
    TestRunner = self(),
    ProcessorOptions = #{
        signal_handler => fun
            ({init, <<>>}, AuxState, []) -> {AuxState, [1], #{}}
        end,
        call_handler => fun
            (raise, AuxState, [1]) -> {ok, AuxState, [2], #{}}
        end,
        repair_handler => fun
            (<<>>, AuxState, [1, 2]) -> {ok, AuxState, [3], #{}}
        end,
        sink_handler => fun
            ([2]) -> erlang:error(test_error);
            (Events) -> TestRunner ! {sink_events, Events}, ok
        end
    },
    {Pid, Options} = start_automaton(ProcessorOptions, NS),
    ok = start(Options, MachineID, <<>>),
    ?assertReceive({sink_events, [1]}),
    ?assertException(throw, {logic, machine_failed}, call(Options, MachineID, raise)),
    ok = repair(Options, MachineID, <<>>),
    ?assertReceive({sink_events, [2, 3]}),
    ?assertEqual([{1, 1}, {2, 2}, {3, 3}], get_history(Options, MachineID)),
    ok = stop_automaton(Pid).

%% Processor handlers

-spec process_signal(options(), req_ctx(), deadline(), mg_events_machine:signal_args()) -> signal_result().
process_signal(Options, _ReqCtx, _Deadline, {EncodedSignal, Machine}) ->
    Handler = maps:get(signal_handler, Options, fun dummy_signal_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Signal = decode_signal(EncodedSignal),
    Events = extract_events(History),
    ct:pal("call signal handler ~p with [~p, ~p, ~p]", [Handler, Signal, AuxState, Events]),
    {NewAuxState, NewEvents, ComplexAction} = Handler(Signal, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {StateChange, ComplexAction}.

-spec process_call(options(), req_ctx(), deadline(), mg_events_machine:call_args()) -> call_result().
process_call(Options, _ReqCtx, _Deadline, {EncodedCall, Machine}) ->
    Handler = maps:get(call_handler, Options, fun dummy_call_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Call = decode(EncodedCall),
    Events = extract_events(History),
    ct:pal("call call handler ~p with [~p, ~p, ~p]", [Handler, Call, AuxState, Events]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Call, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {encode(Result), StateChange, ComplexAction}.

-spec process_repair(options(), req_ctx(), deadline(), mg_events_machine:repair_args()) -> repair_result().
process_repair(Options, _ReqCtx, _Deadline, {EncodedArgs, Machine}) ->
    Handler = maps:get(repair_handler, Options, fun dummy_repair_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Args = decode(EncodedArgs),
    Events = extract_events(History),
    ct:pal("call repair handler ~p with [~p, ~p, ~p]", [Handler, Args, AuxState, Events]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Args, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {ok, {encode(Result), StateChange, ComplexAction}}.

-spec add_events(options(), mg:ns(), mg:id(), [event()], req_ctx(), deadline()) ->
    ok.
add_events(Options, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Options, fun dummy_sink_handler/1),
    ct:pal("call sink handler ~p with [~p]", [Handler, Events]),
    ok = Handler(extract_events(decode_history(Events))).

-spec dummy_signal_handler(signal(), aux_state(), [event()]) ->
    {aux_state(), [event()], action()}.
dummy_signal_handler(_Signal, AuxState, _Events) ->
    {AuxState, [], #{}}.

-spec dummy_call_handler(call(), aux_state(), [event()]) ->
    {ok, aux_state(), [event()], action()}.
dummy_call_handler(_Call, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_repair_handler(term(), aux_state(), [event()]) ->
    {ok, aux_state(), [event()], action()}.
dummy_repair_handler(_Args, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_sink_handler([event()]) ->
    ok.
dummy_sink_handler(_Events) ->
    ok.

%% Utils

-spec start_automaton(options(), mg:ns()) ->
    pid().
start_automaton(ProcessorOptions, NS) ->
    start_automaton(events_machine_options(ProcessorOptions, NS)).

-spec start_automaton(mg_events_machine:options()) ->
    {pid(), mg_events_machine:options()}.
start_automaton(Options) ->
    {mg_utils:throw_if_error(mg_events_machine:start_link(Options)), Options}.

-spec stop_automaton(pid()) ->
    ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec events_machine_options(options(), mg:ns()) ->
    mg_events_machine:options().
events_machine_options(Options, NS) ->
    events_machine_options(#{}, #{}, Options, NS).

-spec events_machine_options(BaseOptions, StorageOptions, options(), mg:ns()) ->
    mg_events_machine:options() when
        BaseOptions :: mg_events_machine:options(),
        StorageOptions :: mg_storage:storage_options().
events_machine_options(Base, StorageOptions, ProcessorOptions, NS) ->
    Scheduler = #{},
    Options = maps:merge(
        #{
            pulse => ?MODULE,
            default_processing_timeout => timer:seconds(10),
            event_stash_size => 5,
            event_sinks => [
                {?MODULE, ProcessorOptions}
            ]
        },
        Base
    ),
    Pulse = maps:get(pulse, Options),
    Storage = {mg_storage_memory, StorageOptions},
    Options#{
        namespace => NS,
        processor => {?MODULE, ProcessorOptions},
        tagging => #{
            namespace => <<NS/binary, "_tags">>,
            storage => Storage,
            worker => #{
                registry => mg_procreg_gproc
            },
            pulse => Pulse,
            retries => #{}
        },
        machines => #{
            namespace => NS,
            storage => mg_ct_helper:build_storage(NS, Storage),
            worker => #{
                registry => mg_procreg_gproc
            },
            pulse => Pulse,
            schedulers => #{
                timers         => Scheduler,
                timers_retries => Scheduler,
                overseer       => Scheduler
            }
        },
        events_storage => mg_ct_helper:build_storage(<<NS/binary, "_sink">>, Storage)
    }.

-spec start(mg_events_machine:options(),  mg:id(), term()) ->
    ok.
start(Options, MachineID, Args) ->
    Deadline = mg_deadline:from_timeout(3000),
    mg_events_machine:start(Options, MachineID, encode(Args), <<>>, Deadline).

-spec call(mg_events_machine:options(), mg:id(), term()) ->
    term().
call(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_deadline:from_timeout(3000),
    Response = mg_events_machine:call(Options, {id, MachineID}, encode(Args), HRange, <<>>, Deadline),
    decode(Response).

-spec repair(mg_events_machine:options(), mg:id(), term()) ->
    ok.
repair(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_deadline:from_timeout(3000),
    {ok, Response} = mg_events_machine:repair(Options, {id, MachineID}, encode(Args), HRange, <<>>, Deadline),
    decode(Response).

-spec get_history(mg_events_machine:options(), mg:id()) ->
    history().
get_history(Options, MachineID) ->
    HRange = {undefined, undefined, forward},
    get_history(Options, MachineID, HRange).

-spec get_history(mg_events_machine:options(), mg:id(), mg_events:history_range()) ->
    history().
get_history(Options, MachineID, HRange) ->
    Machine = mg_events_machine:get_machine(Options, {id, MachineID}, HRange),
    {_AuxState, History} = decode_machine(Machine),
    History.

-spec extract_events(history()) ->
    [event()].
extract_events(History) ->
    [Event || {_ID, Event} <- History].

%% Codecs

-spec decode_machine(machine()) ->
    {aux_state(), History :: [event()]}.
decode_machine(#{aux_state := EncodedAuxState, history := EncodedHistory}) ->
    {decode_aux_state(EncodedAuxState), decode_history(EncodedHistory)}.

-spec decode_aux_state(mg_events_machine:aux_state()) ->
    aux_state().
decode_aux_state({#{format_version := 1}, EncodedAuxState}) ->
    decode(EncodedAuxState);
decode_aux_state({#{}, <<>>}) ->
    <<>>.

-spec decode_history([mg_events:event()]) ->
    [event()].
decode_history(Events) ->
    [
        {ID, decode(EncodedEvent)}
        || #{id := ID, body := {#{}, EncodedEvent}} <- Events
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

-include_lib("mg/include/pulse.hrl").

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat = #mg_machine_lifecycle_failed{}) ->
    ct:pal("~p", [Beat]);
handle_beat(_, Beat = #mg_machine_lifecycle_transient_error{}) ->
    ct:pal("~p", [Beat]);
handle_beat(quiet, _Beat) ->
    ok;
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
