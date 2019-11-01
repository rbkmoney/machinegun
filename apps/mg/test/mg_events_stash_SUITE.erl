-module(mg_events_stash_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% Tests
-export([stash_enlarge_test/1]).
-export([stash_shrink_test/1]).

%% mg_events_machine handler
-behaviour(mg_events_machine).
-export([process_signal/4, process_call/4]).

%% mg_events_sink handler
-behaviour(mg_events_sink).
-export([add_events/6]).

%% Pulse
-export([handle_beat/2]).

%% Internal types

-type action() :: mg_events_machine:complex_action().
-type aux_state() :: term().
-type call() :: term().
-type call_result() :: mg_events_machine:call_result().
-type deadline() :: mg_deadline:deadline().
-type event() :: term().
-type machine() :: mg_events_machine:machine().
-type req_ctx() :: mg:request_context().
-type signal() :: mg_events_machine:signal().
-type signal_result() :: mg_events_machine:signal_result().


-type handlers() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun((call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}),
    sink_handler => fun(([event()]) -> ok)
}.
-type processor() :: {module(), handlers()}.
-type options() :: #{
    processor := processor(),
    namespace := mg:ns(),
    max_internal_events := non_neg_integer()
}.

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%% Common test handlers

-spec all() ->
    [test_name()].
all() ->
    [
       stash_enlarge_test,
       stash_shrink_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([mg]),
    {ok, StoragePid} = mg_storage_memory:start_link(#{name => ?MODULE}),
    true = erlang:unlink(StoragePid),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec stash_enlarge_test(config()) ->
    any().
stash_enlarge_test(_C) ->
    MachineID = <<"machine_1">>,
    Options0 = #{
        processor => {?MODULE, #{
            signal_handler => fun
                ({init, NewEvents}, AuxState, []) -> {AuxState, NewEvents, #{}}
            end,
            call_handler => fun
                ({emit, NewEvents}, AuxState, _) -> {ok, AuxState, NewEvents, #{}}
            end
        }},
        namespace => <<"event_stash_test">>,
        max_internal_events => 3
    },
    Pid0 = start_automaton(Options0),
    ok = start(Options0, MachineID, [1, 2]),
    ok = call(Options0, MachineID, {emit, [3, 4]}),
    ?assertEqual([1, 2, 3, 4], get_history(Options0, MachineID)),
    ok = stop_automaton(Pid0),
    Options1 = Options0#{max_internal_events => 5},
    Pid1 = start_automaton(Options1),
    ok = call(Options1, MachineID, {emit, [5, 6]}),
    ?assertEqual([1, 2, 3, 4, 5, 6], get_history(Options1, MachineID)),
    ok = stop_automaton(Pid1).

-spec stash_shrink_test(config()) ->
    any().
stash_shrink_test(_C) ->
    MachineID = <<"machine_2">>,
    Options0 = #{
        processor => {?MODULE, #{
            signal_handler => fun
                ({init, NewEvents}, AuxState, []) -> {AuxState, NewEvents, #{}}
            end,
            call_handler => fun
                ({emit, NewEvents}, AuxState, _) -> {ok, AuxState, NewEvents, #{}}
            end
        }},
        namespace => <<"event_stash_test">>,
        max_internal_events => 5
    },
    Pid0 = start_automaton(Options0),
    ok = start(Options0, MachineID, [1, 2]),
    ok = call(Options0, MachineID, {emit, [3, 4, 5]}),
    ?assertEqual([1, 2, 3, 4, 5], get_history(Options0, MachineID)),
    ok = stop_automaton(Pid0),
    Options1 = Options0#{max_internal_events => 3},
    Pid1 = start_automaton(Options1),
    ok = call(Options1, MachineID, {emit, [6, 7]}),
    ?assertEqual([1, 2, 3, 4, 5, 6, 7], get_history(Options1, MachineID)),
    ok = stop_automaton(Pid1).

%% Processor handlers

-spec process_signal(handlers(), req_ctx(), deadline(), mg_events_machine:signal_args()) -> signal_result().
process_signal(Handlers, _ReqCtx, _Deadline, {EncodedSignal, Machine}) ->
    Handler = maps:get(signal_handler, Handlers, fun dummy_signal_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Signal = decode_signal(EncodedSignal),
    ct:pal("call signal handler ~p with [~p, ~p, ~p]", [Handler, Signal, AuxState, History]),
    {NewAuxState, NewEvents, ComplexAction} = Handler(Signal, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {StateChange, ComplexAction}.

-spec process_call(handlers(), req_ctx(), deadline(), mg_events_machine:call_args()) -> call_result().
process_call(Handlers, _ReqCtx, _Deadline, {EncodedCall, Machine}) ->
    Handler = maps:get(call_handler, Handlers, fun dummy_call_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Call = decode(EncodedCall),
    ct:pal("call call handler ~p with [~p, ~p, ~p]", [Handler, Call, AuxState, History]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Call, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {encode(Result), StateChange, ComplexAction}.

-spec add_events(handlers(), mg:ns(), mg:id(), [event()], req_ctx(), deadline()) ->
    ok.
add_events(Handlers, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Handlers, fun dummy_sink_handler/1),
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

%% Pulse handler

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

%% Internal functions

-spec start_automaton(options()) ->
    pid().
start_automaton(Options) ->
    mg_utils:throw_if_error(mg_events_machine:start_link(events_machine_options(Options))).

-spec stop_automaton(pid()) ->
    ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec events_machine_options(options()) ->
    mg_events_machine:options().
events_machine_options(Options) ->
    NS = maps:get(namespace, Options),
    Scheduler = #{registry => mg_procreg_gproc, interval => 100},
    #{
        namespace => NS,
        processor => maps:get(processor, Options),
        tagging => #{
            namespace => <<NS/binary, "_tags">>,
            storage => {mg_storage_memory, #{
                existing_storage_name => ?MODULE
            }},
            worker => #{
                registry => mg_procreg_gproc
            },
            pulse => ?MODULE,
            retries => #{}
        },
        machines => #{
            namespace => NS,
            storage => {mg_storage_memory, #{
                existing_storage_name => ?MODULE
            }},
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
        events_storage => {mg_storage_memory, #{
            existing_storage_name => ?MODULE
        }},
        event_sinks => [
            {?MODULE, Options}
        ],
        pulse => ?MODULE,
        default_processing_timeout => timer:seconds(10),
        max_internal_events => maps:get(max_internal_events, Options)
    }.

%%

-spec start(options(), mg:id(), term()) ->
    ok.
start(Options, MachineID, Args) ->
    Deadline = mg_deadline:from_timeout(3000),
    MgOptions = events_machine_options(Options),
    mg_events_machine:start(MgOptions, MachineID, encode(Args), <<>>, Deadline).

-spec call(options(), mg:id(), term()) ->
    term().
call(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_deadline:from_timeout(3000),
    MgOptions = events_machine_options(Options),
    Result = mg_events_machine:call(MgOptions, {id, MachineID}, encode(Args), HRange, <<>>, Deadline),
    decode(Result).

-spec get_history(options(), mg:id()) ->
    ok.
get_history(Options, MachineID) ->
    HRange = {undefined, undefined, forward},
    MgOptions = events_machine_options(Options),
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
