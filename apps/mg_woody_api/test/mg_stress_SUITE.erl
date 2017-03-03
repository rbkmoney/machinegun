-module(mg_stress_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all             /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).

-export([stress_test/1]).

-define(NS, <<"NS">>).
-define(ES_ID, <<"test_event_sink">>).


-type test_name () :: atom().
-type config    () :: [{atom(), _}].


-spec all() ->
    [test_name()].
all() ->
    [
        stress_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps =
        genlib_app:start_application_with(hackney, [{use_default_pool, false}])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 100}])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(C))
    ,

    CallFunc =
        fun({Args, _Machine}) ->
            case Args of
                <<"event">> -> {Args, {<<>>, [<<"event_body">>]}, #{timer => undefined, tag => undefined}};
                _           -> {Args, {<<>>, []}, #{timer => undefined, tag => undefined}}
            end
        end
    ,
    SignalFunc =
        fun({Args, _Machine}) ->
            case Args of
                _ -> mg_test_processor:default_result(signal)
            end
        end
    ,
    {ok, ProcessorPid} = start_processor({0, 0, 0, 0}, 8023, "/processor", {SignalFunc, CallFunc}),

    [
        {apps              , Apps                             },
        {automaton_options , #{
            url => "http://localhost:8022",
            ns => ?NS,
            retry_strategy => mg_utils:genlib_retry_new({exponential, 5, 2, 1000})
        }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    true = erlang:exit(?config(processor_pid, C), kill),
    _ = [application:stop(App) || App <- proplists:get_value(apps, C)],
    ok.

-spec mg_woody_api_config(config()) ->
    list().
mg_woody_api_config(_C) ->
    [
        {storage, mg_storage_memory},
        {namespaces, #{
            ?NS => #{
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, ns}, {max_connections, 100}]
                },
                event_sink => ?ES_ID
            }
        }}
    ].

-spec stress_test(config()) -> _.
stress_test(C) ->
    TestTimeout = 5 * 1000,
    N = 10,

    Processes = [stress_test_start_processes(C, integer_to_binary(ID)) || ID <- lists:seq(1, N)],

    ok = timer:sleep(TestTimeout),
    ok = mg_utils:stop_wait_all(Processes, shutdown, 2000).

-spec stress_test_start_processes(term(), mg:id()) ->
    _.
stress_test_start_processes(C, ID) ->
    Pid =
        erlang:spawn_link(
            fun() ->
                start_machine(C, ID),
                create(C, ID)
            end
        ),
    timer:sleep(1000),
    Pid.

%%
%% utils
%%
-spec start_machine(config(), mg:id()) ->
    _.
start_machine(C, ID) ->
    mg_automaton_client:start(automaton_options(C), ID, ID).

-spec create(config(), mg:id()) ->
    _.
create(C, ID) ->
    create_event(<<"event">>, C, ID),
    timer:sleep(1000),
    create(C, ID).

-spec create_event(binary(), config(), mg:id()) ->
    _.
create_event(Event, C, ID) ->
    Event = mg_automaton_client:call(automaton_options(C), {id, ID}, Event).

-spec start_processor(Address, Port, Path, Functions) -> {ok, pid()} when
    Address   :: mg_test_processor:host_address(),
    Port      :: integer(),
    Path      :: string(),
    Functions :: {mg_test_processor:processor_function(), mg_test_processor:processor_function()}.
start_processor(Address, Port, Path, {SignalFunc, CallFunc}) ->
    {ok, ProcessorPid} = mg_test_processor:start_link({Address, Port, Path, {SignalFunc, CallFunc}}),
    true = erlang:unlink(ProcessorPid),
    {ok, ProcessorPid}.

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).
