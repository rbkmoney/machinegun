%%%
%%% TODO сделать нормальный тест автомата, как вариант, через пропер
%%%
-module(mg_stress_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% stress_test group
-export([stress_test/1]).

-define(NS, <<"NS">>).
-define(ID, <<"ID">>).
-define(Tag, <<"tag">>).
-define(Ref, {tag, ?Tag}).
-define(ES_ID, <<"test_event_sink">>).


%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].


-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        {group, memory}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory, [sequence], tests_groups()}
    ].

-spec tests_groups() ->
    [{group_name(), list(_), test_name()}].
tests_groups() ->
    [
        {stress, [sequence], [
            stress_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_woody_api_processor, '_', '_'}, x),
    % dbg:tpl({mg_machine_event_sink, '_', '_'}, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(memory, C) ->
    [{storage, mg_storage_memory} | C];
init_per_group(TestGroup, C0) ->
    C = [{test_instance, erlang:atom_to_binary(TestGroup, utf8)} | C0],
    %% TODO сделать нормальную генерацию урлов
    Apps =
        genlib_app:start_application_with(lager, [
            {handlers, [{lager_common_test_backend, info}]},
            {async_threshold, undefined}
        ])
        ++
        genlib_app:start_application_with(hackney, [{use_default_pool, false}])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 100}])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(TestGroup, C))
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
        {automaton_options , {"http://localhost:8022", ?NS   }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec mg_woody_api_config(atom(), config()) ->
    list().
mg_woody_api_config(event_sink, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS => #{
                processor  => #{ url => <<"http://localhost:8023/processor">>, recv_timeout => 5000 },
                event_sink => ?ES_ID
            }
        }}
    ];
mg_woody_api_config(_, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS => #{
                processor => #{ url => <<"http://localhost:8023/processor">> },
                event_sink => ?ES_ID
            }
        }}
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(memory, _) ->
    ok;
end_per_group(_, C) ->
    true = erlang:exit(?config(processor_pid, C), kill),
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(atom()) ->
    _.
application_stop(App=sasl) ->
    %% hack for preventing sasl deadlock
    %% http://erlang.org/pipermail/erlang-questions/2014-May/079012.html
    error_logger:delete_report_handler(cth_log_redirect),
    application:stop(App),
    error_logger:add_report_handler(cth_log_redirect),
    ok;
application_stop(App) ->
    application:stop(App).

%%
%% stress test
%%
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
                lager:warning("starting: ~s", [ID]),
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

-spec create_event(binary(), config(), mg:id()) ->
    _.
create_event(Event, C, ID) ->
    Strategy = mg_utils:genlib_retry_new({linear, 5, 1000}),
    mg_automaton_client:call_with_retry(automaton_options(C), {id, ID}, Event, Strategy).

-spec create(config(), mg:id()) ->
    _.
create(C, ID) ->
    create_event(<<"event">>, C, ID),
    timer:sleep(1000),
    create(C, ID).

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
