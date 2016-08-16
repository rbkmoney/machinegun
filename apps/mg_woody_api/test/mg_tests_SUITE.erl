%%%
%%% TODO сделать нормальный тест автомата, как вариант, через пропер
%%%
-module(mg_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups        /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).
-export([init_per_group/2]).
-export([end_per_group /2]).

%% base group tests
-export([namespace_not_found   /1]).
-export([machine_start         /1]).
-export([machine_already_exists/1]).
-export([machine_call_by_id    /1]).
-export([machine_id_not_found  /1]).
-export([machine_tag_not_found /1]).
-export([machine_call_by_tag   /1]).

%% repair group tests
-export([machine_processor_error    /1]).
-export([failed_machine_call        /1]).
-export([failed_machine_repair_error/1]).
-export([failed_machine_repair      /1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).

%% test_door group tests
-export([machine_test_door/1]).

-define(NS, <<"mg_test_ns">>).
-define(ID, <<"ID">>).
-define(Tag, <<"Tag">>).
-define(Ref, {tag, ?Tag}).


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
        {group, base      },
        {group, repair    },
        {group, event_sink},
        {group, test_door }
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {base, [sequence], [
            namespace_not_found,
            machine_start,
            machine_already_exists,
            machine_call_by_id,
            machine_id_not_found,
            machine_tag_not_found,

            % machine_tag_action,
            machine_call_by_tag

            % machine_timer_action
            % machine_get_history_by,

            % machine_get_unexisted_event
            % machine_double_tagging
        ]},

        {repair, [sequence], [
            machine_start,
            machine_processor_error,
            failed_machine_call,
            failed_machine_repair_error,
            failed_machine_call,
            failed_machine_repair,
            machine_call_by_id
        ]},

        {event_sink, [], [
            event_sink_get_empty_history,
            event_sink_get_not_empty_history,
            event_sink_get_last_event
        ]},

        {test_door, [sequence], [
            machine_test_door
        ]}
    ].


%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all,c),
    % dbg:tpl({mg_machine_test_door, update_state, '_'}, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(_, C) ->
    %% TODO сделать нормальную генерацию урлов
    Apps =
        genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, debug}]}])
        % genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, error}]}])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 1}])
        ++
        genlib_app:start_application_with(mg_woody_api, [{nss, [{?NS , <<"http://localhost:8821/processor">>}]}])
    ,

    {ok, ProcessorPid} = mg_machine_test_door:start_link({{0, 0, 0, 0}, 8821, "/processor"}),
    true = erlang:unlink(ProcessorPid),

    [
        {apps              , Apps                          },
        {processor_pid     , ProcessorPid                  },
        {automaton_options , {"http://localhost:8820", ?NS}},
        {event_sink_options,  "http://localhost:8820"      }
    |
        C
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
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
%% base group tests
%%
-spec namespace_not_found(config()) -> _.
namespace_not_found(C) ->
    {URL, _NS} = a_opts(C),
    #'NamespaceNotFound'{} = (catch mg_machine_test_door:start({URL, <<"incorrect_NS">>}, ?ID, ?Tag)).

-spec machine_start(config()) -> _.
machine_start(C) ->
    ok = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag).

-spec machine_already_exists(config()) -> _.
machine_already_exists(C) ->
    #'MachineAlreadyExists'{} = (catch mg_machine_test_door:start(a_opts(C), ?ID, <<"another_Tag">>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    ok = mg_machine_test_door:do_action(a_opts(C), touch, {id, ?ID}).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
    #'MachineNotFound'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, {id, <<"incorrect_ID">>})).


-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(C) ->
    ok = mg_machine_test_door:do_action(a_opts(C), touch, {tag, ?Tag}).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(C) ->
    #'MachineNotFound'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, {tag, <<"incorrect_Tag">>})).

%% get history к несущестующему эвенту
%% двойное тэгирование

%%
%% repair group tests
%%
%% падение машины
-spec machine_processor_error(config()) ->
    _.
machine_processor_error(C) ->
    #'MachineFailed'{} = (catch mg_machine_test_door:do_action(a_opts(C), fail, ?Ref)).

-spec failed_machine_call(config()) ->
    _.
failed_machine_call(C) ->
    #'MachineFailed'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, ?Ref)).

-spec failed_machine_repair_error(config()) ->
    _.
failed_machine_repair_error(C) ->
    ok = (catch mg_machine_test_door:repair(a_opts(C), ?Ref, error)).

-spec failed_machine_repair(config()) ->
    _.
failed_machine_repair(C) ->
    ok = (catch mg_machine_test_door:repair(a_opts(C), ?Ref, ok)).

%%
%% event_sink group test
%%
-spec event_sink_get_empty_history(config()) ->
    _.
event_sink_get_empty_history(C) ->
    [] = event_sink_get_history(es_opts(C), #'HistoryRange'{direction=forward}).

-spec event_sink_get_not_empty_history(config()) ->
    _.
event_sink_get_not_empty_history(C) ->
    _ID = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag),
    ok = test_door_do_action(C, close),
    ok = test_door_do_action(C, open ),
    [
        #'SinkEvent'{id = 1, source_id = ?ID, source_ns = ?NS, event = #'Event'{}},
        #'SinkEvent'{id = 2, source_id = ?ID, source_ns = ?NS, event = #'Event'{}},
        #'SinkEvent'{id = 3, source_id = ?ID, source_ns = ?NS, event = #'Event'{}}
    ] = event_sink_get_history(es_opts(C), #'HistoryRange'{direction=forward}).

-spec event_sink_get_last_event(config()) ->
    _.
event_sink_get_last_event(C) ->
    [#'SinkEvent'{id = 3, source_id = ?ID, source_ns = ?NS, event = #'Event'{}}] =
        event_sink_get_history(es_opts(C), #'HistoryRange'{direction=backward, limit=1}).

%%
%% test_door group tests
%%
-spec machine_test_door(config()) ->
    _.
machine_test_door(C) ->
    % запустить автомат
    _ID = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag),
    CS0 = #{last_event_id => undefined, state => undefined},
    CS1 = #{state:=open} = test_door_update_state(C, CS0),

    % прогнать по стейтам
    ok = test_door_do_action(C, close),
    CS2 = #{state:=closed} = test_door_update_state(C, CS1),

    ok = test_door_do_action(C, open),
    CS3 = #{state:=open} = test_door_update_state(C, CS2),
    ok = test_door_do_action(C, close),

    CS4 = #{state:=closed} = test_door_update_state(C, CS3),
    ok = test_door_do_action(C, open),
    ok = timer:sleep(2000),
    ok = test_door_do_action(C, {lock, <<"123">>}),
    {error, bad_passwd} = test_door_do_action(C, {unlock, <<"12">>}),
    ok = test_door_do_action(C, {unlock, <<"123">>}),
    _CS5 = #{state:=closed} = test_door_update_state(C, CS4).

%%
%% utils
%%
-spec test_door_update_state(config(), mg_machine_test_door:client_state()) ->
    mg_machine_test_door:client_state().
test_door_update_state(C, CS) ->
    mg_machine_test_door:update_state(a_opts(C), ?Ref, CS).

-spec test_door_do_action(config(), mg_machine_test_door:action()) ->
    _.
test_door_do_action(C, Action) ->
    mg_machine_test_door:do_action(a_opts(C), Action, ?Ref).

-spec a_opts(config()) -> _.
a_opts(C) -> ?config(automaton_options, C).
-spec es_opts(config()) -> _.
es_opts(C) -> ?config(event_sink_options, C).


%%
%% event_sink client
%%
-spec event_sink_get_history(_Options, mg:history_range()) ->
    mg:history().
event_sink_get_history(BaseURL, Range) ->
    call_event_sink_service(BaseURL, 'GetHistory', [Range]).

%%

-spec call_event_sink_service(_BaseURL, atom(), [_arg]) ->
    _.
call_event_sink_service(BaseURL, Function, Args) ->
    try
        {R, _} =
            woody_client:call(
                woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler),
                {{mg_proto_state_processing_thrift, 'EventSink'}, Function, Args},
                #{url => BaseURL ++ "/v1/event_sink"}
            ),
        case R of
            {ok, V} -> V;
             ok     -> ok
        end
    catch throw:{{exception, Exception}, _} ->
        throw(Exception)
    end.
