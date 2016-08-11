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

%% test_door group tests
-export([machine_test_door/1]).


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
        {group, base     },
        {group, repair   },
        {group, test_door}
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
    % dbg:tpl(mg_worker, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(_, C) ->
    %% TODO сделать нормальную генерацию урлов
    NS = <<"mg_test_ns">>,
    Apps =
        genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, debug}]}])
        % genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, error}]}])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 1}])
        ++
        genlib_app:start_application_with(mg, [{nss, [{NS , <<"http://localhost:8821/processor">>}]}])
    ,

    {ok, ProcessorPid} = mg_machine_test_door:start_link({{0,0,0,0}, 8821, "/processor"}),
    true = erlang:unlink(ProcessorPid),

    [
        {apps             , Apps                         },
        {processor_pid    , ProcessorPid                 },
        {automaton_options, {"http://localhost:8820", NS}}
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


-define(ID, <<"ID">>).
-define(Tag, <<"Tag">>).
-define(Ref, {tag, ?Tag}).

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
%% test_door group tests
%%
-spec machine_test_door(config()) ->
    ok.
machine_test_door(C) ->
    % запустить автомат
    _ID = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag),
    CS0 = #{last_event_id => undefined, state => undefined},
    CS1 = #{state:=open} = mg_machine_test_door:update_state(a_opts(C), ?Ref, CS0),

    % прогнать по стейтам
    ok = mg_machine_test_door:do_action(a_opts(C), close, ?Ref),
    CS2 = #{state:=closed} = mg_machine_test_door:update_state(a_opts(C), ?Ref, CS1),

    ok = mg_machine_test_door:do_action(a_opts(C), open , ?Ref),
    CS3 = #{state:=open} = mg_machine_test_door:update_state(a_opts(C), ?Ref, CS2),
    ok = mg_machine_test_door:do_action(a_opts(C), close, ?Ref),

    CS4 = #{state:=closed} = mg_machine_test_door:update_state(a_opts(C), ?Ref, CS3),
    ok = mg_machine_test_door:do_action(a_opts(C), open , ?Ref),
    ok = timer:sleep(2000),
    ok = mg_machine_test_door:do_action(a_opts(C), {lock, <<"123">>}, ?Ref),
    {error, bad_passwd} =
        mg_machine_test_door:do_action(a_opts(C), {unlock, <<"12">>}, ?Ref),
    ok = mg_machine_test_door:do_action(a_opts(C), {unlock, <<"123">>}, ?Ref),
    _CS5 = #{state:=closed} = mg_machine_test_door:update_state(a_opts(C), ?Ref, CS4),
    ok.


%%
%% utils
%%
a_opts(C) -> ?config(automaton_options, C).
