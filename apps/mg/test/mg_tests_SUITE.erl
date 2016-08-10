%%%
%%% TODO сделать нормальный тест автомата, как вариант, через пропер
%%%
-module(mg_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-export([all              /0]).
-export([init_per_testcase/2]).
-export([end_per_testcase /2]).

-export([machine_test               /1]).
-export([namespace_not_found_test   /1]).
-export([machine_already_exists_test/1]).
-export([machine_id_not_found       /1]).
-export([machine_tag_not_found      /1]).

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%%
%% tests descriptions
%%
-spec all() ->
    [test_name()].
all() ->
    [
        machine_test,
        namespace_not_found_test,
        machine_already_exists_test,
        machine_id_not_found,
        machine_tag_not_found
    ].

%%
%% starting/stopping
%%
-spec init_per_testcase(_, config()) ->
    config().
init_per_testcase(_, C) ->
    % dbg:tracer(), dbg:p(all,c),
    % dbg:tpl(mg_worker, x),

    Apps =
        % genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, debug}]}])
        genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, error}]}])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 1}])
        ++
        genlib_app:start_application_with(mg, [{nss, [{<<"mg_test_ns">> , <<"http://localhost:8821/processor">>}]}])
    ,
    [{apps, Apps} | C].

-spec end_per_testcase(_, config()) ->
    ok.
end_per_testcase(_, C) ->
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
%% tests
%%
-spec machine_test(config()) ->
    ok.
machine_test(_C) ->
    ProcessorOptions = {{0,0,0,0}, 8821, "/processor"},
    AutomatonOptions = {"http://localhost:8820", <<"mg_test_ns">>},

    {ok, _} = mg_machine_test_door:start_link(ProcessorOptions),

    % запустить автомат
    Tag = <<"hello">>,
    _ID = mg_machine_test_door:start(AutomatonOptions, <<"ID">>, Tag),
    Ref = {tag, Tag},
    CS0 = #{last_event_id => undefined, state => undefined},
    CS1 = #{state:=open} = mg_machine_test_door:update_state(AutomatonOptions, Ref, CS0),

    % прогнать по стейтам
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Ref),
    CS2 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Ref, CS1),

    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Ref),
    CS3 = #{state:=open} = mg_machine_test_door:update_state(AutomatonOptions, Ref, CS2),
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Ref),

    CS4 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Ref, CS3),
    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Ref),
    ok = timer:sleep(2000),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {lock, <<"123">>}, Ref),
    {error, bad_passwd} =
        mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"12">>}, Ref),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"123">>}, Ref),
    _CS5 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Ref, CS4),
    ok.

%% некорректный namespace
-spec namespace_not_found_test(config()) ->
    ok.
namespace_not_found_test(_C) ->
    AutomatonOptions = {"http://localhost:8820", <<"incorrect_ns">>},
    #'NamespaceNotFound'{} = (catch mg_machine_test_door:start(AutomatonOptions, <<"ID">>, <<"Tag">>)),
    ok.

%% некорректный id при старте
-spec machine_already_exists_test(config()) ->
    ok.
machine_already_exists_test(_C) ->
    ProcessorOptions = {{0,0,0,0}, 8821, "/processor"},
    AutomatonOptions = {"http://localhost:8820", <<"mg_test_ns">>},

    {ok, _} = mg_machine_test_door:start_link(ProcessorOptions),
    ok                         =        mg_machine_test_door:start(AutomatonOptions, <<"ID">>, <<"Tag0">>),
    #'MachineAlreadyExists'{}  = (catch mg_machine_test_door:start(AutomatonOptions, <<"ID">>, <<"Tag1">>)),

    ok.

%% некорректный id
-spec machine_id_not_found(config()) ->
    ok.
machine_id_not_found(_C) ->
    ProcessorOptions = {{0,0,0,0}, 8821, "/processor"},
    AutomatonOptions = {"http://localhost:8820", <<"mg_test_ns">>},

    {ok, _} = mg_machine_test_door:start_link(ProcessorOptions),
    ok = mg_machine_test_door:start(AutomatonOptions, <<"ID">>, <<"Tag">>),
    #'MachineNotFound'{} =
        (catch mg_machine_test_door:do_action(AutomatonOptions, close, {id, <<"incorrect_ID">>})),

    ok.

%% некорректный tag
-spec machine_tag_not_found(config()) ->
    ok.
machine_tag_not_found(_C) ->
    ProcessorOptions = {{0,0,0,0}, 8821, "/processor"},
    AutomatonOptions = {"http://localhost:8820", <<"mg_test_ns">>},

    {ok, _} = mg_machine_test_door:start_link(ProcessorOptions),
    ok = mg_machine_test_door:start(AutomatonOptions, <<"ID">>, <<"Tag">>),
    #'MachineNotFound'{} =
        (catch mg_machine_test_door:do_action(AutomatonOptions, close, {tag, <<"incorrect_Tag">>})),

    ok.

%% TODO
%% get history к несущестующему эвенту

%% двойное тэгирование

%% падение машины
%% запрос к упавшей машине
%% восстановление машины
%% упавший запрос на восстановление
