-module(mg_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all              /0]).
-export([init_per_testcase/2]).
-export([end_per_testcase /2]).
-export([machine_test     /1]).

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%%
%% tests descriptions
%%
-spec all() ->
    [test_name()].
all() ->
    [
        machine_test
    ].

%%
%% starting/stopping
%%
-spec init_per_testcase(_, config()) ->
    config().
init_per_testcase(machine_test, C) ->
    dbg:tracer(), dbg:p(all,c),
    % dbg:tpl(mg_db_test_server, x),

    Apps =
        genlib_app:start_application_with(lager, [{handlers, [{lager_common_test_backend, debug}]}])
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
    % TODO сделать нормальный тест автомата, как вариант, через пропер
    ProcessorOptions = {{0,0,0,0}, 8821, "/processor"},
    AutomatonOptions = {"http://localhost:8820", <<"mg_test_ns">>},

    {ok, _} = mg_machine_test_door:start_link(ProcessorOptions),

    % запустить автомат
    Tag = <<"hello">>,
    _ID = mg_machine_test_door:start(AutomatonOptions, Tag),
    CS0 = #{last_event_id => undefined, state => undefined},
    CS1 = #{state:=open} = mg_machine_test_door:update_state(AutomatonOptions, Tag, CS0),

    % прогнать по стейтам
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Tag),
    CS2 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Tag, CS1),

    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Tag),
    CS3 = #{state:=open} = mg_machine_test_door:update_state(AutomatonOptions, Tag, CS2),
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Tag),

    CS4 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Tag, CS3),
    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Tag),
    ok = timer:sleep(2000),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {lock, <<"123">>}, Tag),
    {error, bad_passwd} =
        mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"12">>}, Tag),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"123">>}, Tag),
    _CS5 = #{state:=closed} = mg_machine_test_door:update_state(AutomatonOptions, Tag, CS4),
    ok.
