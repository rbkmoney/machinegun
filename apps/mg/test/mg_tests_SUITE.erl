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
    % dbg:tpl(mg_machine, x),
    dbg:tpl(mg_db_test, x),
    % dbg:tpl({mg_machine_test_door, apply_events, '_'}, x),
    % dbg:tpl({mg_machine_test_door, handle_signal_, '_'}, x),
    % dbg:tpl({mg_machine_test_door, handle_action, '_'}, x),
    % dbg:tpl(mg_timers, x),
    % dbg:tpl(mg_machine_worker, x),

    _ = application:load(lager),
    application:set_env(lager, handlers, [
        {lager_common_test_backend, debug}
    ]),
    _ = application:load(woody),
    application:set_env(woody, acceptors_pool_size, 1),
    _ = application:load(mg),
    application:set_env(mg, nss, [
        {<<"mg_test_ns">> , <<"http://localhost:8821/processor">>}
    ]),
    {ok, Apps } = application:ensure_all_started(mg  ),
    % {ok, Apps1} = application:ensure_all_started(sasl),
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
    _ID = mg_machine_test_door:start(AutomatonOptions, Tag),

    % прогнать по стейтам
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Tag),
    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Tag),
    ok = mg_machine_test_door:do_action(AutomatonOptions, close, Tag),
    ok = mg_machine_test_door:do_action(AutomatonOptions, open , Tag),
    ok = timer:sleep(2000),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {lock, <<"123">>}, Tag),
    {error, bad_passwd} =
        mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"12">>}, Tag),
    ok = mg_machine_test_door:do_action(AutomatonOptions, {unlock, <<"123">>}, Tag),
    ok.
