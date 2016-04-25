-module(mg_machine_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%%
%% tests descriptions
%%
-spec all() ->
    [test_name()].
all() ->
    [
        simple_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all,c), dbg:tpl(gproc, x),
    {ok, Apps} = application:ensure_all_started(gproc),
    {ok, Apps1} = application:ensure_all_started(sasl),
    [{apps, Apps ++ Apps1} | C].


-spec init_per_testcase(atom(), config()) ->
    config().
init_per_testcase(_, C) ->
    {ok, _} = mg_machine_test_door:doors_sup_start_link(),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
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
-spec simple_test(config()) ->
    ok.
simple_test(_C) ->
    % запустить автомат
    % прогнать по стейтам
    ID = <<"hello">>,
    ok = mg_machine_test_door:close (ID),
    ok = mg_machine_test_door:open  (ID),
    ok = mg_machine_test_door:close (ID),
    ok = mg_machine_test_door:lock  (ID, <<"123">>),
    {error, bad_passwd} = mg_machine_test_door:unlock(ID, <<"12">>),
    ok = mg_machine_test_door:unlock(ID, <<"123">>),
    ok.
