-module(mg_tests_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).
-export([machine_test  /1]).

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
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all,c),
    % dbg:tpl(mg_machine, x),
    % dbg:tpl(mg_machine_db_test, x),
    % dbg:tpl(mg_machine_test_door, x),
    % dbg:tpl(mg_timers, x),
    % dbg:tpl(mg_machine_server, x),
    {ok, Apps } = application:ensure_all_started(gproc),
    {ok, Apps1} = application:ensure_all_started(sasl ),
    [{apps, Apps ++ Apps1} | C].

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
-spec machine_test(config()) ->
    ok.
machine_test(_C) ->
    {ok, _} = mg_machine_test_door:start_link(),

    % mg_machine_sup:start_link({mg_machine_test_door, mg_machine_db_test}).

    % запустить автомат
    % прогнать по стейтам
    Tag = <<"hello">>,
    _ID = mg_machine_test_door:start (Tag),
    ok = mg_machine_test_door:close (Tag),
    ok = mg_machine_test_door:open  (Tag),
    ok = mg_machine_test_door:close (Tag),
    ok = mg_machine_test_door:open  (Tag),
    ok = timer:sleep(1000),
    ok = mg_machine_test_door:lock  (Tag, <<"123">>),
    {error, bad_passwd} = mg_machine_test_door:unlock(Tag, <<"12">>),
    ok = mg_machine_test_door:unlock(Tag, <<"123">>),
    ok.

