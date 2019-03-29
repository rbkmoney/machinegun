%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

%%%
%%% Юнит тесты для воркеров.
%%% Задача — проверить корректность работы части отвечающей за автоматическое поднятие и выгрузку воркеров для машин.
%%%
%%% TODO:
%%%  - проверить выгрузку
%%%  - проверить ограничение очереди
%%%  -
%%%
-module(mg_workers_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).

%% tests
-export([base_test       /1]).
-export([load_fail_test  /1]).
-export([load_error_test /1]).
-export([call_fail_test  /1]).
-export([unload_fail_test/1]).
-export([stress_test     /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/3, handle_call/5, handle_unload/1]).

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
       base_test,
       load_fail_test,
       load_error_test,
       call_fail_test,
       unload_fail_test,
       stress_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_workers_manager, '_', '_'}, x),
    % dbg:tpl({mg_workers, '_', '_'}, x),
    Apps = mg_ct_helper:start_applications([mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%%
%% base group tests
%%
-define(unload_timeout, 10).
-define(req_ctx, <<"req_ctx">>).

-spec base_test(config()) ->
    _.
base_test(_C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?unload_timeout, #{link_pid=>erlang:self()}),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_utils:default_deadline()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec load_fail_test(config()) ->
    _.
load_fail_test(_C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?unload_timeout, #{fail_on=>load}),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_utils:default_deadline()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec load_error_test(config()) ->
    _.
load_error_test(_C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?unload_timeout, #{load_error=>test_error, link_pid=>erlang:self()}),
    Pid     = start_workers(Options),
    {error, test_error} = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_utils:default_deadline()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec call_fail_test(config()) ->
    _.
call_fail_test(_C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?unload_timeout, #{fail_on=>call}),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 43, hello, ?req_ctx, mg_utils:default_deadline()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec unload_fail_test(config()) ->
    _.
unload_fail_test(_C) ->
    % падение при unload'е мы не замечаем :(
    Options = workers_options(?unload_timeout, #{fail_on=>unload}),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_utils:default_deadline()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec stress_test(config()) ->
    _.
stress_test(_C) ->
    TestTimeout        = 5 * 1000,
    WorkersCount       = 50,
    TestProcessesCount = 1000,
    UnloadTimeout      = 0, % чтобы машины выгружались в процессе теста

    Options = workers_options(UnloadTimeout, #{link_pid=>erlang:self()}),
    WorkersPid = start_workers(Options),

    TestProcesses = [stress_test_start_process(Options, WorkersCount) || _ <- lists:seq(1, TestProcessesCount)],
    ok = timer:sleep(TestTimeout),

    ok = mg_utils:stop_wait_all(TestProcesses, shutdown, 1000),
    ok = wait_machines_unload(UnloadTimeout + 10),
    ok = stop_workers(WorkersPid).

-spec stress_test_start_process(mg_workers_manager:options(), pos_integer()) ->
    pid().
stress_test_start_process(Options, WorkersCount) ->
    erlang:spawn_link(fun() -> stress_test_process(Options, WorkersCount) end).

-spec stress_test_process(mg_workers_manager:options(), pos_integer()) ->
    no_return().
stress_test_process(Options, WorkersCount) ->
    ok = stress_test_do_test_call(Options, WorkersCount),
    stress_test_process(Options, WorkersCount).

-spec stress_test_do_test_call(mg_workers_manager:options(), pos_integer()) ->
    ok.
stress_test_do_test_call(Options, WorkersCount) ->
    ID = rand:uniform(WorkersCount),
    % проверим, что отвечают действительно на наш запрос
    Call = {hello, erlang:make_ref()},
    Call = mg_workers_manager:call(Options, ID, Call, ?req_ctx, mg_utils:default_deadline()),
    ok.

-spec workers_options(non_neg_integer(), worker_params()) ->
    mg_workers_manager:options().
workers_options(UnloadTimeout, WorkerParams) ->
    #{
        name => base_test_workers,
        pulse => undefined,
        message_queue_len_limit => 10000,
        worker_options => #{
            worker            => {?MODULE, WorkerParams},
            hibernate_timeout => UnloadTimeout div 2,
            unload_timeout    => UnloadTimeout
        }
    }.

%%
%% worker callbacks
%%
%% Реализуется простая логика с поднятием, принятием запроса и выгрузкой.
%%
-type worker_stage() :: load | call | unload.
-type worker_params() :: #{
    link_pid   => pid(),
    load_error => term(),
    fail_on    => worker_stage()
}.
-type worker_state() :: worker_params().

-spec handle_load(_ID, _, worker_params()) ->
    {ok, worker_state()} | {error, _}.
handle_load(_, #{load_error := Reason}, ?req_ctx) ->
    {error, Reason};
handle_load(_, Params, ?req_ctx) ->
    ok = try_link(Params),
    ok = try_exit(load, Params),
    {ok, Params}.

-spec handle_call(_Call, _From, _, _, worker_state()) ->
    {{reply, _Resp}, worker_state()}.
handle_call(Call, _From, ?req_ctx, _Deadline, State) ->
    ok = try_exit(call, State),
    {{reply, Call}, State}.

-spec handle_unload(worker_state()) ->
    ok.
handle_unload(State) ->
    ok = try_exit(unload, State),
    ok = try_unlink(State).

-spec try_exit(worker_stage(), worker_params()) ->
    ok.
try_exit(CurrentStage, #{fail_on := FailOnStage}) when CurrentStage =:= FailOnStage ->
    exit(fail);
try_exit(_Stage, #{}) ->
    ok.

-spec try_link(worker_params()) ->
    ok.
try_link(#{link_pid:=Pid}) ->
    true = erlang:link(Pid), ok;
try_link(#{}) ->
    ok.

-spec try_unlink(worker_params()) ->
    ok.
try_unlink(#{link_pid:=Pid}) ->
    true = erlang:unlink(Pid), ok;
try_unlink(#{}) ->
    ok.

%%
%% utils
%%
-spec start_workers(_Options) ->
    pid().
start_workers(Options) ->
    mg_utils:throw_if_error(mg_workers_manager:start_link(Options)).

-spec stop_workers(pid()) ->
    ok.
stop_workers(Pid) ->
    true = erlang:unlink(Pid),
    true = erlang:exit(Pid, kill),
    ok.

-spec wait_machines_unload(pos_integer()) ->
    ok.
wait_machines_unload(UnloadTimeout) ->
    ok = timer:sleep(UnloadTimeout * 2).
