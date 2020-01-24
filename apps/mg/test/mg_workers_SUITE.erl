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
-include("ct_helper.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% tests
-export([base_test       /1]).
-export([load_fail_test  /1]).
-export([load_error_test /1]).
-export([call_fail_test  /1]).
-export([unload_fail_test/1]).
-export([unload_test/1]).
-export([unload_loading_test/1]).
-export([stress_test     /1]).
-export([manager_contention_test/1]).

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
        {group, with_gproc},
        {group, with_consuela}
    ].

-spec groups() ->
    [{group_name(), list(_), [test_name() | {group, group_name()}]}].
groups() ->
    [
        {with_gproc    , [], [{group, base}]},
        {with_consuela , [], [{group, base}]},
        {base          , [], [
            base_test,
            load_fail_test,
            load_error_test,
            call_fail_test,
            unload_fail_test,
            unload_test,
            unload_loading_test,
            stress_test,
            manager_contention_test
        ]}
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
    Apps = mg_ct_helper:start_applications([consuela, mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(with_gproc, C) ->
    [
        {registry, mg_procreg_gproc},
        {load_pressure, 100},
        {runner_retry_strategy, #{
            noproc  => genlib_retry:linear(3, 100),
            default => finish
        }} | C
    ];
init_per_group(with_consuela, C) ->
    [
        {registry, {mg_procreg_consuela, #{}}},
        {load_pressure, 40},
        {runner_retry_strategy, #{
            noproc     => genlib_retry:linear(3, 100),
            noregistry => genlib_retry:linear(3, 500),
            default    => finish
        }} | C
    ];
init_per_group(base, C) ->
    C.

-spec end_per_group(group_name(), config()) ->
    _.
end_per_group(_, _C) ->
    ok.

%%
%% base group tests
%%
-define(unload_timeout, 200).
-define(req_ctx, <<"req_ctx">>).

-spec base_test(config()) ->
    _.
base_test(C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?unload_timeout, #{link_pid=>erlang:self()}, C),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:default()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec load_fail_test(config()) ->
    _.
load_fail_test(C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?unload_timeout, #{fail_on=>load}, C),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:default()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec load_error_test(config()) ->
    _.
load_error_test(C) ->
    % чтобы увидеть падение воркера линкуемся к нему
    Options = workers_options(?unload_timeout, #{load_error=>test_error, link_pid=>erlang:self()}, C),
    Pid     = start_workers(Options),
    {error, test_error} = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:default()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec call_fail_test(config()) ->
    _.
call_fail_test(C) ->
    % тут процесс специально падает, поэтому линк не нужен
    Options = workers_options(?unload_timeout, #{fail_on=>call}, C),
    Pid     = start_workers(Options),
    {error, {unexpected_exit, _}} =
        mg_workers_manager:call(Options, 43, hello, ?req_ctx, mg_deadline:default()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec unload_fail_test(config()) ->
    _.
unload_fail_test(C) ->
    % падение при unload'е мы не замечаем :(
    Options = workers_options(?unload_timeout, #{fail_on=>unload}, C),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:default()),
    ok      = wait_machines_unload(?unload_timeout),
    ok      = stop_workers(Pid).

-spec unload_test(config()) ->
    _.
unload_test(C) ->
    Options = workers_options(?unload_timeout, #{link_pid => self()}, C),
    Pid     = start_workers(Options),
    hello   = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:default()),
    WorkerPid = wait_worker_pid(42),
    ok      = wait_worker_unload(WorkerPid, ?unload_timeout * 2),
    ok      = stop_workers(Pid).

-spec unload_loading_test(config()) ->
    _.
unload_loading_test(C) ->
    LoadLag = 100,
    Options = workers_options(?unload_timeout, #{link_pid => self(), load_lag => LoadLag}, C),
    Pid     = start_workers(Options),
    {error, {timeout, _}} = mg_workers_manager:call(Options, 42, hello, ?req_ctx, mg_deadline:from_timeout(LoadLag div 2)),
    WorkerPid = wait_worker_pid(42),
    ok      = wait_worker_unload(WorkerPid, LoadLag + ?unload_timeout * 2),
    ok      = stop_workers(Pid).

-spec wait_worker_pid(_ID) ->
    pid().
wait_worker_pid(ID) ->
    wait_worker_pid(ID, 100).

-spec wait_worker_pid(_ID, timeout()) ->
    pid().
wait_worker_pid(ID, Timeout) ->
    receive
        {worker, ID, Pid} -> Pid
    after
        Timeout -> erlang:error(no_pid_received)
    end.

-spec wait_worker_unload(_WorkerPid :: pid(), timeout()) ->
    ok.
wait_worker_unload(WorkerPid, Timeout) ->
    MRef = erlang:monitor(process, WorkerPid),
    ok = receive
        {'DOWN', MRef, process, WorkerPid, normal} -> ok
    after
        Timeout -> erlang:error(unload_timed_out)
    end.

-type retry_strategy() :: #{_Reason => genlib_retry:strategy()}.

-spec stress_test(config()) ->
    _.
stress_test(C) ->
    Concurrency   = erlang:system_info(schedulers),
    RunnersCount  = ?config(load_pressure, C) * Concurrency,
    WorkersCount  = RunnersCount div 20,
    UnloadTimeout = 100, % чтобы машины выгружались в процессе теста
    RetryStrategy = ?config(runner_retry_strategy, C),
    Job = fun (ManagerOptions, _N, RetrySt) ->
        stress_test_do_test_call(ManagerOptions, WorkersCount, RetrySt)
    end,
    ok = run_load_test(#{
        duration        => 10 * 1000,
        runners         => RunnersCount,
        job             => {Job, RetryStrategy},
        manager_options => workers_options(UnloadTimeout, #{link_pid=>erlang:self()}, C)
    }).

-spec stress_test_do_test_call(mg_workers_manager:options(), pos_integer(), retry_strategy()) ->
    ok.
stress_test_do_test_call(Options, WorkersCount, RetrySt) ->
    ID = rand:uniform(WorkersCount),
    % проверим, что отвечают действительно на наш запрос
    Call = {hello, erlang:make_ref()},
    case mg_workers_manager:call(Options, ID, Call, ?req_ctx, mg_deadline:default()) of
        Call            -> ok;
        {error, Reason} -> maybe_retry(Reason, RetrySt)
    end.

-spec manager_contention_test(config()) ->
    _.
manager_contention_test(C) ->
    Concurrency   = erlang:system_info(schedulers),
    RunnersCount  = ?config(load_pressure, C) * Concurrency,
    UnloadTimeout = 100, % чтобы машины выгружались в процессе теста
    RetryStrategy = ?config(runner_retry_strategy, C),
    ok = run_load_test(#{
        duration        => 10 * 1000,
        runners         => RunnersCount,
        job             => {fun manager_contention_test_call/3, RetryStrategy},
        manager_options => workers_options(UnloadTimeout, 10 * Concurrency, #{link_pid=>erlang:self()}, C)
    }).

-spec manager_contention_test_call(mg_workers_manager:options(), pos_integer(), retry_strategy()) ->
    ok.
manager_contention_test_call(Options, N, RetrySt) ->
    % проверим, что отвечают действительно на наш запрос
    Call = {hello, erlang:make_ref()},
    case mg_workers_manager:call(Options, N, Call, ?req_ctx, mg_deadline:default()) of
        Call                           -> ok;
        {error, {transient, overload}} -> ok;
        {error, Reason}                -> maybe_retry(Reason, RetrySt)
    end.

-spec maybe_retry(_Reason, retry_strategy()) ->
    {ok, retry_strategy()}.
maybe_retry(Reason, RetrySt) ->
    Class = case Reason of
        {transient, noproc}   -> noproc;
        {transient, normal}   -> noproc;
        {transient, shutdown} -> noproc;
        {transient, {registry_unavailable, timeout}} -> noregistry;
        {transient, {registry_unavailable, {unknown,timeout}}} -> noregistry;
        _ -> default
    end,
    {ID, Retry} = case maps:find(Class, RetrySt) of
        {ok, R} -> {Class, R};
        error   -> {default, maps:get(default, RetrySt)}
    end,
    case genlib_retry:next_step(Retry) of
        {wait, Timeout, RetryLeft} ->
            _ = ct:pal(warning, "~p retrying error: ~p, retries left: ~p", [self(), Reason, RetryLeft]),
            ok = timer:sleep(Timeout),
            {ok, RetrySt#{ID := RetryLeft}};
        finish ->
            _ = ct:pal(warning, "~p unretryable error: ~p", [self(), Reason]),
            erlang:error(Reason)
    end.

-type load_options() :: #{
    duration        := timeout(),
    workers         := pos_integer(),
    runners         := pos_integer(),
    job             := {load_job_fun(), _InitialState},
    manager_options := mg_workers_manager:options()
}.

-type load_job_fun() :: fun((_N :: pos_integer()) -> no_return()).

-spec run_load_test(load_options()) ->
    _.
run_load_test(#{
    duration        := Duration,
    runners         := RunnersCount,
    job             := {Job, St0},
    manager_options := ManagerOptions = #{worker_options := WorkerOptions}
} = Options) ->
    _ = ct:pal("running load test ~p", [Options]),
    Ts = now_diff(0),
    WorkersPid = start_workers(ManagerOptions),
    _ = ct:pal("===> [~p] start workers done", [now_diff(Ts)]),
    RunnerPids = [stress_test_start_process(ManagerOptions, Job, N, St0) || N <- lists:seq(1, RunnersCount)],
    _ = ct:pal("===> [~p] start runners done", [now_diff(Ts)]),
    ok = timer:sleep(Duration),
    _ = ct:pal("===> [~p] sleep done", [now_diff(Ts)]),
    ok = mg_ct_helper:stop_wait_all(RunnerPids, shutdown, RunnersCount * 10),
    _ = ct:pal("===> [~p] stop runners done", [now_diff(Ts)]),
    ok = wait_machines_unload(maps:get(unload_timeout, WorkerOptions, 60 * 1000)),
    ok = stop_workers(WorkersPid),
    _ = ct:pal("===> [~p] stop workers done", [now_diff(Ts)]).

-spec now_diff(integer()) ->
    non_neg_integer().
now_diff(Ts) ->
    erlang:system_time(millisecond) - Ts.

-spec stress_test_start_process(mg_workers_manager:options(), load_job_fun(), _N :: pos_integer(), _State) ->
    pid().
stress_test_start_process(Options, Job, N, State) ->
    Runner = fun Runner (St) ->
        case Job(Options, N, St) of
            {ok, St1} -> Runner(St1);
            ok        -> Runner(St)
        end
    end,
    erlang:spawn_link(fun () ->
        Runner(State)
    end).

-spec workers_options(non_neg_integer(), worker_params(), config()) ->
    mg_workers_manager:options().
workers_options(UnloadTimeout, WorkerParams, C) ->
    workers_options(UnloadTimeout, 5000, WorkerParams, C).

-spec workers_options(non_neg_integer(), non_neg_integer(), worker_params(), config()) ->
    mg_workers_manager:options().
workers_options(UnloadTimeout, MsgQueueLen, WorkerParams, C) ->
    #{
        name => base_test_workers,
        pulse => undefined,
        registry => ?config(registry, C),
        message_queue_len_limit => MsgQueueLen,
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
    load_lag   => pos_integer(), % milliseconds
    load_error => term(),
    fail_on    => worker_stage()
}.
-type worker_state() :: worker_params().

-spec handle_load(_ID, _, worker_params()) ->
    {ok, worker_state()} | {error, _}.
handle_load(_, #{load_error := Reason}, ?req_ctx) ->
    {error, Reason};
handle_load(ID, Params, ?req_ctx) ->
    ok = try_link(ID, Params),
    ok = try_exit(load, Params),
    ok = timer:sleep(maps:get(load_lag, Params, 0)),
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

-spec try_link(_ID, worker_params()) ->
    ok.
try_link(ID, #{link_pid:=Pid}) ->
    _ = Pid ! {worker, ID, self()},
    true = erlang:link(Pid),
    ok;
try_link(_ID, #{}) ->
    ok.

-spec try_unlink(worker_params()) ->
    ok.
try_unlink(#{link_pid:=Pid}) ->
    true = erlang:unlink(Pid),
    ok;
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
    ok = proc_lib:stop(Pid, normal, 60 * 1000),
    ok.

-spec wait_machines_unload(pos_integer()) ->
    ok.
wait_machines_unload(UnloadTimeout) ->
    ok = timer:sleep(UnloadTimeout * 2).
