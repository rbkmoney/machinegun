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

-module(mg_storage_pool).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, do_request/3]).
-export([start_link/2]).

-type options() :: #{
    worker          := mg_storage:options(),
    size            := pos_integer(), % 100
    queue_len_limit := pos_integer(), % 10
    retry_attempts  := pos_integer()  % size
}.
-type worker_id() :: term().


%%
%% mg_storage callbacks
%%
-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, RegName) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, RegName]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options(), mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(#{worker := Worker}, RegName) ->
    mg_utils_supervisor_wrapper:start_link(
        RegName,
        #{strategy => simple_one_for_one},
        [
            fix_restart_type(mg_storage:child_spec(Worker, worker))
        ]
    ).

-spec do_request(options(), mg_utils:gen_ref(), mg_storage:request()) ->
    mg_storage:response() | no_return().
do_request(Options = #{retry_attempts := RetryAttempts}, SelfRef, Req) ->
    do_request(Options, SelfRef, Req, RetryAttempts).

-spec do_request(options(), mg_utils:gen_ref(), mg_storage:request(), pos_integer()) ->
    mg_storage:response() | no_return().
do_request(_Options, _SelfRef, _Req, 0) ->
    throw_error({'storage request error', overload});
do_request(Options = #{size := Size, worker := Worker, queue_len_limit := Limit}, SelfRef, Req, RemainAttempts) ->
    WorkerID = random_worker_id(Size),
    WorkerRef = worker_ref(SelfRef, WorkerID),
    F = fun() ->
            ok = mg_utils:check_overload(WorkerRef, Limit),
            mg_storage:do_request(Worker, WorkerRef, Req)
        end,
    case try_gen_call(F) of
        {error, noproc} ->
            ok = start_worker(Options, SelfRef, WorkerID),
            do_request(Options, SelfRef, Req, RemainAttempts);
        {error, overload} ->
            do_request(Options, SelfRef, Req, RemainAttempts -1 );
        {error, Reason} ->
            throw_error({'storage request error', Reason});
        R ->
            R
    end.

%%
%% local
%%
-spec start_worker(options(), mg_utils:gen_ref(), worker_id()) ->
    ok | no_return().
start_worker(#{queue_len_limit := Limit}, SelfRef, WorkerID) ->
    F = fun() ->
            ok = mg_utils:check_overload(SelfRef, Limit),
            supervisor:start_child(SelfRef, [worker_reg_name(SelfRef, WorkerID)])
        end,
    case try_gen_call(F) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            throw_error({'start pool worker error', Reason})
    end.

-spec throw_error(term()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({transient, {storage_unavailable, Error}}).

-spec worker_reg_name(mg_utils:gen_ref(), worker_id()) ->
    mg_utils:gen_ref().
worker_reg_name(SelfRef, WorkerID) ->
    {via, gproc, gproc_key(SelfRef, WorkerID)}.

-spec worker_ref(mg_utils:gen_ref(), worker_id()) ->
    mg_utils:gen_ref().
worker_ref(SelfRef, WorkerID) ->
    {via, gproc, gproc_key(SelfRef, WorkerID)}.

-spec gproc_key(mg_utils:gen_ref(), worker_id()) ->
    gproc:key().
gproc_key(SelfRef, WorkerID) ->
    {n, l, wrap(SelfRef, WorkerID)}.

-spec wrap(mg_utils:gen_ref(), worker_id()) ->
    term().
wrap(SelfRef, _) when is_pid(SelfRef) ->
    % если сюда передать pid, то ничего работать не будет :)
    exit(unsupported);
wrap(SelfRef, WorkerID) ->
    {?MODULE, SelfRef, WorkerID}.

-spec random_worker_id(pos_integer()) ->
    worker_id().
random_worker_id(N) ->
    rand:uniform(N).

-spec try_gen_call(fun(() -> Result)) ->
    {ok, Result} | {error, _}.
try_gen_call(F) ->
    try
        F()
    catch
        % TODO сделать нормально
        % Тут есть проблема, что если сторадж запустить в обход пула, то нормально работать не будет
        % (например riak будет падать с exit при дисконнекте).
        % И лучше будет сделать универсальный интерфейс для кидания ошибок стораджа,
        % в который ввести ошибку отсутствия процесса и ловить её тут.
        exit: noproc                                     -> {error, noproc             };
        exit:{noproc  ,           {gen_server, call, _}} -> {error, noproc             };
        exit:{normal  ,           {gen_server, call, _}} -> {error, noproc             };
        exit:{shutdown,           {gen_server, call, _}} -> {error, noproc             };
        exit:{timeout , Details = {gen_server, call, _}} -> {error, {timeout , Details}};
        exit:overload                                    -> {error, overload           }
    end.

-spec fix_restart_type(supervisor:child_spec()) ->
    supervisor:child_spec().
fix_restart_type(Spec = #{}) ->
    Spec#{restart => temporary}.
