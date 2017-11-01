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

-module(mg_worker).

%% API
-export_type([options     /0]).
% -export_type([call_context/0]).

-export([child_spec/2]).
-export([start_link/2]).
-export([call      /5]).

%% raft_server
-behaviour(raft_server).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, handle_info/3, apply_delta/4]).

%%
%% API
%%
-type options() :: #{
    namespace      => _,
    worker         => worker(),
    raft           => raft_server:options(),
    unload_timeout => pos_integer(),
    unload_fun     => fun(() -> _)
}.

-type worker      () :: mg_utils:mod_opts().
-type worker_state() :: _.

%%

-callback handle_load(_, _ID, _ReqCtx) ->
    {ok, worker_state()} | {error, _Error}.

-callback handle_call(_, _ID, _Call, _ReqCtx, worker_state()) ->
    {{reply, _Reply} | noreply, worker_state()}.

-callback handle_unload(_, _ID, _State) ->
    ok.

%%

-spec child_spec(_ID, options()) ->
    supervisor:child_spec().
child_spec(ID, Options) ->
    #{
        id       => ID,
        start    => {?MODULE, start_link, [Options, ID]},
        restart  => permanent,
        shutdown => brutal_kill
    }.

-spec start_link(options(), _ID) ->
    mg_utils:gen_start_ret().
start_link(Options, ID) ->
    raft_server:start_link(self_reg_name(Options, ID), {?MODULE, {ID, Options}}, raft_options(Options, ID)).

-spec call(options(), _ID, _Call, _ReqCtx, mg_utils:deadline()) ->
    _Result | {error, _}.
call(Options = #{raft := #{rpc := RPC}}, ID, Call, ReqCtx, Deadline) ->
    raft_server:send_command(
        RPC,
        cluster_with_id(Options, ID),
        undefined,
        {call, Deadline, Call, ReqCtx},
        genlib_retry:linear({max_total_timeout, 1000}, 100)
    ).

%%
%% raft_server
%%
-type command() :: {call, mg_utils:deadline(), _Call, _ReqCtx}.
-type status () :: loading | {working, worker_state()}.
-type state  () :: #{
    status      => status(),
    unload_tref => reference() | undefined
}.
-type delta  () :: worker_state().

-spec init(_) ->
    state().
init(_) ->
    #{
        status      => loading,
        unload_tref => undefined
    }.

-spec handle_election({_ID, options()}, state()) ->
    {delta() | undefined, state()}.
handle_election({_, Options}, State) ->
    {undefined, schedule_unload_timer(Options, State)}.

-spec handle_async_command(_, raft_rpc:request_id(), _, state()) ->
    {raft_server:reply_action(), state()}.
handle_async_command(_, _, AsyncCmd, State) ->
    ok = error_logger:error_msg("unexpected async_command received: ~p", [AsyncCmd]),
    {noreply, State}.

-spec handle_command({_, options()}, raft_rpc:request_id(), command(), state()) ->
    {raft_server:reply_action(), delta() | undefined, state()}.
handle_command({ID, Options = #{worker := Worker}}, ReqID, Cmd = {call, _, _, ReqCtx}, State = #{status := loading}) ->
    case mg_utils:apply_mod_opts(Worker, handle_load, [ID, ReqCtx]) of
        {ok, NewWorkerState} ->
            handle_command({ID, Options}, ReqID, Cmd, State#{status := {working, NewWorkerState}});
        Error = {error, _} ->
            {{reply, Error}, undefined, schedule_unload_timer(Options, State)}
    end;
handle_command({ID, Options = #{worker := Worker}}, _, Call, State = #{status := {working, WorkerState}}) ->
    {call, Deadline, CallBody, ReqCtx} = Call,
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            {ReplyAction, NewWorkerState} =
                mg_utils:apply_mod_opts(Worker, handle_call, [ID, CallBody, ReqCtx, WorkerState]),
            {ReplyAction, NewWorkerState, schedule_unload_timer(Options, State)};
        true ->
            ok = error_logger:warning_msg("rancid worker call received: ~p", [Call]),
            {noreply, undefined, schedule_unload_timer(Options, State)}
    end.

-spec handle_info({_, options()}, _Info, state()) ->
    {delta(), state()}.
handle_info({ID, Options}, {timeout, TRef, unload}, State = #{unload_tref := TRef}) ->
    #{unload_fun := UnloadFun, worker := Worker} = Options,
    % приготовления перед смертью
    case State of
        #{status := {working, WorkerState}} ->
            _ = (catch mg_utils:apply_mod_opts(Worker, handle_unload, [ID, WorkerState]));
        #{status := _} ->
            ok
    end,
    % вызов приведёт к самоубийству
    _ = UnloadFun(),
    {undefined, State}.

-spec apply_delta(_, raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(_, _, WorkerState, State) ->
    State#{status := {working, WorkerState}}.

%%

-spec self_reg_name(options(), _ID) ->
    mg_utils:gen_reg_name().
self_reg_name(#{namespace := NS}, ID) ->
    {via, gproc, {n, l, {?MODULE, NS, ID}}}.

-spec raft_options(options(), _ID) ->
    raft_server:options().
raft_options(Options = #{raft := RaftOptions = #{self := SelfNode}}, ID) ->
    RaftOptions#{
        self    := {self_reg_name(Options, ID), SelfNode},
        cluster := cluster_with_id(Options, ID)
    }.

% делается предположение, что у всех endpoint'ов вид {Name, Node}
-spec cluster_with_id(options(), _ID) ->
    raft_rpc:endpoint().
cluster_with_id(Options = #{raft := #{cluster := Cluster}}, ID) ->
    [{self_reg_name(Options, ID), Node} || Node <- Cluster].

-spec unload_timeout(options()) ->
    timeout().
unload_timeout(Options) ->
    maps:get(unload_timeout, Options, 5000).

-spec schedule_unload_timer(options(), state()) ->
    state().
schedule_unload_timer(Options, State = #{unload_tref := UnloadTRef}) ->
    _ = case UnloadTRef of
            undefined -> ok;
            TRef      -> erlang:cancel_timer(TRef)
        end,
    State#{unload_tref := start_timer(Options)}.

-spec start_timer(options()) ->
    reference().
start_timer(Options) ->
    erlang:start_timer(unload_timeout(Options), erlang:self(), unload).
