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
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:{loading, Error}
%%%
%%% TODO:
%%%  - сделать выгрузку не по таймеру, а по занимаемой памяти и времени последней активности
%%%
-module(mg_workers).

%% API
-export_type([options/0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([call      /5]).
-export([is_loaded /2]).
-export([load      /2]).
-export([unload    /2]).

%%
%% API
%%
-type options() :: #{
    namespace      => _,
    worker_options => mg_worker:options(),
    raft           => raft:options()
}.

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    raft_supervisor:start_link(raft_server_name(Options), sup_name(Options), raft_options(Options)).

-spec call(options(), _ID, _Call, _ReqCtx, mg_utils:deadline()) ->
    _Reply | {error, _}.
call(Options = #{}, ID, Call, ReqCtx, Deadline) ->
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            ok = load_worker_if_needed(Options, ID),
            mg_worker:call(worker_options(Options, ID), ID, Call, ReqCtx, Deadline);
        true ->
            {error, {timeout, worker_call_deadline_reached}}
    end.

-spec load_worker_if_needed(options(), _ID) ->
    ok.
load_worker_if_needed(Options, ID) ->
    case is_loaded(Options, ID) of
        true  -> ok;
        false -> _ = load(Options, ID)
    end.

-spec is_loaded(options(), _ID) ->
    boolean().
is_loaded(Options, ID) ->
    raft_supervisor:is_started(raft_options(Options), ID).

-spec load(options(), _ID) ->
    ok.
load(Options, ID) ->
    _ = raft_supervisor:start_child(raft_options(Options), mg_worker:child_spec(ID, worker_options(Options, ID))),
    ok.

-spec unload(options(), _ID) ->
    ok.
unload(Options, ID) ->
    _ = raft_supervisor:stop_child(raft_options(Options), ID),
    ok.

%%

-spec raft_server_name(options()) ->
    raft_utils:gen_reg_name().
raft_server_name(#{namespace := NS}) ->
    {via, gproc, gproc_key({?MODULE, raft_server, NS})}.

-spec sup_name(options()) ->
    raft_utils:gen_reg_name().
sup_name(#{namespace := NS}) ->
    {via, gproc, gproc_key({?MODULE, supervisor, NS})}.

-spec gproc_key(term()) ->
    gproc:key().
gproc_key(Term) ->
    {n, l, Term}.

-spec raft_options(options()) ->
    raft:options().
raft_options(Options = #{raft := RaftOptions = #{cluster := Cluster, self := SelfNode}}) ->
    RaftOptions#{
        self    := {raft_server_name(Options), SelfNode},
        cluster := [{raft_server_name(Options), Node} || Node <- Cluster]
    }.

-spec worker_options(options(), _) ->
    mg_worker:options().
worker_options(Options = #{worker_options := WorkerOptions, namespace := NS, raft := Raft}, ID) ->
    WorkerOptions#{
        namespace  => NS,
        unload_fun => fun() -> unload(Options, ID) end,
        raft       => Raft
    }.
