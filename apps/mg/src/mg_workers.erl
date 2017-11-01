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
    raft           => raft_server:options()
}.
-define(leader_group_size, 1). % TODO get from conf

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
start_link(Options = #{raft := #{cluster := Cluster, self := SelfNode}}) ->
    AllGroups = groups(Cluster, ?leader_group_size),
    {LocalGroupsIDs, _} =
        lists:unzip(
            lists:filter(
                fun({_, Group}) ->
                    lists:member(SelfNode, Group)
                end,
                lists:zip(lists:seq(1, erlang:length(AllGroups)), AllGroups)
            )
        ),

    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        [raft_supervisor_child_spec(Options, GroupID) || GroupID <- LocalGroupsIDs]
    ).

-spec raft_supervisor_child_spec(options(), group_id()) ->
    supervisor:child_spec().
raft_supervisor_child_spec(Options, GroupID) ->
    SupStartArgs =
        [
            sup_raft_server_name(Options, GroupID),
            sup_name(Options, GroupID),
            sup_raft_options(Options, GroupID)
        ],
    #{
        id       => {group, GroupID},
        start    => {mg_workers_raft_supervisor, start_link, SupStartArgs},
        restart  => permanent,
        type     => supervisor
    }.

-spec call(options(), _ID, _Call, _ReqCtx, mg_utils:deadline()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline) ->
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
    mg_workers_raft_supervisor:is_started(sup_raft_options(Options, worker_id_to_group_id(ID)), ID).

-spec load(options(), _ID) ->
    ok.
load(Options, ID) ->
    _ = mg_workers_raft_supervisor:start_child(
            sup_raft_options(Options, worker_id_to_group_id(ID)),
            mg_worker:child_spec(ID, worker_options(Options, ID))
        ),
    ok.

-spec unload(options(), _ID) ->
    ok.
unload(Options, ID) ->
    _ = mg_workers_raft_supervisor:stop_child(sup_raft_options(Options, worker_id_to_group_id(ID)), ID),
    ok.

%%

-spec sup_raft_server_name(options(), group_id()) ->
    raft_utils:gen_reg_name().
sup_raft_server_name(#{namespace := NS}, GroupID) ->
    {via, gproc, gproc_key({?MODULE, raft_server, NS, GroupID})}.

-spec sup_name(options(), group_id()) ->
    raft_utils:gen_reg_name().
sup_name(#{namespace := NS}, GroupID) ->
    {via, gproc, gproc_key({?MODULE, supervisor, NS, GroupID})}.

-spec gproc_key(term()) ->
    gproc:key().
gproc_key(Term) ->
    {n, l, Term}.

-spec sup_raft_options(options(), group_id()) ->
    raft_server:options().
sup_raft_options(Options = #{raft := RaftOptions = #{cluster := Cluster, self := SelfNode}}, GroupID) ->
    Group = group(Cluster, ?leader_group_size, GroupID),
    RaftOptions#{
        self    := {sup_raft_server_name(Options, GroupID), SelfNode},
        cluster := [{sup_raft_server_name(Options, GroupID), Node} || Node <- Group]
    }.

-spec worker_options(options(), _) ->
    mg_worker:options().
worker_options(Options, ID) ->
    #{worker_options := WorkerOptions, namespace := NS, raft := Raft = #{cluster := Cluster}} = Options,
    WorkerOptions#{
        namespace  => NS,
        unload_fun => fun() -> unload(Options, ID) end,
        raft       => Raft#{cluster := group(Cluster, ?leader_group_size, worker_id_to_group_id(ID))}
    }.

-spec worker_id_to_group_id(_ID) ->
    group_id().
worker_id_to_group_id(ID) ->
    Bin = erlang:term_to_binary(ID),
    BitSize = erlang:size(Bin) * 8,
    <<N:BitSize/integer>> = Bin,
    N rem ?leader_group_size + 1.

%%
%% leader groups
%%
-type group_id() :: pos_integer().

-spec group([Element], pos_integer(), pos_integer()) ->
    [Element].
group(Cluster, GroupSize, GroupNumber) ->
    Groups = groups(Cluster, GroupSize),
    lists:nth(GroupNumber rem erlang:length(Groups) + 1, Groups).

-spec groups([Element], non_neg_integer()) ->
    [Element].
groups(L, 1) ->
    [[E] || E <- L];
groups(L, S) when length(L) =:= S ->
    [L];
groups([H | T], S) ->
    [[H | L] || L <- groups(T, S - 1)] ++ groups(T, S).
