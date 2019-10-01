%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_queue_scanner).

-type milliseconds() :: pos_integer().

-type name() :: mg_procreg:name().
-type scan_status() :: continue | completed.
-type scan_interval() :: #{scan_status() => milliseconds()}.

-type options() :: #{
    scheduler     := mg_procreg:ref(),
    queue_handler := mg_utils:mod_opts(),
    interval      => scan_interval(),
    squad_opts    => mg_gen_squad:opts()
}.

-define(DEFAULT_INTERVAL, #{
    continue  => 1000, % 1 second
    completed => 1000  % 1 second
}).

-export([child_spec/3]).
-export([start_link/2]).

-behaviour(mg_gen_squad).
-export([init/1]).
-export([discover/1]).
-export([handle_rank_change/3]).
-export([handle_call/5]).
-export([handle_cast/4]).
-export([handle_info/4]).

%%

-spec child_spec(name(), options(), _ChildID) ->
    supervisor:child_spec().
child_spec(Name, Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Name, Options]},
        restart  => permanent,
        type     => worker
    }.

-spec start_link(name(), options()) ->
    mg_utils:gen_start_ret().
start_link(Name, Options) ->
    SquadOpts = maps:get(squad_opts, Options, #{}),
    mg_gen_squad:start_link(reg_name(Name), ?MODULE, {Name, Options}, SquadOpts).

%%

-record(st, {
    name          :: name(),
    scheduler     :: mg_procreg:ref(),
    queue_handler :: mg_utils:mod_opts(),
    interval      :: scan_interval(),
    timer         :: reference() | undefined
}).

-type st() :: #st{}.

-spec init({name(), options()}) ->
    {ok, st()}.
init({Name, Options}) ->
    QueueHandler = {Module, _} = mg_utils:separate_mod_opts(maps:get(queue_handler, Options)),
    QueueState = mg_utils:apply_mod_opts(QueueHandler, init),
    #st{
        name          = Name,
        scheduler     = maps:get(scheduler, Options),
        queue_handler = {Module, QueueState},
        interval      = maps:merge(?DEFAULT_INTERVAL, maps:get(interval, Options, #{}))
    }.

-spec discover(st()) ->
    {ok, [pid()], st()}.
discover(St = #st{name = Name}) ->
    Nodes = erlang:nodes(),
    Timeout = 1000,
    {Pids, _} = rpc:multicall(Nodes, mg_utils, gen_ref_to_pid, [ref(Name)], Timeout),
    true = lists:all(fun erlang:is_pid/1, Pids),
    {ok, Pids, St}.

-spec handle_rank_change(mg_gen_squad:rank(), mg_gen_squad:squad(), st()) ->
    {noreply, st()}.
handle_rank_change(leader, Squad, St) ->
    % well then start right away
    {noreply, handle_scan(Squad, St)};
handle_rank_change(follower, _Squad, St) ->
    % no more scanning for you today
    {noreply, cancel_timer(St)}.

-type info() :: scan.

-spec handle_info(info(), mg_gen_squad:rank(), mg_gen_squad:squad(), st()) ->
    {noreply, st()}.
handle_info(scan, leader, Squad, St) ->
    St1 = scan_queue(St0),
    St2 = start_timer(ScanStatus, St0),
    {noreply, St2};

-spec handle_scan(mg_gen_squad:squad(), st()) ->
    st().
handle_scan(Squad, St0) ->
    St1 = scan_queue(St0),
    St2 = start_timer(ScanStatus, St0),
    St2.

%%

-spec cancel_timer(st()) ->
    st().
cancel_timer(St = #st{timer = TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    St#st{timer = undefined};
cancel_timer(St) ->
    St.

-spec start_timer(scan_status(), st()) ->
    st().
start_timer(Status, St = #st{interval = Interval}) ->
    Timeout = maps:get(Status, Interval),
    St#st{timer = erlang:send_after(Timeout, self(), scan)}.

%%

-spec reg_name(mg_procreg:name()) ->
    mg_procreg:reg_name().
reg_name(Name) ->
    mg_procreg:reg_name(mg_procreg_gproc, Name).

-spec ref(mg_procreg:name()) ->
    mg_procreg:ref().
ref(Name) ->
    mg_procreg:ref(mg_procreg_gproc, Name).
