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

%%% We

-module(mg_queue_scanner).

-type milliseconds() :: pos_integer().

-type name() :: mg_procreg:name().
-type scan_status() :: continue | completed.
-type scan_interval() :: #{scan_status() => milliseconds()}.

-type options() :: #{
    scheduler     := mg_procreg:ref(),
    queue_handler := queue_handler(),
    interval      => scan_interval(),
    squad_opts    => mg_gen_squad:opts(),
    pulse         => mg_pulse:handler()
}.

-export_type([options/0]).
-export_type([scan_status/0]).

%%

-type task_id() :: any().
-type task_payload() :: any().
-type queue_state() :: any().
-type queue_options() :: any().
-type queue_handler() :: mg_utils:mod_opts(queue_options()).

-type task(TaskID, TaskPayload) :: #{
    id          := TaskID,
    payload     := TaskPayload,
    created_at  := integer(),  % erlang monotonic time
    target_time => genlib_time:ts(),  % unix timestamp in seconds
    machine_id  => mg:id()
}.

-type task() :: task(task_id(), task_payload()).

-callback child_spec(queue_options(), atom()) -> supervisor:child_spec() | undefined.
-callback init(queue_options()) -> {ok, queue_state()}.
-callback search_new_tasks(Options, Limit, State) -> {ok, Status, Result, State} when
    Options :: queue_options(),
    Limit :: non_neg_integer(),
    Result :: [task()],
    Status :: scan_status(),
    State :: queue_state().

-optional_callbacks([child_spec/2]).

-export_type([task/2]).
-export_type([task/0]).

%%

-define(DEFAULT_INTERVAL, #{
    continue  => 1000, % 1 second
    completed => 1000  % 1 second
}).

-define(DISCOVER_TIMEOUT , 1000).
-define(INQUIRY_TIMEOUT  , 1000).

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
    queue_handler :: mg_utils:mod_opts(queue_state()),
    interval      :: scan_interval(),
    timer         :: reference() | undefined,
    pulse         :: mg_pulse:handler() | undefined
}).

-type st() :: #st{}.

-type rank() :: mg_gen_squad:rank().
-type squad() :: mg_gen_squad:squad().

-spec init({name(), options()}) ->
    {ok, st()}.
init({Name, Options}) ->
    #st{
        name          = Name,
        scheduler     = maps:get(scheduler, Options),
        queue_handler = init_handler(maps:get(queue_handler, Options)),
        interval      = maps:merge(?DEFAULT_INTERVAL, maps:get(interval, Options, #{})),
        pulse         = maps:get(pulse, Options, undefined)
    }.

-spec discover(st()) ->
    {ok, [pid()], st()}.
discover(St = #st{name = Name}) ->
    Nodes = erlang:nodes(),
    Pids = multicall(Nodes, mg_utils, gen_ref_to_pid, [ref(Name)], ?DISCOVER_TIMEOUT),
    {ok, lists:filter(fun erlang:is_pid/1, Pids), St}.

-spec handle_rank_change(rank(), squad(), st()) ->
    {noreply, st()}.
handle_rank_change(leader, Squad, St) ->
    % well then start right away
    {noreply, handle_scan(Squad, St)};
handle_rank_change(follower, _Squad, St) ->
    % no more scanning for you today
    {noreply, cancel_timer(St)}.

-spec handle_cast(_Cast, rank(), squad(), st()) ->
    {noreply, st()}.
handle_cast(Cast, Rank, _Squad, St) ->
    ok = logger:error(
        "unexpected mg_gen_squad cast received: ~p, from ~p, rank ~p, state ~p",
        [Cast, Rank, St]
    ),
    {noreply, St}.

-spec handle_call(_Call, mg_utils:gen_server_from(), rank(), squad(), st()) ->
    {noreply, st()}.
handle_call(Call, From, Rank, _Squad, St) ->
    ok = logger:error(
        "unexpected mg_gen_squad call received: ~p, from ~p, rank ~p, state ~p",
        [Call, From, Rank, St]
    ),
    {noreply, St}.

-type info() :: scan.

-spec handle_info(info(), rank(), squad(), st()) ->
    {noreply, st()}.
handle_info(scan, leader, Squad, St) ->
    {noreply, handle_scan(Squad, St)};
handle_info(scan, follower, _Squad, St) ->
    {noreply, St};
handle_info(Info, Rank, _Squad, St) ->
    ok = logger:error(
        "unexpected mg_gen_squad info received: ~p, rank ~p, state ~p",
        [Info, Rank, St]
    ),
    {noreply, St}.


-spec handle_scan(mg_gen_squad:squad(), st()) ->
    st().
handle_scan(Squad, St0) ->
    ScanStartedAt = now_ms(),
    {{ScanStatus, Tasks}, St1} = scan_queue(St0),
    ok = disseminate_tasks(Tasks, Squad, St1),
    start_timer(ScanStartedAt, ScanStatus, St1).

-spec scan_queue(st()) ->
    {{scan_status(), [task()]}, st()}.
scan_queue(St = #st{queue_handler = Handler, pulse = Pulse}) ->
    {Result, Handler1} = try
        run_handler(Handler, search_tasks, [])
    catch
        throw:({ErrorType, _Details} = Reason):Stacktrace when
            ErrorType =:= transient orelse
            ErrorType =:= timeout
        ->
            ok = mg_pulse:handle_beat(Pulse, {search, {failed, {throw, Reason, Stacktrace}}}),
            {{continue, []}, Handler}
    end,
    {Result, St#st{queue_handler = Handler1}}.

-spec disseminate_tasks([task()], squad(), st()) ->
    ok.
disseminate_tasks(Tasks, Squad, St) ->
    %% Take all known members, there's at least one which is `self()`
    Members = mg_gen_squad:members(Squad),
    %% Try to find out which schedulers are here, getting their pids
    Schedulers = inquire_schedulers(Members, St),
    %% Partition tasks uniformly among known schedulers
    Partitions = mg_utils:partition(Tasks, Schedulers),
    %% Distribute shares of tasks among schedulers
    maps:fold(
        fun (Scheduler, TasksShare, _) ->
            mg_scheduler:add_tasks(Scheduler, TasksShare)
        end,
        ok,
        Partitions
    ).

-spec inquire_schedulers([pid()], st()) ->
    [pid()].
inquire_schedulers(Members, #st{scheduler = SchedulerRef}) ->
    Nodes = lists:map(fun erlang:node/1, Members),
    multicall(Nodes, mg_utils, gen_ref_to_pid, [SchedulerRef], ?INQUIRY_TIMEOUT).

%%

-spec multicall([node()], module(), _Function :: atom(), _Args :: list(), timeout()) ->
    [_Result].
multicall(Nodes, Module, Function, Args, Timeout) ->
    {Results, BadNodes} = rpc:multicall(Nodes, Module, Function, Args, Timeout),
    _ = logger:warning("error making rpc to non-existent nodes: ~p", [BadNodes]),
    lists:filter(
        fun
            ({badrpc, Reason}) ->
                % Yeah, we don't know offending node here. Cool, huh?
                _ = logger:warning("error making rpc: ~p", [Reason]),
                false;
            (_) ->
                true
        end,
        Results
    ).

%%

-spec start_timer(integer(), scan_status(), st()) ->
    st().
start_timer(RefTime, Status, St = #st{interval = Interval}) ->
    Timeout = maps:get(Status, Interval),
    St#st{timer = erlang:send_after(RefTime + Timeout, self(), scan, [{abs, true}])}.

-spec cancel_timer(st()) ->
    st().
cancel_timer(St = #st{timer = TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    St#st{timer = undefined};
cancel_timer(St) ->
    St.

-spec now_ms() ->
    integer().
now_ms() ->
    erlang:monotonic_time(millisecond).

%%

-spec init_handler(mg_utils:mod_opts()) ->
    mg_utils:mod_opts().
init_handler(Handler) ->
    {Module, _} = mg_utils:separate_mod_opts(Handler),
    InitialSt = mg_utils:apply_mod_opts(Handler, init),
    {Module, InitialSt}.

-spec run_handler(mg_utils:mod_opts(T), _Function :: atom(), _Args :: list()) ->
    {_Result, mg_utils:mod_opts(T)}.
run_handler(Handler = {Module, _}, Function, Args) ->
    {Result, NextSt} = mg_utils:apply_mod_opts(Handler, Function, Args),
    {Result, {Module, NextSt}}.

%%

-spec reg_name(mg_procreg:name()) ->
    mg_procreg:reg_name().
reg_name(Name) ->
    mg_procreg:reg_name(mg_procreg_gproc, Name).

-spec ref(mg_procreg:name()) ->
    mg_procreg:ref().
ref(Name) ->
    mg_procreg:ref(mg_procreg_gproc, Name).
