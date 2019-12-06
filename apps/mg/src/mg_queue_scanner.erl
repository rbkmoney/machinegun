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

%%% Queue scanner is responsible for grabbing tasks from a persistent store and distributing them
%%% among a set of schedulers.
%%%
%%% Queue scanners on some set of nodes organize into a squad, so that there's (at least) one
%%% process responsible for scanning a store, thus there's no statically designated leader, members
%%% are free to come and go as they like.
%%%
%%% Distribution process DOES NOT take into account processing locality (_allocate tasks near
%%% idling machines_), it just splits tasks uniformly among a set of known schedulers.

-module(mg_queue_scanner).

-type scheduler_id() :: mg_scheduler:id().
-type scan_delay()   :: milliseconds().
-type scan_limit()   :: non_neg_integer().
-type scan_ahead()   :: {_A :: float(), _B :: scan_limit()}. % as in A×X + B

-type milliseconds()  :: non_neg_integer().

-type options() :: #{
    queue_handler    := queue_handler(),
    max_scan_limit   => scan_limit() | unlimited,
    scan_ahead       => scan_ahead(),
    retry_scan_delay => scan_delay(),
    squad_opts       => mg_gen_squad:opts(),
    pulse            => mg_pulse:handler()
}.

-export_type([options/0]).
-export_type([queue_handler/0]).
-export_type([scan_delay/0]).
-export_type([scan_limit/0]).
-export_type([scan_ahead/0]).

-type beat() :: {squad, {atom(), mg_gen_squad_pulse:beat(), _ExtraMeta}}.
-export_type([beat/0]).

%%

-type task() :: mg_queue_task:task().

-type queue_state() :: any().
-type queue_options() :: any().
-type queue_handler() :: mg_utils:mod_opts(queue_options()).

-callback child_spec(queue_options(), atom()) -> supervisor:child_spec() | undefined.
-callback init(queue_options()) -> {ok, queue_state()}.
-callback search_tasks(Options, Limit, State) -> {{Delay, Tasks}, State} when
    Options :: queue_options(),
    Limit   :: scan_limit(),
    Tasks   :: [task()],
    Delay   :: scan_delay(),
    State   :: queue_state().

-optional_callbacks([child_spec/2]).

%%

-define(DEFAULT_MAX_LIMIT        , unlimited).
-define(DEFAULT_SCAN_AHEAD       , {1.0, 0}).
-define(DEFAULT_RETRY_SCAN_DELAY , 1000).

-define(DISCOVER_TIMEOUT , 1000).
-define(INQUIRY_TIMEOUT  , 1000).

-export([child_spec/3]).
-export([start_link/2]).
-export([where_is/1]).

-behaviour(mg_gen_squad).
-export([init/1]).
-export([discover/1]).
-export([handle_rank_change/3]).
-export([handle_call/5]).
-export([handle_cast/4]).
-export([handle_info/4]).

-behaviour(mg_gen_squad_pulse).
-export([handle_beat/2]).

%%

-spec child_spec(scheduler_id(), options(), _ChildID) ->
    supervisor:child_spec().
child_spec(SchedulerID, Options, ChildID) ->
    mg_utils_supervisor_wrapper:child_spec(
        #{strategy => rest_for_one},
        mg_utils:lists_compact([
            handler_child_spec(Options, {ChildID, handler}),
            #{
                id       => {ChildID, scanner},
                start    => {?MODULE, start_link, [SchedulerID, Options]},
                restart  => permanent,
                type     => worker
            }
        ]),
        ChildID
    ).

-spec handler_child_spec(options(), _ChildID) ->
    supervisor:child_spec() | undefined.
handler_child_spec(#{queue_handler := Handler}, ChildID) ->
    mg_utils:apply_mod_opts_if_defined(Handler, child_spec, undefined, [ChildID]).

%%

-spec start_link(scheduler_id(), options()) ->
    mg_utils:gen_start_ret().
start_link(SchedulerID, Options) ->
    SquadOpts = maps:merge(
        maps:get(squad_opts, Options, #{}),
        maps:map(
            fun (pulse, Pulse) -> {?MODULE, {Pulse, SchedulerID}} end,
            maps:with([pulse], Options)
        )
    ),
    mg_gen_squad:start_link(self_reg_name(SchedulerID), ?MODULE, {SchedulerID, Options}, SquadOpts).

-spec where_is(scheduler_id()) ->
    pid() | undefined.
where_is(SchedulerID) ->
    mg_utils:gen_ref_to_pid(self_ref(SchedulerID)).

%%

-type queue_handler_state() :: {queue_handler(), queue_state()}.

-record(st, {
    scheduler_id  :: scheduler_id(),
    queue_handler :: queue_handler_state(),
    max_limit     :: scan_limit(),
    scan_ahead    :: scan_ahead(),
    retry_delay   :: scan_delay(),
    timer         :: reference() | undefined,
    pulse         :: mg_pulse:handler() | undefined
}).

-type st() :: #st{}.

-type rank() :: mg_gen_squad:rank().
-type squad() :: mg_gen_squad:squad().

-spec init({scheduler_id(), options()}) ->
    {ok, st()}.
init({SchedulerID, Options}) ->
    {ok, #st{
        scheduler_id  = SchedulerID,
        queue_handler = init_handler(maps:get(queue_handler, Options)),
        max_limit     = maps:get(max_scan_limit, Options, ?DEFAULT_MAX_LIMIT),
        scan_ahead    = maps:get(scan_ahead, Options, ?DEFAULT_SCAN_AHEAD),
        retry_delay   = maps:get(retry_scan_delay, Options, ?DEFAULT_RETRY_SCAN_DELAY),
        pulse         = maps:get(pulse, Options, undefined)
    }}.

-spec discover(st()) ->
    {ok, [pid()], st()}.
discover(St = #st{scheduler_id = SchedulerID}) ->
    Nodes = erlang:nodes(),
    Pids = multicall(Nodes, ?MODULE, where_is, [SchedulerID], ?DISCOVER_TIMEOUT),
    {ok, lists:filter(fun erlang:is_pid/1, Pids), St}.

-spec handle_rank_change(rank(), squad(), st()) ->
    {noreply, st()}.
handle_rank_change(leader, Squad, St) ->
    % NOTE
    % Starting right away.
    % This may cause excessive storage resource usage if leader is flapping frequently enough.
    % However, it's hard for me to devise a scenario which would cause such flapping.
    {noreply, handle_scan(Squad, St)};
handle_rank_change(follower, _Squad, St) ->
    % No more scanning for you today.
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
    ok = logger:warning(
        "unexpected mg_gen_squad info received: ~p, rank ~p, state ~p",
        [Info, Rank, St]
    ),
    {noreply, St}.


-spec handle_scan(mg_gen_squad:squad(), st()) ->
    st().
handle_scan(Squad, St0 = #st{max_limit = MaxLimit, retry_delay = RetryDelay}) ->
    StartedAt = erlang:monotonic_time(),
    %% Try to find out which schedulers are here, getting their statuses
    case inquire_schedulers(Squad, St0) of
        Schedulers = [_ | _] ->
            %% Compute total limit given capacity left on each scheduler
            Capacities = [compute_adjusted_capacity(S, St0) || S <- Schedulers],
            Limit = erlang:min(lists:sum(Capacities), MaxLimit),
            {{Delay, Tasks}, St1} = scan_queue(Limit, St0),
            %% Distribute tasks taking into account respective capacities
            ok = disseminate_tasks(Tasks, Schedulers, Capacities, St1),
            start_timer(StartedAt, Delay, St1);
        [] ->
            %% No one to distribute tasks to, let's retry
            start_timer(StartedAt, RetryDelay, St0)
    end.

-spec scan_queue(scan_limit(), st()) ->
    {{scan_delay(), [task()]}, st()}.
scan_queue(Limit, St = #st{queue_handler = HandlerState, retry_delay = RetryDelay}) ->
    StartedAt = erlang:monotonic_time(),
    {Result, HandlerStateNext} = try
        run_handler(HandlerState, search_tasks, [Limit])
    catch
        throw:({ErrorType, _Details} = Reason):Stacktrace when
            ErrorType =:= transient orelse
            ErrorType =:= timeout
        ->
            ok = emit_scan_error_beat({throw, Reason, Stacktrace}, St),
            {{RetryDelay, []}, HandlerState}
    end,
    ok = emit_scan_success_beat(Result, Limit, StartedAt, St),
    {Result, St#st{queue_handler = HandlerStateNext}}.

-spec disseminate_tasks([task()], [mg_scheduler:status()], [scan_limit()], st()) ->
    ok.
disseminate_tasks(Tasks, [_Scheduler = #{pid := Pid}], _Capacities, _St) ->
    %% A single scheduler, just send him all tasks optimizing away meaningless partitioning
    mg_scheduler:distribute_tasks(Pid, Tasks);
disseminate_tasks(Tasks, Schedulers, Capacities, _St) ->
    %% Partition tasks among known schedulers proportionally to their capacities
    Partitions = mg_utils:partition(Tasks, lists:zip(Schedulers, Capacities)),
    %% Distribute shares of tasks among schedulers, sending directly to pids
    maps:fold(
        fun (_Scheduler = #{pid := Pid}, TasksShare, _) ->
            mg_scheduler:distribute_tasks(Pid, TasksShare)
        end,
        ok,
        Partitions
    ).

-spec inquire_schedulers(mg_gen_squad:squad(), st()) ->
    [mg_scheduler:status()].
inquire_schedulers(Squad, #st{scheduler_id = SchedulerID}) ->
    %% Take all known members, there's at least one which is `self()`
    Members = mg_gen_squad:members(Squad),
    Nodes = lists:map(fun erlang:node/1, Members),
    multicall(Nodes, mg_scheduler, inquire, [SchedulerID], ?INQUIRY_TIMEOUT).

-spec compute_adjusted_capacity(mg_scheduler:status(), st()) ->
    scan_limit().
compute_adjusted_capacity(#{waiting_tasks := W, capacity := C}, #st{scan_ahead = {A, B}}) ->
    erlang:max(erlang:round(A * erlang:max(C - W, 0)) + B, 0).

%%

-spec multicall([node()], module(), _Function :: atom(), _Args :: list(), timeout()) ->
    [_Result].
multicall(Nodes, Module, Function, Args, Timeout) ->
    {Results, BadNodes} = rpc:multicall(Nodes, Module, Function, Args, Timeout),
    _ = BadNodes == [] orelse
        logger:warning(
            "error making rpc ~p:~p(~p) to non-existent nodes: ~p",
            [Module, Function, Args, BadNodes]
        ),
    lists:filter(
        fun
            ({badrpc, Reason}) ->
                % Yeah, we don't know offending node here. Cool, huh?
                _ = logger:warning(
                    "error making rpc ~p:~p(~p): ~p",
                    [Module, Function, Args, Reason]
                ),
                false;
            (_) ->
                true
        end,
        Results
    ).

%%

-spec start_timer(integer(), scan_delay(), st()) ->
    st().
start_timer(RefTime, Delay, St) ->
    FireTime = erlang:convert_time_unit(RefTime, native, millisecond) + Delay,
    St#st{timer = erlang:send_after(FireTime, self(), scan, [{abs, true}])}.

-spec cancel_timer(st()) ->
    st().
cancel_timer(St = #st{timer = TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    St#st{timer = undefined};
cancel_timer(St) ->
    St.

%%

-spec init_handler(queue_handler()) ->
    queue_handler_state().
init_handler(Handler) ->
    {ok, InitialState} = mg_utils:apply_mod_opts(Handler, init),
    {Handler, InitialState}.

-spec run_handler(queue_handler_state(), _Function :: atom(), _Args :: list()) ->
    {_Result, queue_handler_state()}.
run_handler({Handler, State}, Function, Args) ->
    {Result, NextState} = mg_utils:apply_mod_opts(Handler, Function, Args ++ [State]),
    {Result, {Handler, NextState}}.

%%

-spec self_reg_name(scheduler_id()) ->
    mg_procreg:reg_name().
self_reg_name(SchedulerID) ->
    mg_procreg:reg_name(mg_procreg_gproc, {?MODULE, SchedulerID}).

-spec self_ref(scheduler_id()) ->
    mg_procreg:ref().
self_ref(SchedulerID) ->
    mg_procreg:ref(mg_procreg_gproc, {?MODULE, SchedulerID}).

%%

-include_lib("mg/include/pulse.hrl").

-spec emit_scan_error_beat(mg_utils:exception(), st()) ->
    ok.
emit_scan_error_beat(Exception, #st{pulse = Pulse, scheduler_id = {Name, NS}}) ->
    mg_pulse:handle_beat(Pulse, #mg_scheduler_search_error{
        namespace = NS,
        scheduler_name = Name,
        exception = Exception
    }).

-spec emit_scan_success_beat({scan_delay(), [task()]}, scan_limit(), integer(), st()) ->
    ok.
emit_scan_success_beat({Delay, Tasks}, Limit, StartedAt, #st{pulse = Pulse, scheduler_id = {Name, NS}}) ->
    mg_pulse:handle_beat(Pulse, #mg_scheduler_search_success{
        namespace = NS,
        scheduler_name = Name,
        delay = Delay,
        tasks = Tasks,
        limit = Limit,
        duration = erlang:monotonic_time() - StartedAt
    }).

%%

-spec handle_beat({mg_pulse:handler(), scheduler_id()}, mg_gen_squad_pulse:beat()) ->
    _.
handle_beat({Handler, {Name, NS}}, Beat) ->
    Producer = queue_scanner,
    Extra = [{scheduler_type, Name}, {namespace, NS}],
    mg_pulse:handle_beat(Handler, {squad, {Producer, Beat, Extra}}).
