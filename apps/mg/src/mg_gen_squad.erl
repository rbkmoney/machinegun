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

%%% Squad behaviour
%%%
%%% A squad is a group of processes (usually distributed over a cluster of
%%% nodes) with dynamic membership which choose a leader among themselves
%%% in a simple yet only eventually consistent manner. Implementation is
%%% is simple: everyone knows everyone else and sends them heartbeats,
%%% so it's not well suited for large clusters (more than 10 nodes).
%%%
%%% TODO
%%%  - Streamline event interface
%%%  - Do we even need monitors?
%%%  - More tests
%%%
-module(mg_gen_squad).

%%

-export([leader/1]).
-export([members/1]).

-type rank()      :: leader | follower.
-type age()       :: pos_integer().
-type timestamp() :: integer().

-opaque squad() :: #{ % presumably _always_ nonempty
    pid() => member()
}.

-type member() :: #{
    age             => age(),
    last_contact    => timestamp(),
    heartbeat_timer => reference(), % when self()
    loss_timer      => reference(),
    monitor         => reference()
}.

-type from() :: {pid(), _}.
-type vsn()  :: _Vsn | {down, _Vsn}.

-type reason() ::
    normal | shutdown | {shutdown, _} | _.

-type reply(Reply, St) ::
    {reply, Reply, St} |
    {reply, Reply, St, _Timeout :: pos_integer() | hibernate} |
    noreply(St).

-type noreply(St) ::
    {noreply, St} |
    {noreply, St, _Timeout :: pos_integer() | hibernate} |
    stop(St).

-type stop(St) ::
    {stop, _Reason, St}.

-callback init(_Args) ->
    {ok, _State} | ignore | {stop, _Reason}.

-callback discover(State) ->
    {ok, [pid()], State} | stop(State).

-callback handle_rank_change(rank(), squad(), State) ->
    noreply(State).

-callback handle_call(_Call, from(), rank(), squad(), State) ->
    reply(_Reply, State).

-callback handle_cast(_Cast, rank(), squad(), State) ->
    noreply(State).

-callback handle_info(_Info, rank(), squad(), State) ->
    noreply(State).

-callback terminate(reason(), _State) ->
    _.

-callback code_change(vsn(), State, _Extra) ->
    {ok, State} | {error, _Reason}.

-optional_callbacks([
    terminate/2,
    code_change/3
]).

-export_type([rank/0]).
-export_type([squad/0]).

-export([start_link/3]).
-export([start_link/4]).

%%

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%

-type milliseconds() :: pos_integer().

-type discovery_opts() :: #{
    initial_interval => milliseconds(), %  1000 by default
    refresh_interval => milliseconds()  % 60000 by default
}.

-type heartbeat_opts() :: #{
    broadcast_interval => milliseconds(), %  200 by default
    loss_timeout       => milliseconds()  % 1000 by default
}.

-type promotion_opts() :: #{
    min_squad_age => age() % 3 by default
}.

-type opts() :: #{
    discovery => discovery_opts(),
    heartbeat => heartbeat_opts(),
    promotion => promotion_opts(),
    pulse     => mg_gen_squad_pulse:handler()
}.

-export_type([opts/0]).

-spec start_link(module(), _Args, opts()) ->
    {ok, pid()} | ignore | {error, _}.
start_link(Module, Args, Opts) ->
    gen_server:start_link(?MODULE, mk_state(Module, Args, set_defaults(Opts)), []).

-spec start_link(mg_procreg:reg_name(), module(), _Args, opts()) ->
    {ok, pid()} | ignore | {error, _}.
start_link(RegName, Module, Args, Opts) ->
    gen_server:start_link(RegName, ?MODULE, mk_state(Module, Args, set_defaults(Opts)), []).

-spec start_sup(_ServerArgs :: list(), st(), heart()) ->
    {ok, pid()}.
start_sup(ServerArgs, ServerSt, HeartSt) ->
    Strategy = #{strategy => one_for_all, intensity => 0, period => 1},
    {ok, SupPid} = genlib_adhoc_supervisor:start_link(Strategy, []),
    % TODO we should handle errors here probably
    {ok, ServerPid} = supervisor:start_child(SupPid, #{
        id    => heart,
        start => {gen_server, start_link, [?MODULE, {server, ServerSt}, []]}
    }),
    {ok, _HeartPid} = supervisor:start_child(SupPid, #{
        id    => heart,
        start => {gen_server, start_link, [?MODULE, {heart, HeartSt, ServerPid}, []]}
    }),
    {ok, SupPid}.

-spec set_defaults(opts()) ->
    opts().
set_defaults(Opts) ->
    Defaults = #{
        discovery => #{
            initial_interval =>  1000,
            refresh_interval => 60000
        },
        heartbeat => #{
            broadcast_interval =>  400,
            loss_timeout       => 1000
        },
        promotion => #{
            min_squad_age => 3
        }
    },
    maps:fold(
        fun (K, V, M) ->
            M#{K => maps:merge(V, maps:get(K, Opts, #{}))}
        end,
        Opts,
        Defaults
    ).

%%

-spec leader(squad()) ->
    pid().
leader(Squad) ->
    [Leader | _] = lists:sort(maps:keys(Squad)), Leader.

-spec rank(pid(), squad()) ->
    rank().
rank(Pid, Squad) ->
    case leader(Squad) of
        Pid -> leader;
        _   -> follower
    end.

-spec members(squad()) ->
    [pid()].
members(Squad) ->
    maps:keys(maps:remove(self(), Squad)).

%%

-type st() :: #{
    squad    := squad(),
    rank     => rank(),
    modstate := {module(), _ArgsOrState},
    opts     := opts(),
    timers   := #{atom() => reference()}
}.

-spec mk_state(module(), _Args, opts()) ->
    st().
mk_state(Module, Args, Opts) ->
    #{
        squad    => #{},
        modstate => {Module, Args},
        opts     => Opts,
        timers   => #{}
    }.

-spec get_rank(st()) ->
    rank().
get_rank(St) ->
    maps:get(rank, St, follower).

-spec init(st()) ->
    {ok, st()} | ignore | {stop, _Reason}.
init(St0) ->
    case invoke_callback(init, [], St0) of
        {ok, St = #{squad := Squad, opts := Opts}} ->
            {ok, defer_discovery(St#{squad := add_member(self(), Squad, Opts)})};
        Ret ->
            Ret
    end.

-spec handle_call(_Call, from(), st()) ->
    reply(_, _).
handle_call(Call, From, St0 = #{squad := Squad}) ->
    invoke_callback(handle_call, [Call, From, get_rank(St0), Squad], try_cancel_st_timer(user, St0)).

-type cast() ::
    payload().

-spec handle_cast(cast(), st()) ->
    noreply(_).
handle_cast({?MODULE, Payload = #{vsn := 1, msg := _, from := _}}, St) ->
    _ = beat({{broadcast, Payload}, received}, St),
    handle_broadcast(Payload, St);
handle_cast(Cast, St0 = #{squad := Squad}) ->
    invoke_callback(handle_cast, [Cast, get_rank(St0), Squad], try_cancel_st_timer(user, St0)).

-spec handle_broadcast(payload(), st()) ->
    noreply(st()).
handle_broadcast(#{msg := howdy, from := Pid, members := Pids}, St = #{squad := Squad0, opts := Opts}) ->
    Squad = refresh_member(Pid, add_members([Pid | Pids], Squad0, Opts), Opts),
    % NOTE
    % Simple approach: retransmit another broadcast to those members we see for the first time.
    % It's possible to reduce message rate here at the expense of higher squad convergence time,
    % for example taking just half or some m << size(Squad) of new squad members randomly. Would
    % be better to consider some gossip protocol scheme instead though.
    ok = broadcast(howdy, [newbies(Squad0)], broadcast, Squad, Opts),
    try_update_squad(Squad, St).

-type timer() ::
    discover      |
    heartbeat     |
    {lost, pid()} |
    user.

-type info() ::
    {timeout, reference(), timer()} |
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    noreply(st()).
handle_info({timeout, TRef, Msg}, St) ->
    _ = beat({{timer, TRef}, {fired, Msg}}, St),
    handle_timeout(Msg, TRef, St);
handle_info({'DOWN', MRef, process, Pid, Reason}, St = #{squad := Squad, opts := Opts}) ->
    _ = beat({{monitor, MRef}, {fired, Pid, Reason}}, St),
    try_update_squad(handle_member_down(Pid, MRef, Reason, Squad, Opts), St);
handle_info(Info, St0 = #{squad := Squad}) ->
    invoke_callback(handle_info, [Info, get_rank(St0), Squad], try_cancel_st_timer(user, St0)).

-spec handle_timeout(_Msg, reference(), st()) ->
    st().
handle_timeout(discovery, TRef, St = #{timers := Timers0}) ->
    {TRef, Timers} = maps:take(discovery, Timers0),
    try_discover(St#{timers := Timers});
handle_timeout(heartbeat, TRef, St = #{squad := Squad, opts := Opts}) ->
    try_update_squad(handle_heartbeat(TRef, Squad, Opts), St);
handle_timeout({lost, Pid}, TRef, St = #{squad := Squad, opts := Opts}) ->
    try_update_squad(handle_loss_timeout(TRef, Pid, Squad, Opts), St);
handle_timeout(user, TRef, St = #{squad := Squad, timers := Timers0}) ->
    {TRef, Timers} = maps:take(user, Timers0),
    invoke_callback(handle_info, [timeout, get_rank(St), Squad], St#{timers := Timers}).

-spec restart_st_timer(atom(), pos_integer(), st()) ->
    st().
restart_st_timer(Type, Timeout, St) ->
    start_st_timer(Type, Timeout, try_cancel_st_timer(Type, St)).

-spec try_cancel_st_timer(atom(), st()) ->
    st().
try_cancel_st_timer(Type, St = #{timers := Timers}) ->
    case Timers of
        #{Type := TRef} ->
            ok = cancel_timer(TRef, St),
            St#{timers => maps:remove(Type, Timers)};
        #{} ->
            St
    end.

-spec start_st_timer(atom(), pos_integer(), st()) ->
    st().
start_st_timer(Type, Timeout, St = #{timers := Timers, opts := Opts}) ->
    St#{timers := Timers#{Type => start_timer(Type, Timeout, Opts)}}.

-spec terminate(reason(), st()) ->
    _.
terminate(Reason, St) ->
    try_invoke_callback(terminate, [Reason], ok, St).

-spec code_change(_, st(), _) ->
    {ok, st()} | {error, _Reason}.
code_change(OldVsn, St, Extra) ->
    try_invoke_callback(code_change, [OldVsn], [Extra], {ok, St}, St).

%% Core logic

-spec try_discover(st()) ->
    st().
try_discover(St0 = #{squad := Squad0, opts := Opts}) ->
    case invoke_callback(discover, [], St0) of
        {ok, Members, St} ->
            Squad = add_members(Members, Squad0, Opts),
            ok = broadcast(howdy, [newbies(Squad0)], discover, Squad, Opts),
            try_update_squad(Squad, defer_discovery(St));
        Ret ->
            Ret
    end.

-spec defer_discovery(st()) ->
    st().
defer_discovery(St = #{squad := Squad, opts := #{discovery := DOpts}}) ->
    Timeout = case maps:size(Squad) of
        S when S < 2 -> maps:get(initial_interval, DOpts);
        _            -> maps:get(refresh_interval, DOpts)
    end,
    restart_st_timer(discovery, Timeout, St).

-spec try_update_squad(squad(), st()) ->
    noreply(st()).
try_update_squad(Squad, St0 = #{opts := Opts}) ->
    St1 = St0#{squad := Squad},
    Rank = try_promote(Squad, Opts),
    case St1 of
        #{} when Rank == undefined ->
            {noreply, St1};
        #{rank := Rank} ->
            {noreply, St1};
        #{} ->
            St2 = St1#{rank => Rank},
            _ = beat({rank, {changed, Rank}}, St2),
            invoke_callback(handle_rank_change, [Rank, Squad], try_cancel_st_timer(user, St2))
    end.

-spec try_promote(squad(), opts()) ->
    rank() | undefined.
try_promote(Squad, #{promotion := #{min_squad_age := MinAge}}) ->
    SquadAge = maps:fold(fun (_, Member, Age) -> min(Age, maps:get(age, Member, 0)) end, MinAge, Squad),
    case SquadAge >= MinAge of
        true  -> rank(self(), Squad);
        false -> undefined
    end.

%%

-spec add_members([pid()], squad(), opts()) ->
    squad().
add_members(Members, Squad, Opts) ->
    lists:foldl(fun (Pid, S) -> add_member(Pid, S, Opts) end, Squad, Members).

-spec add_member(pid(), squad(), opts()) ->
    squad().
add_member(Pid, Squad, Opts) when not is_map_key(Pid, Squad) ->
    Member = watch_member(Pid, Opts),
    _ = beat({{member, Pid}, {added, Member}}, Opts),
    Squad#{Pid => Member};
add_member(_Pid, Squad, _Opts) ->
    Squad.

-spec refresh_member(pid(), squad(), opts()) ->
    squad().
refresh_member(Pid, Squad, Opts) when is_map_key(Pid, Squad) ->
    Member = account_heartbeat(rewatch_member(Pid, maps:get(Pid, Squad), Opts)),
    _ = beat({{member, Pid}, {refreshed, Member}}, Opts),
    Squad#{Pid := Member};
refresh_member(_Pid, Squad, _Opts) ->
    Squad.

-spec remove_member(pid(), member(), _Reason :: lost | {down, _}, squad(), opts()) ->
    squad().
remove_member(Pid, Member0, Reason, Squad, Opts) when is_map_key(Pid, Squad) ->
    _ = unwatch_member(Member0, Opts),
    _ = beat({{member, Pid}, {removed, Reason}}, Opts),
    maps:remove(Pid, Squad);
remove_member(_Pid, _Member, _Reason, Squad, _Opts) ->
    Squad.

-spec watch_member(pid(), opts()) ->
    member().
watch_member(Pid, Opts) when Pid /= self() ->
    defer_loss(Pid, start_monitor(Pid, #{}, Opts), Opts);
watch_member(Pid, Opts) when Pid == self() ->
    defer_heartbeat(#{}, Opts).

-spec rewatch_member(pid(), member(), opts()) ->
    member().
rewatch_member(Pid, Member0, Opts) when Pid /= self() ->
    {TRef, Member} = maps:take(loss_timer, Member0),
    ok = cancel_timer(TRef, Opts),
    defer_loss(Pid, Member, Opts);
rewatch_member(Pid, Member, Opts) when Pid == self() ->
    defer_heartbeat(Member, Opts).

-spec unwatch_member(member(), opts()) ->
    member().
unwatch_member(Member = #{loss_timer := TRef}, Opts) ->
    ok = cancel_timer(TRef, Opts),
    unwatch_member(maps:remove(loss_timer, Member), Opts);
unwatch_member(Member = #{monitor := MRef}, Opts) ->
    ok = cancel_monitor(MRef, Opts),
    unwatch_member(maps:remove(monitor, Member), Opts);
unwatch_member(Member = #{}, _Opts) ->
    Member.

-spec defer_heartbeat(member(), opts()) ->
    member().
defer_heartbeat(Member, Opts = #{heartbeat := #{broadcast_interval := Timeout}}) ->
    false = maps:is_key(heartbeat_timer, Member),
    Member#{heartbeat_timer => start_timer(heartbeat, Timeout, Opts)}.

-spec handle_heartbeat(reference(), squad(), opts()) ->
    squad().
handle_heartbeat(TRef, Squad, Opts) ->
    Pid = self(),
    {TRef, Member} = maps:take(heartbeat_timer, maps:get(Pid, Squad)),
    ok = broadcast(howdy, [], heartbeat, Squad, Opts),
    refresh_member(Pid, Squad#{Pid := Member}, Opts).

-spec defer_loss(pid(), member(), opts()) ->
    member().
defer_loss(Pid, Member, Opts = #{heartbeat := #{loss_timeout := Timeout}}) ->
    false = maps:is_key(loss_timer, Member),
    Member#{loss_timer => start_timer({lost, Pid}, Timeout, Opts)}.

-spec handle_loss_timeout(reference(), pid(), squad(), opts()) ->
    squad().
handle_loss_timeout(TRef, Pid, Squad, Opts) ->
    {TRef, Member} = maps:take(loss_timer, maps:get(Pid, Squad)),
    remove_member(Pid, Member, lost, Squad, Opts).

-spec start_monitor(pid(), member(), opts()) ->
    member().
start_monitor(Pid, Member, Opts) ->
    false = maps:is_key(monitor, Member),
    Member#{monitor => start_monitor(Pid, Opts)}.

-spec handle_member_down(pid(), reference(), _Reason, squad(), opts()) ->
    squad().
handle_member_down(Pid, MRef, Reason, Squad, Opts) ->
    {MRef, Member} = maps:take(monitor, maps:get(Pid, Squad)),
    remove_member(Pid, Member, {down, Reason}, Squad, Opts).

-spec account_heartbeat(member()) ->
    member().
account_heartbeat(Member) ->
    Member#{
        age          => maps:get(age, Member, 0) + 1,
        last_contact => erlang:system_time(millisecond)
    }.

%%

-type message() :: howdy.

-type payload() ::
    #{
        vsn     := 1,
        msg     := message(),
        from    := pid(),
        members := [pid()]
    }.

-type recepient_filter() :: fun((pid(), member()) -> boolean()).

-spec broadcast(message(), [recepient_filter()], _Ctx, squad(), opts()) ->
    ok.
broadcast(Message, RecepientFilters, Ctx, Squad, Opts) ->
    Payload = mk_payload(Message, Squad),
    Recepients = lists:foldl(fun maps:filter/2, maps:remove(self(), Squad), RecepientFilters),
    genlib_map:foreach(
        fun (Pid, _) ->
            _ = gen_server:cast(Pid, {?MODULE, Payload}),
            _ = beat({{broadcast, Payload}, {sent, Pid, Ctx}}, Opts),
            ok
        end,
        Recepients
    ).

-spec newbies(squad()) ->
    recepient_filter().
newbies(Squad) ->
    fun (Pid, _) -> not maps:is_key(Pid, Squad) end.

-spec mk_payload(message(), squad()) ->
    payload().
mk_payload(Message, Squad) ->
    #{
        vsn     => 1,
        msg     => Message,
        from    => self(),
        members => members(Squad)
    }.

%% Utilities

-spec invoke_callback(_Name :: atom(), _Args :: list(), st()) ->
    _Result.
invoke_callback(Name, Args, St = #{modstate := {Module, ModState}}) ->
    handle_callback_ret( erlang:apply(Module, Name, Args ++ [ModState]), St).

-spec try_invoke_callback(_Name :: atom(), _Args :: list(), _Default, st()) ->
    _Result.
try_invoke_callback(Name, Args, Default, St) ->
    try_invoke_callback(Name, Args, [], Default, St).

-spec try_invoke_callback(_Name :: atom(), _Args :: list(), _LastArgs :: list(), _Default, st()) ->
    _Result.
try_invoke_callback(Name, Args, LastArgs, Default, St = #{modstate := {Module, ModState}}) ->
    handle_callback_ret(
        try erlang:apply(Module, Name, Args ++ [ModState] ++ LastArgs) catch
            error:undef -> Default
        end,
        St
    ).

-spec handle_callback_ret(_Result, st()) ->
    _Result.
handle_callback_ret({ok, ModSt}, St) ->
    {ok, update_modstate(ModSt, St)};
handle_callback_ret({ok, Result, ModSt}, St) ->
    {ok, Result, update_modstate(ModSt, St)};
handle_callback_ret({reply, Reply, ModSt}, St) ->
    {reply, Reply, update_modstate(ModSt, St)};
handle_callback_ret({reply, Reply, ModSt, hibernate}, St) ->
    {reply, Reply, update_modstate(ModSt, St), hibernate};
handle_callback_ret({reply, Reply, ModSt, Timeout}, St) ->
    {reply, Reply, start_st_timer(user, Timeout, update_modstate(ModSt, St))};
handle_callback_ret({noreply, ModSt}, St) ->
    {noreply, update_modstate(ModSt, St)};
handle_callback_ret({noreply, ModSt, hibernate}, St) ->
    {noreply, update_modstate(ModSt, St), hibernate};
handle_callback_ret({noreply, ModSt, Timeout}, St) ->
    {noreply, start_st_timer(user, Timeout, update_modstate(ModSt, St))};
handle_callback_ret({stop, Reason, ModSt}, St) ->
    {stop, Reason, update_modstate(ModSt, St)};
handle_callback_ret(Ret, _St) ->
    Ret.

-spec update_modstate(_ModState, st()) ->
    st().
update_modstate(ModSt, St = #{modstate := {Module, _}}) ->
    St#{modstate := {Module, ModSt}}.

%%

-spec start_timer(_Msg, timeout(), opts()) ->
    reference().
start_timer(Msg, Timeout, Opts) ->
    TRef = erlang:start_timer(Timeout, self(), Msg),
    _ = beat({{timer, TRef}, {started, Timeout, Msg}}, Opts),
    TRef.

-spec cancel_timer(reference(), opts()) ->
    ok.
cancel_timer(TRef, Opts) ->
    _ = beat({{timer, TRef}, cancelled}, Opts),
    case erlang:cancel_timer(TRef) of
        false -> receive {timeout, TRef, _} -> ok after 0 -> ok end;
        _Time -> ok
    end.

-spec start_monitor(pid(), opts()) ->
    reference().
start_monitor(Pid, Opts) ->
    MRef = erlang:monitor(process, Pid),
    _ = beat({{monitor, MRef}, {started, Pid}}, Opts),
    MRef.

-spec cancel_monitor(reference(), opts()) ->
    ok.
cancel_monitor(MRef, Opts) ->
    true = erlang:demonitor(MRef, [flush]),
    _ = beat({{monitor, MRef}, cancelled}, Opts),
    ok.

%%

-spec beat(mg_gen_squad_pulse:beat(), st() | opts()) ->
    _.
beat(Beat, #{pulse := Handler}) ->
    mg_gen_squad_pulse:handle_beat(Handler, Beat);
beat(Beat, #{opts := Opts}) ->
    beat(Beat, Opts);
beat(_Beat, _St) ->
    ok.
