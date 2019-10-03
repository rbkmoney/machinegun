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

-module(mg_gen_squad_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("ct_helper.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([squad_view_consistency_holds/1]).
-export([squad_shrinks_consistently/1]).

%% squad behaviour
-behaviour(mg_gen_squad).
-export([init/1]).
-export([discover/1]).
-export([handle_rank_change/3]).
-export([handle_call/5]).
-export([handle_cast/4]).
-export([handle_info/4]).

-behaviour(mg_gen_squad_pulse).
-export([handle_beat/2]).

%% tests descriptions

-type test_name() :: atom().
-type config()    :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        squad_view_consistency_holds,
        squad_shrinks_consistently
    ].

%% starting/stopping

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([mg]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

%%

-spec squad_view_consistency_holds(config()) ->
    _.
squad_view_consistency_holds(_) ->
    N = 5,
    Opts = #{
        pulse => {?MODULE, erlang:system_time(millisecond)},
        heartbeat => #{broadcast_interval => 200},
        discovery => #{initial_interval => 200, refresh_interval => 1000},
        promotion => #{min_squad_age => 3}
    },
    Members = [Leader | Followers] = lists:map(fun (_) -> start_member(Opts) end, lists:seq(1, N)),
    ok = lists:foreach(fun ({M0, M1}) -> gen_server:cast(M0, {known, [M1]}) end, neighbours(Members)),
    _ = [?assertReceive({Follower, follower, _}) || Follower <- Followers],
    SquadViews = lists:sort([gen_server:call(M, report) || M <- Members]),
    [LeaderView | FollowerViews] = SquadViews,
    _ = ?assertMatch({Leader, leader, _}, LeaderView),
    _ = ?assertReceive(LeaderView),
    _ = [?assertMatch({_, follower, _}, V) || V <- FollowerViews],
    MembersViews = [ordsets:from_list([Pid | Ms]) || {Pid, _, Ms} <- SquadViews],
    _ = [?assertEqual(N, ordsets:size(V)) || V <- MembersViews],
    _ = [?assertEqual(V0, V1) || {V0, V1} <- neighbours(MembersViews)],
    _ = ?assertNoReceive(),
    ok.

-spec squad_shrinks_consistently(config()) ->
    _.
squad_shrinks_consistently(_) ->
    N = 5,
    Opts = #{
        pulse => {?MODULE, erlang:system_time(millisecond)},
        heartbeat => #{broadcast_interval => 200},
        discovery => #{initial_interval => 200, refresh_interval => 1000},
        promotion => #{min_squad_age => 3}
    },
    Members = [Leader | Rest] = lists:map(fun (_) -> start_member(Opts) end, lists:seq(1, N)),
    ok = lists:foreach(fun ({M0, M1}) -> gen_server:cast(M0, {known, [M1]}) end, neighbours(Members)),
    _ = ?assertReceive({Leader, leader, Rest}),
    _ = [?assertReceive({Follower, follower, _}) || Follower <- Rest],
    _ = [?assertReceive({Follower, leader, SquadRest}) || [Follower | SquadRest] <- [Rest | tails(Rest)]],
    _ = ?assertEqual([lists:last(Members)], lists:filter(fun erlang:is_process_alive/1, Members)),
    ok.

-spec start_member(mg_gen_squad:opts()) ->
    pid().
start_member(Opts) ->
    {ok, Pid} = mg_gen_squad:start_link(?MODULE, #{runner => self(), known => []}, Opts),
    Pid.

-spec tails(list(A)) ->
    list(list(A)).
tails([_ | T]) -> [T | tails(T)];
tails([])      -> [].

-spec neighbours([T]) ->
    [{T, T}].
neighbours([A | [B | _] = Rest]) -> [{A, B} | neighbours(Rest)];
neighbours([_])                  -> [];
neighbours([])                   -> [].

%%

-type rank() :: mg_gen_squad:rank().
-type squad() :: mg_gen_squad:squad().

-type st() :: #{
    runner := pid(),
    known  := [pid()]
}.

-spec init(st()) ->
    {ok, st()}.
init(St) ->
    {ok, St}.

-spec discover(st()) ->
    {ok, [pid()], st()}.
discover(St = #{known := Known}) ->
    {ok, Known, St}.

-spec handle_rank_change(rank(), squad(), st()) ->
    {noreply, st()}.
handle_rank_change(Rank, Squad, St = #{runner := Runner}) ->
    _ = Runner ! {self(), Rank, mg_gen_squad:members(Squad)},
    case Rank of
        leader   -> {noreply, St, 200};
        follower -> {noreply, St}
    end;
handle_rank_change(_Rank, _Squad, St) ->
    {noreply, St}.

-type call() :: report.

-spec handle_call(call(), _From, rank(), squad(), st()) ->
    {noreply, st()} | {reply, _, st()}.
handle_call(report, _From, Rank, Squad, St) ->
    {reply, {self(), Rank, mg_gen_squad:members(Squad)}, St};
handle_call(Call, From, _Rank, _Squad, _St) ->
    erlang:error({unexpected, {call, Call, From}}).

-type cast() :: {known, [pid()]}.

-spec handle_cast(cast(), rank(), squad(), st()) ->
    {noreply, st()}.
handle_cast({known, More}, _Rank, _Squad, St = #{known := Known}) ->
    {noreply, St#{known := Known ++ More}};
handle_cast(Cast, _Rank, _Squad, _St) ->
    erlang:error({unexpected, {cast, Cast}}).

-type info() :: timeout.

-spec handle_info(info(), rank(), squad(), st()) ->
    {noreply, st()}.
handle_info(timeout, leader, _Squad, St) ->
    {stop, normal, St};
handle_info(Info, _Rank, _Squad, _St) ->
    erlang:error({unexpected, {info, Info}}).

-spec handle_beat(_, mg_gen_squad_pulse:beat()) ->
    _.
handle_beat(_Start, {{timer, _}, _}) -> ok;
handle_beat(_Start, {{monitor, _}, _}) -> ok;
handle_beat(Start, Beat) ->
    io:format("+~6..0Bms ~0p ~0p", [erlang:system_time(millisecond) - Start, self(), Beat]).
