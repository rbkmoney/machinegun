%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_quota_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_test/2]).
-export([end_per_test/2]).

%% tests

-export([no_over_allocation_test/1]).
-export([fair_sharing_without_usage_test/1]).
-export([sharing_respects_usage_test/1]).
-export([fair_sharing_with_full_usage_test/1]).
-export([fair_share_with_large_limit/1]).
-export([unwanted_resources_redistribution_test/1]).
-export([guaranteed_resources_redistribution_test/1]).
-export([large_amount_of_clients_not_freeze_test/1]).
-export([large_amount_of_clients_with_zero_share_not_freeze_test/1]).
-export([sharing_respects_shares/1]).
-export([sharing_respects_zero_shares/1]).
-export([share_can_be_changed/1]).

%% Internal types

-type group() :: {Name :: atom(), Opts :: list(), [test_name()]}.
-type config() :: [{atom(), any()}].
-type test_name() :: atom().
-type group_name() :: atom().

-record(client, {
    options :: mg_quota:client_options(),
    usage = 0 :: resource(),
    expectation = 0 :: resource(),
    reserved = 0 :: resource()
}).
-type client() :: #client{}.
-type quota() :: mg_quota:state().
-type resource() :: mg_quota:resource().

%%
%% tests descriptions
%%

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        {group, all}
    ].

-spec groups() ->
    [group()].
groups() ->
    [
        {all, [parallel], [
            no_over_allocation_test,
            fair_sharing_without_usage_test,
            sharing_respects_usage_test,
            fair_sharing_with_full_usage_test,
            fair_share_with_large_limit,
            unwanted_resources_redistribution_test,
            guaranteed_resources_redistribution_test,
            large_amount_of_clients_not_freeze_test,
            large_amount_of_clients_with_zero_share_not_freeze_test,
            sharing_respects_shares,
            sharing_respects_zero_shares,
            share_can_be_changed
       ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = genlib_app:start_application(stdlib),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(_Name, C) ->
    C.

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_Name, _C) ->
    ok.

-spec init_per_test(test_name(), config()) ->
    config().
init_per_test(_Name, C) ->
    C.

-spec end_per_test(test_name(), config()) ->
    ok.
end_per_test(_Name, _C) ->
    ok.

%%
%% base group tests
%%

-spec no_over_allocation_test(config()) -> any().
no_over_allocation_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10),
    Clients1 = [C#client{expectation = 20} || C <- Clients0],
    {Clients2, _Q1} = reserve(Clients1, Q0),
    ok = validate_quota_contract(Clients2, Limit).

-spec fair_sharing_without_usage_test(config()) -> any().
fair_sharing_without_usage_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10),
    % Reserved more resources when availiable
    Clients1 = [C#client{expectation = 20} || C <- Clients0],
    {Clients2, Q1} = reserve(Clients1, Q0),
    ok = validate_quota_contract(Clients2, Limit),
    % Don't use reserved resurces and recalculate targets
    {ok, Q2} = mg_quota:recalculate_targets(Q1),
    {Clients3, _Q3} = reserve(Clients2, Q2),
    ok = validate_quota_contract(Clients3, Limit),
    Expected = repeat(10, 10),
    ?assertEqual(Expected, get_reserve(Clients3)).

-spec sharing_respects_usage_test(config()) -> any().
sharing_respects_usage_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10),
    % Reserved more resources when availiable
    Clients1 = [C#client{expectation = 20} || C <- Clients0],
    {Clients2, Q1} = reserve(Clients1, Q0),
    ok = validate_quota_contract(Clients2, Limit),
    ?assertEqual(repeat(20, 5) ++ repeat(0, 5), get_reserve(Clients2)),
    % Use reserved resources completelly
    Clients3 = [C#client{usage = C#client.reserved} || C <- Clients2],
    {Clients4, Q2} = reserve(Clients3, Q1),
    ok = validate_quota_contract(Clients4, Limit),
    % Recalculate targets
    {ok, Q3} = mg_quota:recalculate_targets(Q2),
    {Clients5, _Q4} = reserve(Clients4, Q3),
    ok = validate_quota_contract(Clients5, Limit),
    Expected = repeat(10, 5) ++ repeat(0, 5),
    ?assertEqual(Expected, get_reserve(Clients5)).

-spec fair_sharing_with_full_usage_test(config()) -> any().
fair_sharing_with_full_usage_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2),
    Clients1 = [C#client{expectation = 100} || C <- Clients0],
    {Clients2, Q1} = reserve(Clients1, Q0),
    ok = validate_quota_contract(Clients2, Limit),
    ?assertEqual([100, 0], get_reserve(Clients2)),
    % Use reserved resources completelly
    Clients3 = [C#client{usage = C#client.reserved} || C <- Clients2],
    {Clients4, Q2} = reserve(Clients3, Q1),
    ok = validate_quota_contract(Clients4, Limit),
    % Recalculate targets
    {ok, Q3} = mg_quota:recalculate_targets(Q2),
    {Clients5, Q4} = reserve(Clients4, Q3),
    ok = validate_quota_contract(Clients5, Limit),
    ?assertEqual([50, 0], get_reserve(Clients5)),
    % Set usage as recommended
    Clients6 = [C#client{usage = C#client.reserved} || C <- Clients5],
    {Clients7, _Q5} = reserve(Clients6, Q4),
    ok = validate_quota_contract(Clients7, Limit),
    ?assertEqual([50, 50], get_reserve(Clients7)).

-spec fair_share_with_large_limit(config()) -> any().
fair_share_with_large_limit(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10),
    {Clients1, _Q1} = loop(set_expectation(Clients0, repeat(1, 10)), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual(get_expectation(Clients1), get_reserve(Clients1)).

-spec unwanted_resources_redistribution_test(config()) -> any().
unwanted_resources_redistribution_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2),
    {Clients1, Q1} = loop(set_expectation(Clients0, [25, 100]), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual([25, 75], get_reserve(Clients1)),

    {Clients2, _Q2} = loop(set_expectation(Clients1, [10, 100]), Q1),
    ok = validate_quota_contract(Clients2, Limit),
    ?assertEqual([10, 90], get_reserve(Clients2)).

-spec guaranteed_resources_redistribution_test(config()) -> any().
guaranteed_resources_redistribution_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2),
    {Clients1, Q1} = loop(set_expectation(Clients0, [25, 100]), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual([25, 75], get_reserve(Clients1)),

    {Clients2, Q2} = loop(set_expectation(Clients1, [40, 100]), Q1),
    ok = validate_quota_contract(Clients2, Limit),
    ?assertEqual([40, 60], get_reserve(Clients2)),

    {Clients3, _Q3} = loop(set_expectation(Clients2, [60, 100]), Q2),
    ok = validate_quota_contract(Clients3, Limit),
    ?assertEqual([50, 50], get_reserve(Clients3)).

-spec large_amount_of_clients_not_freeze_test(config()) -> any().
large_amount_of_clients_not_freeze_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10000),
    {Clients1, _Q1} = loop(set_expectation(Clients0, repeat(1, 10000)), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assert(lists:sum(get_reserve(Clients1)) > 0).

-spec large_amount_of_clients_with_zero_share_not_freeze_test(config()) -> any().
large_amount_of_clients_with_zero_share_not_freeze_test(_C) ->
    Limit = 100,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(10000, repeat(0, 10000)),
    {Clients1, _Q1} = loop(set_expectation(Clients0, repeat(1, 10000)), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assert(lists:sum(get_reserve(Clients1)) > 0).

-spec sharing_respects_shares(config()) -> any().
sharing_respects_shares(_C) ->
    Limit = 6,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2, [1, 2]),
    {Clients1, _Q1} = loop(set_expectation(Clients0, [10, 10]), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual([2, 4], get_reserve(Clients1)).

-spec sharing_respects_zero_shares(config()) -> any().
sharing_respects_zero_shares(_C) ->
    Limit = 6,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2, [0, 2]),
    {Clients1, _Q1} = loop(set_expectation(Clients0, [10, 5]), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual([1, 5], get_reserve(Clients1)).

-spec share_can_be_changed(config()) -> any().
share_can_be_changed(_C) ->
    Limit = 6,
    Q0 = mg_quota:new(#{
        limit => #{value => Limit}
    }),
    Clients0 = create_clients(2, [1, 1]),
    {Clients1, Q1} = loop(set_expectation(Clients0, [10, 10]), Q0),
    ok = validate_quota_contract(Clients1, Limit),
    ?assertEqual([3, 3], get_reserve(Clients1)),

    {Clients2, _Q2} = loop(set_share(Clients1, [1, 2]), Q1),
    ok = validate_quota_contract(Clients2, Limit),
    ?assertEqual([2, 4], get_reserve(Clients2)).

%% Internals

-spec repeat(term(), non_neg_integer()) ->
    [term()].
repeat(Element, Count) ->
    [Element || _ <- lists:seq(1, Count)].

-spec create_clients(non_neg_integer()) ->
    [client()].
create_clients(Number) ->
    create_clients(Number, repeat(1, Number)).

-spec create_clients(non_neg_integer(), [mg_quota:share()]) ->
    [client()].
create_clients(Number, Shares) ->
    [
        #client{
            options = #{
                client_id => N,
                share => S
            }
        }
        || {N, S} <- lists:zip(lists:seq(1, Number), Shares)
    ].

-spec reserve([client()], quota()) ->
    {[client()], quota()}.
reserve(Clients, Quota) ->
    {NewClients, NewQuota} = lists:foldl(fun do_reserve/2, {[], Quota}, Clients),
    {lists:reverse(NewClients), NewQuota}.

-spec do_reserve(client(), Acc) -> Acc when
    Acc :: {[client()], quota()}.
do_reserve(Client, {Acc, Quota}) ->
    #client{options = Options, usage = Usage, expectation = Exp} = Client,
    {ok, Reserved, NewQuota} = mg_quota:reserve(Options, Usage, Exp, Quota),
    {[Client#client{reserved = Reserved} | Acc], NewQuota}.

-spec loop([client()], quota()) ->
    {[client()], quota()}.
loop(Clients, Quota) ->
    loop(Clients, Quota, 5).

-spec loop([client()], quota(), non_neg_integer()) ->
    {[client()], quota()}.
loop(Clients, Quota, 0) ->
    {Clients, Quota};
loop(Clients0, Quota0, N) when N > 0 ->
    {Clients1, Quota1} = reserve(Clients0, Quota0),
    {ok, Quota2} = mg_quota:recalculate_targets(Quota1),
    loop(Clients1, Quota2, N - 1).

-spec get_reserve([client()]) ->
    [resource()].
get_reserve(Clients) ->
    [C#client.reserved || C <- Clients].

-spec get_expectation([client()]) ->
    [resource()].
get_expectation(Clients) ->
    [C#client.expectation || C <- Clients].

-spec set_expectation([client()], [resource()]) ->
    [client()].
set_expectation(Clients, Expecations) ->
    [C#client{expectation = E} || {C, E} <- lists:zip(Clients, Expecations)].

-spec set_share([client()], [mg_quota:share()]) ->
    [client()].
set_share(Clients, Shares) ->
    [
        C#client{options = O#{share => S}}
        || {#client{options = O} = C, S} <- lists:zip(Clients, Shares)
    ].

-spec validate_quota_contract([client()], Limit :: mg_quota:resource()) ->
    ok.
validate_quota_contract(Clients, Limit) ->
    true = lists:sum(get_reserve(Clients)) =< Limit,
    TotalUsage = [C#client.usage || C <- Clients],
    true = lists:sum(TotalUsage) =< Limit,
    MaxPossibleUsage = [erlang:max(R, U) || #client{reserved = R, usage = U} <- Clients],
    true = lists:sum(MaxPossibleUsage) =< Limit,
    ok.
