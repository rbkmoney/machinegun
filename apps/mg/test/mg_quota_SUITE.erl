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

%% Internal types

-type group() :: {Name :: atom(), Opts :: list(), [test_name()]}.
-type config() :: [{atom(), any()}].
-type test_name() :: atom().
-type group_name() :: atom().

-record(client, {
    options :: mg_quota:client_options(),
    usage = 0 :: resource(),
    expectation = 0 :: resource(),
    reserve = 0 :: resource()
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
            no_over_allocation_test
       ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [ok = application:stop(App) || App <- proplists:get_value(apps, C)],
    ok.

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
    ok = validate_clients(Clients2, Limit).

%% Clients management

-spec create_clients(non_neg_integer()) ->
    [client()].
create_clients(Number) ->
    [
        #client{
            options = #{
                client_id => N,
                share => 1
            }
        }
        || N <- lists:seq(0, Number)
    ].

-spec reserve([client()], quota()) ->
    {[client()], quota()}.
reserve(Clients, Quota) ->
    lists:foldl(fun do_reserve/2, {[], Quota}, Clients).

-spec do_reserve(client(), Acc) -> Acc when
    Acc :: {[client()], quota()}.
do_reserve(Client, {Acc, Quota}) ->
    #client{options = Options, usage = Usage, expectation = Exp} = Client,
    {ok, Reserve, NewQuota} = mg_quota:reserve(Options, Usage, Exp, Quota),
    {[Client#client{reserve = Reserve} | Acc], NewQuota}.

-spec validate_clients([client()], Limit :: resource()) ->
    ok | no_return().
validate_clients(Clients, Limit) ->
    TotalReserve = lists:sum([C#client.reserve || C <- Clients]),
    ?assert(TotalReserve =< Limit).
