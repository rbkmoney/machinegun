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

%%%
%%% Retry helpers.
%%% TODO: Move to genlib_retry
%%%
-module(mg_retry).

-export_type([policy/0]).
-export_type([strategy/0]).
-export([new_strategy/1]).
-export([new_strategy/3]).

-type retries_num() :: pos_integer() | infinity.
-type policy() ::
      {linear, retries_num() | {max_total_timeout, pos_integer()}, pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer(), timeout()}
    | {intervals, [pos_integer(), ...]}
    | {timecap, timeout(), policy()}
.

-type strategy() :: genlib_retry:strategy().

%% API

-spec new_strategy(policy()) ->
    strategy().
new_strategy({linear, Retries, Timeout}) ->
    genlib_retry:linear(Retries, Timeout);
new_strategy({exponential, Retries, Factor, Timeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout);
new_strategy({exponential, Retries, Factor, Timeout, MaxTimeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout, MaxTimeout);
new_strategy({intervals, Array}) ->
    genlib_retry:intervals(Array);
new_strategy({timecap, Timeout, Policy}) ->
    genlib_retry:timecap(Timeout, new_strategy(Policy));
new_strategy(BadPolicy) ->
    erlang:error(badarg, [BadPolicy]).

-spec new_strategy
    (policy(), genlib_time:ts(), non_neg_integer()) -> strategy();
    (policy(), undefined, undefined) -> strategy().
new_strategy(PolicySpec, undefined, undefined) ->
    new_strategy(PolicySpec);
new_strategy(PolicySpec, _InitialTimestamp, Attempt) ->
    %% TODO: Use InitialTimestamp
    Strategy = new_strategy(PolicySpec),
    skip_steps(Strategy, Attempt).

%% Internals

-spec skip_steps(strategy(), non_neg_integer()) -> strategy().
skip_steps(Strategy, 0) ->
    Strategy;
skip_steps(Strategy, N) when N > 0 ->
    NewStrategy = case genlib_retry:next_step(Strategy) of
        {wait, _Timeout, NextStrategy} ->
            NextStrategy;
        finish = NextStrategy ->
            NextStrategy
    end,
    skip_steps(NewStrategy, N - 1).
