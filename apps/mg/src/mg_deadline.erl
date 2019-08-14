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

-module(mg_deadline).

%% API
-export_type([deadline/0]).
-export([from_timeout/1]).
-export([to_timeout/1]).
-export([from_unixtime_ms/1]).
-export([to_unixtime_ms/1]).
-export([is_reached/1]).
-export([default/0]).
-export([format/1]).

%%
-type deadline() :: undefined | pos_integer(). % milliseconds since unix epoch

-spec from_timeout(timeout()) ->
    deadline().
from_timeout(infinity) ->
    undefined;
from_timeout(Timeout) when is_integer(Timeout) ->
    now_ms() + Timeout;
from_timeout(Timeout) ->
    erlang:error(badarg, [Timeout]).

-spec to_timeout(deadline()) ->
    timeout().
to_timeout(undefined) ->
    infinity;
to_timeout(Deadline) when is_integer(Deadline) ->
    erlang:max(Deadline - now_ms(), 0);
to_timeout(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec from_unixtime_ms(non_neg_integer()) ->
    deadline().
from_unixtime_ms(Deadline) when is_integer(Deadline) ->
    Deadline;
from_unixtime_ms(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec to_unixtime_ms(deadline()) ->
    non_neg_integer().
to_unixtime_ms(Deadline) when is_integer(Deadline) ->
    Deadline;
to_unixtime_ms(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec is_reached(deadline()) ->
    boolean().
is_reached(undefined) ->
    false;
is_reached(Deadline) ->
    Deadline - now_ms() =< 0.

-spec default() ->
    deadline().
default() ->
    %% For testing purposes only
    from_timeout(30000).

-spec format(mg_deadline:deadline()) ->
    binary().
format(Deadline) ->
    {ok, Bin} = rfc3339:format(Deadline, millisecond),
    Bin.

%%

-spec now_ms() ->
    pos_integer().
now_ms() ->
    erlang:system_time(millisecond).
