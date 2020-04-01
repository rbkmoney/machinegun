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

-module(mg_dirange).

-export_type([dirange/1]).

-export([align/2]).
-export([reverse/1]).
-export([dissect/2]).
-export([intersect/2]).
-export([limit/2]).
-export([fold/3]).
-export([enumerate/1]).
-export([direction/1]).

%% Directed range over integers
-type dirange(_T) ::
      % non-empty range [from..to]
      {_T :: integer(), _T :: integer()}
      % empty range
    | undefined
    .

%%

-spec align(dirange(T), _Pivot :: dirange(T)) ->
    dirange(T).
align(R, Rp) ->
    case direction(R) * direction(Rp) of
        -1 -> reverse(R);
        _S -> R
    end.

-spec reverse(dirange(T)) ->
    dirange(T).
reverse({A, B}) ->
    {B, A};
reverse(undefined) ->
    undefined.

-spec dissect(dirange(T), T) ->
    {dirange(T), dirange(T)}.
dissect({A, B} = R, C) when A =< B ->
    if
        C < A         -> {undefined, R};
        B < C         -> {R, undefined};
        A =< C, C < B -> {{A, C}, {C + 1, B}};
        A =< C        -> {{A, C}, undefined}
    end;
dissect({B, A} = R, C) when B > A ->
    {R1, R2} = dissect(reverse(R), C - 1),
    {reverse(R2), reverse(R1)};
dissect(undefined, _) ->
    {undefined, undefined}.

-spec intersect(_Range :: dirange(T), _With :: dirange(T)) ->
    {
        _LeftDiff :: dirange(T),     % part of `Range` to the «left» of `With`
        _Intersection :: dirange(T), % intersection between `Range` and `With`
        _RightDiff :: dirange(T)     % part of `Range` to the «right» of `With`
    }.
intersect(R0, With0) ->
    With = {WA, WB} = align(With0, R0),
    {LeftDiff, R1} = dissect(R0, WA - direction(With)), % to NOT include WA itself
    {Intersection, RightDiff} = dissect(R1, WB),
    {LeftDiff, Intersection, RightDiff}.

-spec limit(dirange(T), undefined | non_neg_integer()) ->
    dirange(T).
limit(R, undefined) ->
    R;
limit({A, B}, N) when A =< B, N > 0 ->
    {A, erlang:min(B, A + N - 1)};
limit({B, A}, N) when B > A, N > 0 ->
    {B, erlang:max(A, B - N + 1)};
limit(_, _) ->
    undefined.

-spec enumerate(dirange(T)) ->
    [T].
enumerate({A, B} = R) ->
    lists:seq(A, B, direction(R));
enumerate(undefined) ->
    [].

-spec fold(fun((T, Acc) -> Acc), Acc, dirange(T)) ->
    Acc.
fold(F, Acc, {A, B} = R) ->
    fold(F, Acc, A, B, direction(R));
fold(_, Acc, undefined) ->
    Acc.

-spec fold(fun((T, Acc) -> Acc), Acc, T, T, -1..1) ->
    Acc.
fold(F, Acc, A, A, _) ->
    F(A, Acc);
fold(F, Acc, A, B, S) ->
    fold(F, F(A, Acc), A + S, B, S).

-spec direction(dirange(_)) ->
    -1..1.
direction({A, B}) when A =< B ->
    1;
direction({B, A}) when B > A ->
    -1;
direction(undefined) ->
    0.
