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

-module(mg_dirange_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([direction_test/1]).
-export([size_test/1]).
-export([limit_test/1]).
-export([dissect_test/1]).
-export([intersect_test/1]).
-export([enumerate_test/1]).
-export([fold_test/1]).
-export([storage_test/1]).

%% tests

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        direction_test,
        size_test,
        limit_test,
        dissect_test,
        intersect_test,
        enumerate_test,
        fold_test,
        storage_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    _ = logger:set_handler_config(default, formatter, {logger_formatter, #{}}),
    C.

-spec end_per_suite(config()) ->
    _.
end_per_suite(_) ->
    ok.

-spec direction_test(config()) ->
    _.
direction_test(_) ->
    ?assertEqual(+1, mg_dirange:direction(fw(1, 10))),
    ?assertEqual(-1, mg_dirange:direction(bw(10, 1))),
    ?assertEqual(-1, mg_dirange:direction(bw(42, 42))),
    ?assertEqual(0, mg_dirange:direction(undefined)),
    ?assert(check_property(
        % Reversal changes direction
        ?FORALL(R, range(),
            equals(-mg_dirange:direction(R), mg_dirange:direction(mg_dirange:reverse(R)))
        )
    )).

-spec size_test(config()) ->
    _.
size_test(_) ->
    ?assertEqual(10, mg_dirange:size(fw(1, 10))),
    ?assertEqual(10, mg_dirange:size(bw(10, 1))),
    ?assertEqual(10, mg_dirange:size(fw(-10, -1))),
    ?assert(check_property(
        % Size is non-negative for every range
        ?FORALL(R, range(), mg_dirange:size(R) >= 0)
    )),
    ?assert(check_property(
        % Reversal preserves size
        ?FORALL(R, range(), equals(mg_dirange:size(R), mg_dirange:size(mg_dirange:reverse(R))))
    )).

-spec limit_test(config()) ->
    _.
limit_test(_) ->
    ?assert(check_property(
        % Size of limited range always under limit
        ?FORALL({R, Limit}, {range(), non_neg_integer()},
            mg_dirange:size(mg_dirange:limit(R, Limit)) =< Limit
        )
    )).

-spec dissect_test(config()) ->
    _.
dissect_test(_) ->
    % TODO
    % Technically this matching is opaque type violation. To be a good guy with
    % dialyzer we probably should match on some exported representation. Still,
    % fine for now I guess.
    ?assertEqual({undefined, undefined}, mg_dirange:dissect(undefined, 42)),
    ?assertEqual({fw(1, 10), undefined}, mg_dirange:dissect(fw(1, 10), 42)),
    ?assertEqual({undefined, bw(10, 1)}, mg_dirange:dissect(bw(10, 1), 42)),
    ?assertEqual({bw(10, 1), undefined}, mg_dirange:dissect(bw(10, 1), 1)),
    ?assertEqual({bw(10, 2), bw(1, 1)}, mg_dirange:dissect(bw(10, 1), 2)),
    ?assert(check_property(
        % Dissection does not change direction
        ?FORALL({R, At}, {range(), integer()}, begin
            {R1, R2} = mg_dirange:dissect(R, At),
            mg_dirange:direction(R1) * mg_dirange:direction(R2) =/= -1
        end)
    )),
    ?assert(check_property(
        % Dissection preserves range size
        ?FORALL({R, At}, {range(), integer()}, begin
            {R1, R2} = mg_dirange:dissect(R, At),
            equals(mg_dirange:size(R), mg_dirange:size(R1) + mg_dirange:size(R2))
        end)
    )),
    ?assert(check_property(
        % Dissection is complemented by conjoining
        ?FORALL({R, At}, {range(), integer()}, begin
            {R1, R2} = mg_dirange:dissect(R, At),
            equals(R, mg_dirange:conjoin(R1, R2))
        end)
    )).

-spec intersect_test(config()) ->
    _.
intersect_test(_) ->
    ?assertEqual({undefined, undefined, undefined}, mg_dirange:intersect(undefined, fw(1, 10))),
    ?assertEqual({bw(10, 7), bw(6, 5), bw(4, 1)}, mg_dirange:intersect(bw(10, 1), fw(5, 6))),
    ?assert(check_property(
        % Range intersects with itself with no left/right differences
        ?FORALL(R, nonempty_range(),
            equals({undefined, R, undefined}, mg_dirange:intersect(R, R))
        )
    )),
    ?assert(check_property(
        % Range intersects with reversal of itself with no left/right differences
        ?FORALL(R, nonempty_range(),
            equals({undefined, R, undefined}, mg_dirange:intersect(R, mg_dirange:reverse(R)))
        )
    )),
    ?assert(check_property(
        % Left/right differences end up being only nonempty ranges when intersected with original range
        ?FORALL({R, With}, {range(), nonempty_range()}, begin
            {LD, _, RD} = mg_dirange:intersect(R, With),
            conjunction([
                {strictly_left_diff,
                    equals({LD, undefined, undefined}, mg_dirange:intersect(LD, With))
                },
                {strictly_right_diff,
                    equals({undefined, undefined, RD}, mg_dirange:intersect(RD, With))
                }
            ])
        end)
    )),
    ?assert(check_property(
        % Intersection preserve range size
        ?FORALL({R0, RWith}, {range(), nonempty_range()}, begin
            {RL, RI, RR} = mg_dirange:intersect(R0, RWith),
            equals(mg_dirange:size(R0), lists:sum([mg_dirange:size(R) || R <- [RL, RI, RR]]))
        end)
    )),
    ?assert(check_property(
        % Intersection complemented by conjoing
        ?FORALL({R0, RWith}, {range(), nonempty_range()}, begin
            {RL, RI, RR} = mg_dirange:intersect(R0, RWith),
            equals(R0, mg_dirange:conjoin(mg_dirange:conjoin(RL, RI), RR))
        end)
    )).

-spec enumerate_test(config()) ->
    _.
enumerate_test(_) ->
    ?assertEqual([1, 2, 3, 4, 5], mg_dirange:enumerate(fw(1, 5))),
    ?assertEqual([5, 4, 3, 2, 1], mg_dirange:enumerate(bw(5, 1))),
    ?assert(check_property(
        % Enumeration preserves range size
        ?FORALL(R, range(),
            equals(length(mg_dirange:enumerate(R)), mg_dirange:size(R))
        )
    )),
    ?assert(check_property(
        % Enumeration preserves reversal
        ?FORALL(R, range(),
            equals(lists:reverse(mg_dirange:enumerate(R)), mg_dirange:enumerate(mg_dirange:reverse(R)))
        )
    )),
    ?assert(check_property(
        % Enumeration preserves range bounds
        ?FORALL(R, nonempty_range(), begin
            {A, B} = mg_dirange:bounds(R),
            L = [H | _] = mg_dirange:enumerate(R),
            conjunction([
                {nonempty, length(L) > 0},
                {starts_with_bound, equals(H, A)},
                {ends_with_bound, equals(lists:last(L), B)}
            ])
        end)
    )).

-spec fold_test(config()) ->
    _.
fold_test(_) ->
    ?assert(check_property(
        % Folding with right accumulation is indistinguishable from enumeration
        ?FORALL(R, range(),
            equals(mg_dirange:enumerate(R), mg_dirange:fold(fun (E, L) -> L ++ [E] end, [], R))
        )
    )).

-spec storage_test(config()) ->
    _.
storage_test(_) ->
    ?assert(check_property(
        % Restoring stored representation preserves original range
        ?FORALL(R, range(),
            equals(R, mg_dirange:from_opaque(mg_dirange:to_opaque(R)))
        )
    )).

-spec range() ->
    proper_types:raw_type().
range() ->
    frequency([
        {9, nonempty_range()},
        {1, undefined}
    ]).

-spec nonempty_range() ->
    proper_types:raw_type().
nonempty_range() ->
    ?LET({A, B}, {integer(), integer()}, oneof([fw(A, B), bw(A, B)])).

-spec check_property(proper:test()) ->
    boolean().
check_property(Property) ->
    proper:quickcheck(Property, [{numtests, 1000}, nocolors]).

%%

-spec fw(T, T) ->
    mg_dirange:nonempty_dirange(T).
fw(A, B) ->
    mg_dirange:forward(A, B).

-spec bw(T, T) ->
    mg_dirange:nonempty_dirange(T).
bw(A, B) ->
    mg_dirange:backward(A, B).
