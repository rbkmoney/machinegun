-module(mg_events_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).

%% tests
-export([range_direction_test/1]).
-export([range_border_test/1]).
-export([range_missing_params_test/1]).
-export([range_no_intersection_test/1]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        range_direction_test,
        % на данный момент тест не работает
        % range_no_intersection_test,
        range_border_test,
        range_missing_params_test
    ].

-spec range_direction_test(config()) ->
    _.
range_direction_test(_C) ->
    EventsRange = {1, 100},
    [4, 3, 2   ] = mg_events:get_event_ids(EventsRange, {5, 3, backward}),
    [5, 6, 7, 8] = mg_events:get_event_ids(EventsRange, {4, 4, forward }).

-spec range_no_intersection_test(config()) ->
    _.
range_no_intersection_test(_C) ->
    EventsRange = {5, 10},
    event_not_found = (catch mg_events:get_event_ids(EventsRange, {1, 3, forward})).

-spec range_border_test(config()) ->
    _.
range_border_test(_C) ->
    EventsRange = {1, 8},
    [1, 2   ] = mg_events:get_event_ids(EventsRange, {undefined, 2, forward }),
    [8, 7   ] = mg_events:get_event_ids(EventsRange, {undefined, 2, backward}),
    [6, 7, 8] = mg_events:get_event_ids(EventsRange, {5        , 5, forward }).

-spec range_missing_params_test(config()) ->
    ok.
range_missing_params_test(_C) ->
    EventsRange = {1, 8},
    [1, 2, 3] = mg_events:get_event_ids(EventsRange, {undefined, 3, forward}),
    [7, 8   ] = mg_events:get_event_ids(EventsRange, {6, undefined, forward}).
