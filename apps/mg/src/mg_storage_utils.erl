-module(mg_storage_utils).

%% API
-export([try_set_timer     /3]).
-export([filter_history_ids/2]).

%%
%% API
%%
-spec try_set_timer(mg:ns(), mg:id(), mg_storage:status()) ->
    ok.
try_set_timer(Namespace, ID, {working, TimerDateTime})
    when TimerDateTime =/= undefined ->
    mg_timers:set(Namespace, ID, TimerDateTime);
try_set_timer(Namespace, ID, _) ->
    mg_timers:cancel(Namespace, ID).

%% TODO
% -spec apply_machine_update() ->
%     ok.
% apply_machine_update() ->
%     ok

-spec filter_history_ids([mg:event_id()], mg:history_range() | undefined) ->
    [mg:event_id()].
filter_history_ids(History, undefined) ->
    History;
filter_history_ids(HistoryIDs, {After, Limit, Direction}) ->
    lists:reverse(filter_history_ids_iter(apply_direction(Direction, HistoryIDs), After, Limit, [])).

%%
%% local
%%
-spec apply_direction(mg:direction(), [mg:event_id()]) ->
    [mg:event_id()].
apply_direction(forward, HistoryIDs) ->
    HistoryIDs;
apply_direction(backward, HistoryIDs) ->
    lists:reverse(HistoryIDs).

-spec filter_history_ids_iter([mg:event_id()], mg:event_id() | undefined, non_neg_integer(), [mg:event_id()]) ->
    [mg:event_id()].
filter_history_ids_iter([], _, _, Result) ->
    Result;
filter_history_ids_iter(_, _, 0, Result) ->
    Result;
filter_history_ids_iter([EventID|HistoryIDsTail], undefined, Limit, Result) ->
    filter_history_ids_iter(HistoryIDsTail, undefined, decrease_limit(Limit), [EventID|Result]);
filter_history_ids_iter([EventID|HistoryIDsTail], After, Limit, []) when EventID =:= After ->
    filter_history_ids_iter(HistoryIDsTail, undefined, Limit, []);
filter_history_ids_iter([_|HistoryIDsTail], After, Limit, []) ->
    filter_history_ids_iter(HistoryIDsTail, After, Limit, []).

-spec decrease_limit(undefined | pos_integer()) ->
    non_neg_integer().
decrease_limit(undefined) ->
    undefined;
decrease_limit(N) ->
    N - 1.
