-module(mg_storage_utils).

%% API
-export([get_machine_events_ids/3]).

-export_type([pool_options/0]).
-export([pool_child_spec/1]).
-export([pool_do        /2]).

%%
%% API
%%
%% в pooler'е нет типов :(
-type pooler_time_interval() :: {non_neg_integer(), min | sec | ms}.

-type pool_options() :: #{
    name                 => term(),
    start_mfa            => {atom(), atom(), list()},
    max_count            => non_neg_integer     (),
    init_count           => non_neg_integer     (),
    cull_interval        => pooler_time_interval(),
    max_age              => pooler_time_interval(),
    member_start_timeout => pooler_time_interval()
}.

-spec pool_child_spec(pool_options()) ->
    supervisor:child_spec().
pool_child_spec(Options) ->
    % ChildID pooler генерит сам добавляя префикс _pooler_
    pooler:pool_child_spec(maps:to_list(Options)).

-spec pool_do(atom(), fun((pid()) -> R)) ->
    R.
pool_do(PoolName, Fun) ->
    % TODO сделать нормально
    Timeout = {30, 'sec'},
    Pid =
        case pooler:take_member(PoolName, Timeout) of
            error_no_members ->
                % TODO log
                throw({temporary, storage_unavailable});
            Pid_ ->
                Pid_
        end,
    try
        R = Fun(Pid),
        ok = pooler:return_member(PoolName, Pid, ok),
        R
    catch Class:Reason ->
        ok = pooler:return_member(PoolName, Pid, fail),
        % TODO log
        erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

-spec get_machine_events_ids(mg:id(), mg_storage:machine(), mg:history_range() | undefined) ->
    [{mg:id(), mg:event_id()}].
get_machine_events_ids(MachineID, #{events_range:=MachineEventsRange}, RequestedRange) ->
    [{MachineID, EventID} || EventID <-
        get_event_ids(MachineEventsRange, expand_request_range(RequestedRange))].

%%
%% local
%%
-spec expand_request_range(mg:history_range() | undefined) ->
    mg:history_range().
expand_request_range(undefined) ->
    {undefined, undefined, forward};
expand_request_range(V={_, _, _}) ->
    V.

-spec get_event_ids(mg_storage:events_range(), mg:history_range()) ->
    _.
get_event_ids(undefined, _) ->
    [];
get_event_ids(R0, {Ef, N, Direction}) ->
    R1 = intersect_range(R0, Ef, Direction),
    R2 = limit_range(R1, N, Direction),
    enumerate_range(R2, Direction).

-spec intersect_range({mg:event_id(), mg:event_id()}, mg:event_id(), mg:direction()) ->
    {mg:event_id(), mg:event_id()}.
intersect_range(R, undefined, _) ->
    R;
intersect_range({A, B}, Ef, forward) when Ef >= A, Ef =< B ->
    {Ef + 1, B};
intersect_range({A, B}, Ef, backward) when Ef >= A, Ef =< B ->
    {A, Ef - 1}.

-spec limit_range({mg:event_id(), mg:event_id()}, undefined | non_neg_integer(), mg:direction()) ->
    {mg:event_id(), mg:event_id()}.
limit_range(R, undefined, _) ->
    R;
limit_range({A, B}, N, forward) ->
    {A, min(A + N - 1, B)};
limit_range({A, B}, N, backward) ->
    {max(B - N + 1, A), B}.

-spec enumerate_range({mg:event_id(), mg:event_id()}, mg:direction()) ->
    [mg:event_id()].
enumerate_range({A, B}, forward) ->
    lists:seq(A, B, 1);
enumerate_range({A, B}, backward) ->
    lists:seq(B, A, -1).
