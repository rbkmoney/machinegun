-module(mg_storage_utils).

%% API
-export([try_set_timer         /3]).
-export([get_machine_events_ids/3]).

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

%%
%% Я, блин над этой хернёй провёл целый день!
%% Пытался сделать без лапшевидного кода.
%% Алгоритм следующий:
%%  Основная его идея в том, чтобы при запросе с HistoryRange.direction=backward,
%%   просто отразить результаты относительно середины диапазона эвентов машины,
%%   а не городить лапшу из условий.
%%  - отражаем точку, которую запросили (HistoryRange.after)
%%  - находим поддиапазон применяя HistoryRange к диапазону эвентов машины
%%  - отражаем результирующий отрезок
%%
%% Такой, вот, забавный алгоритм.
%% Если кто знает как сделать попроще, то милости просим. :)
%%
-spec get_machine_events_ids(mg:id(), mg_storage:machine(), mg:history_range()) ->
    [{mg:id(), mg:event_id()}].
get_machine_events_ids(MachineID, #{events_range:=MachineEventsRange}, RequestedRange) ->
    {ReqFrom, Limit, Direction} = expand_request_range(RequestedRange),
    ResultRange =
        mirror_by_direction(
            MachineEventsRange,
            subrange(
                MachineEventsRange,
                mirror_by_direction(MachineEventsRange, ReqFrom, Direction),
                Limit
            ),
            Direction
        ),
    [{MachineID, EventID} || EventID <- generate_events_ids(ResultRange, Direction)].

-spec expand_request_range(mg:history_range() | undefined) ->
    mg:history_range().
expand_request_range(undefined) ->
    {undefined, undefined, forward};
expand_request_range(V={_, _, _}) ->
    V.

-spec subrange(mg_storage:events_range(), mg:event_id() | undefined, non_neg_integer() | undefined) ->
    mg_storage:events_range() | undefined.
subrange(undefined, undefined, _) ->
    undefined;
% TODO event_not_found
% subrange(undefined, _, _) ->
%     throw(event_not_found);
subrange(Range={From, To}, ReqFrom, undefined) ->
    subrange(Range, ReqFrom, To - From + 1);
subrange({From, To}, undefined, Limit) ->
    {From, erlang:min(From + Limit - 1, To)};
subrange({From, To}, ReqFrom, Limit) when From =< ReqFrom, ReqFrom =< To ->
    {ReqFrom + 1, erlang:min(ReqFrom + 1 + Limit - 1, To)};
subrange(_, _, _) ->
    % TODO event_not_found
    % throw(event_not_found).
    undefined.


-spec mirror_by_direction(mg_storage:events_range()|integer(), mg_storage:events_range()|integer(), mg:direction()) ->
    mg_storage:events_range() | integer().
mirror_by_direction(_A, B, forward) ->
    B;
mirror_by_direction(A, B, backward) ->
    mirror(A, B).

-spec mirror({integer(), integer()} | undefined | integer(), {integer(), integer()} | undefined | integer()) ->
    {integer(), integer()} | undefined | integer().
mirror(undefined, _) ->
    undefined;
mirror(_, undefined) ->
    undefined;
mirror({AxisA, AxisB}, Subj) ->
    % чтобы не было дробных чисел
    'div'(mirror(AxisA + AxisB, 'mul'(Subj, 2)), 2);
mirror(Axis, {A, B}) when is_integer(Axis) ->
    {mirror(Axis, A), mirror(Axis, B)};
mirror(Axis, N) when is_integer(Axis), is_integer(N) ->
    Axis + erlang:abs(Axis - N).

-spec 'mul'({integer(), integer()} | integer(), integer()) ->
    {integer(), integer()} | integer().
'mul'({A, B}, N) when is_integer(N) ->
    {'mul'(A, N), 'mul'(B, N)};
'mul'(N1, N2) when is_integer(N1), is_integer(N1) ->
    N1 * N2.

-spec 'div'({integer(), integer()} | integer(), integer()) ->
    {integer(), integer()} | integer().
'div'({A, B}, N) when is_integer(N) ->
    {'div'(A, N), 'div'(B, N)};
'div'(N1, N2) when is_integer(N1), is_integer(N2) ->
    N1 div N2.

-spec generate_events_ids(mg_storage:events_range(), mg:direction()) ->
    [mg:event_id()].
generate_events_ids(undefined, _) ->
    [];
generate_events_ids({From, To}, Direction) ->
    Delta =
        case Direction of
            forward  -> 1 ;
            backward -> -1
        end,
    lists:seq(From, To, Delta).
