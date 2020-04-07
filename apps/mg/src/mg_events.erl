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

%%%
%%% Базовые вещи связанные с эвентами.
%%%
-module(mg_events).

%% API
-export_type([id           /0]).
-export_type([body         /0]).
-export_type([content      /0]).
-export_type([event        /0]).
-export_type([event        /1]).
-export_type([history_range/0]).
-export_type([events_range /0]).

-export_type([format_version/0]).

%% events ranges intersection
-export([intersect_range/2]).
-export([slice_events/2]).

%% events generation
-export([generate_events_with_range/2]).

%% packer to opaque and kv
-export([events_range_to_opaque/1]).
-export([opaque_to_events_range/1]).
-export([event_id_to_key       /1]).
-export([key_to_event_id       /1]).
-export([event_to_kv           /1]).
-export([kv_to_event           /1]).
-export([events_to_kvs         /1]).
-export([kvs_to_events         /1]).
-export([event_to_kv           /2]).
-export([kv_to_event           /2]).
-export([events_to_kvs         /2]).
-export([kvs_to_events         /2]).
-export([event_to_opaque       /1]).
-export([opaque_to_event       /1]).
-export([event_to_opaque       /2]).
-export([opaque_to_event       /2]).
-export([events_to_opaques     /1]).
-export([opaques_to_events     /1]).
-export([content_to_opaque     /1]).
-export([opaque_to_content     /1]).
-export([history_range_to_opaque/1]).
-export([opaque_to_history_range/1]).
-export([maybe_to_opaque        /2]).
-export([maybe_from_opaque      /2]).
-export([identity               /1]).
-export([add_machine_id        /2]).
-export([remove_machine_id     /2]).

%%
%% API
%%
-type id   () :: pos_integer().
-type body () :: content().
-type event() :: event(body()).
-type event(T) :: #{
    id         => id(),
    created_at => integer(),  % in nanosecond
    body       => T
}.

-type content()  :: {metadata(), mg_storage:opaque()}.
-type metadata() :: #{
    % Версия формата данных
    %
    % Задаётся процессором и должна только _расти_ в процессе эволюции процессора. По умолчанию не задана,
    % что равноценно _минимально возможной_ версии.
    format_version => format_version()
}.

-type format_version() :: integer().

-type direction() :: forward | backward.
-type history_range() :: {After::id() | undefined, Limit::non_neg_integer() | undefined, direction()}.

%% не очень удобно, что получилось 2 формата range'а
%% надо подумать, как это исправить
-type events_range() :: mg_dirange:dirange(id()).

%%
%% events ranges intersection
%%
-spec intersect_range(events_range(), history_range()) ->
    events_range().
intersect_range(R0, {Ef, N, Direction}) ->
    R1 = orient_range(R0, Direction),
    R2 = chop_range(R1, Ef),
    R3 = limit_range(R2, N),
    R3.

-spec orient_range(events_range(), direction()) ->
    events_range().
orient_range(R, forward) ->
    mg_dirange:align(R, mg_dirange:forward(1, 1));
orient_range(R, backward) ->
    mg_dirange:align(R, mg_dirange:backward(1, 1)).

-spec chop_range(events_range(), _From :: id() | undefined) ->
    events_range().
chop_range(R0, undefined) ->
    R0;
chop_range(R0, Ef) when is_integer(Ef) ->
    {_, R1} = mg_dirange:dissect(R0, Ef), R1.

-spec limit_range(events_range(), undefined | non_neg_integer()) ->
    events_range().
limit_range(R, undefined) ->
    R;
limit_range(R, N) ->
    mg_dirange:limit(R, N).

-spec slice_events([event()], events_range()) ->
    [event()].
slice_events(Events = [#{id := First} | _], Range) ->
    D = mg_dirange:direction(Range),
    case mg_dirange:bounds(Range) of
        {A, B} when D == +1 ->
            lists:sublist(Events, A - First + 1, B - A + 1);
        {B, A} when D == -1 ->
            lists:reverse(lists:sublist(Events, A - First + 1, B - A + 1));
        undefined ->
            []
    end;
slice_events([], _) ->
    [].

%%
%% events generation
%%
-spec generate_events_with_range([body()], events_range()) ->
    {[event()], events_range()}.
generate_events_with_range(EventsBodies, EventsRange) ->
    {Events, NewLastEventID} = generate_events(EventsBodies, mg_dirange:to(EventsRange)),
    {Events, update_events_range(EventsRange, NewLastEventID)}.

-spec generate_events([body()], id() | undefined) ->
    {[event()], id() | undefined}.
generate_events(EventsBodies, LastID) ->
    lists:mapfoldl(
        fun generate_event/2,
        LastID,
        EventsBodies
    ).

-spec generate_event(body(), id() | undefined) ->
    {event(), id()}.
generate_event(EventBody, LastID) ->
    ID = get_next_event_id(LastID),
    Event =
        #{
            id         => ID,
            % We use `nanosecond` for backward compatibility
            created_at => os:system_time(nanosecond),
            body       => EventBody
        },
    {Event, ID}.

-spec get_next_event_id(undefined | id()) ->
    id().
get_next_event_id(undefined) ->
    1;
get_next_event_id(N) ->
    N + 1.

-spec update_events_range(events_range(), id() | undefined) ->
    events_range().
update_events_range(undefined, undefined) ->
    undefined;
update_events_range(undefined, NewLastEventID) ->
    mg_dirange:forward(get_next_event_id(undefined), NewLastEventID);
update_events_range(Range, NewLastEventID) ->
    mg_dirange:forward(mg_dirange:from(Range), NewLastEventID).

%%
%% packer to opaque
%%
%% events range
% TODO version
-spec events_range_to_opaque(events_range()) ->
    mg_storage:opaque().
events_range_to_opaque(undefined) ->
    null;
events_range_to_opaque(Range) ->
    {First, Last} = mg_dirange:bounds(Range),
    [First, Last].

-spec opaque_to_events_range(mg_storage:opaque()) ->
    events_range().
opaque_to_events_range(null) ->
    undefined;
opaque_to_events_range([First, Last]) ->
    mg_dirange:forward(First, Last).

%% event
-spec event_id_to_key(id()) ->
    mg_storage:key().
event_id_to_key(EventID) ->
    erlang:integer_to_binary(EventID).

-spec key_to_event_id(mg_storage:key()) ->
    id().
key_to_event_id(Key) ->
    erlang:binary_to_integer(Key).

-spec event_to_kv(event()) ->
    mg_storage:kv().
event_to_kv(Event) ->
    event_to_kv(Event, fun body_to_opaque/2).

-spec kv_to_event(mg_storage:kv()) ->
    event().
kv_to_event(Event) ->
    kv_to_event(Event, fun opaque_to_body/2).

-spec events_to_kvs([event()]) ->
    [mg_storage:kv()].
events_to_kvs(Events) ->
    [mg_events:event_to_kv(Event) || Event <- Events].

-spec kvs_to_events([mg_storage:kv()]) ->
    [event()].
kvs_to_events(Kvs) ->
    [mg_events:kv_to_event(Attr) || Attr <- Kvs].

-spec event_to_kv(event(T), fun((Vsn :: 1..2, T) -> mg_storage:opaque())) ->
    mg_storage:kv().
event_to_kv(#{id := EventID, created_at := Date, body := Body}, BodyToOpaque) ->
    Vsn = 2,
    {
        event_id_to_key(EventID),
        [Vsn, Date, BodyToOpaque(Vsn, Body)]
    }.

-spec kv_to_event(mg_storage:kv(), fun((Vsn :: 1..2, mg_storage:opaque()) -> T)) ->
    event(T).
kv_to_event({EventID, [Vsn, Date, OpaqueBody]}, OpaqueToBody) when
    Vsn =:= 1;
    Vsn =:= 2
->
    #{
        id         => key_to_event_id(EventID),
        created_at => Date,
        body       => OpaqueToBody(Vsn, OpaqueBody)
    }.

-spec events_to_kvs([event(T)], fun((Vsn :: 1..2, T) -> mg_storage:opaque())) ->
    [mg_storage:kv()].
events_to_kvs(Events, BodyToOpaque) ->
    [mg_events:event_to_kv(Event, BodyToOpaque) || Event <- Events].

-spec kvs_to_events([mg_storage:kv()], fun((Vsn :: 1..2, mg_storage:opaque()) -> T)) ->
    [event(T)].
kvs_to_events(Kvs, OpaqueToBody) ->
    [mg_events:kv_to_event(Attr, OpaqueToBody) || Attr <- Kvs].

-spec event_to_opaque(event()) ->
    mg_storage:opaque().
event_to_opaque(Event) ->
    event_to_opaque(Event, fun body_to_opaque/2).

-spec opaque_to_event(mg_storage:opaque()) ->
    event().
opaque_to_event(Event) ->
    opaque_to_event(Event, fun opaque_to_body/2).

-spec event_to_opaque(event(T), fun((Vsn :: 1..2, T) -> mg_storage:opaque())) ->
    mg_storage:opaque().
event_to_opaque(#{id := EventID, created_at := Date, body := Body}, BodyPacker) ->
    Vsn = 2,
    [Vsn, EventID, Date, BodyPacker(Vsn, Body)].

-spec opaque_to_event(mg_storage:opaque(), fun((Vsn :: 1..2, mg_storage:opaque()) -> T)) ->
    event(T).
opaque_to_event([Vsn, EventID, Date, Body], BodyUnpacker) when
    Vsn =:= 1;
    Vsn =:= 2
->
    #{
        id         => EventID,
        created_at => Date,
        body       => BodyUnpacker(Vsn, Body)
    }.

-spec events_to_opaques([event()]) ->
    [mg_storage:opaque()].
events_to_opaques(Events) ->
    [event_to_opaque(Event) || Event <- Events].

-spec opaques_to_events([mg_storage:opaque()]) ->
    [event()].
opaques_to_events(Opaques) ->
    [opaque_to_event(Opaque) || Opaque <- Opaques].

-spec body_to_opaque(2, body()) ->
    mg_storage:opaque().
body_to_opaque(2, Body) ->
    content_to_opaque(Body).

-spec opaque_to_body(1..2, mg_storage:opaque()) ->
    body().
opaque_to_body(2, Body) ->
    opaque_to_content(Body);
opaque_to_body(1, Body) ->
    {
        #{}, % пустая метадата
        Body
    }.

-spec content_to_opaque(content()) ->
    mg_storage:opaque().
content_to_opaque({Metadata, Body}) ->
    [1, metadata_to_opaque(Metadata), Body].

-spec opaque_to_content(mg_storage:opaque()) ->
    content().
opaque_to_content([1, Metadata, Body]) ->
    {opaque_to_metadata(Metadata), Body}.

-spec metadata_to_opaque(metadata()) ->
    mg_storage:opaque().
metadata_to_opaque(Metadata) ->
    maps:fold(
        fun
            (format_version, Vsn, Acc) -> [<<"fv">>, Vsn] ++ Acc
        end,
        [],
        Metadata
    ).

-spec opaque_to_metadata(mg_storage:opaque()) ->
    metadata().
opaque_to_metadata(Metadata) ->
    opaque_to_metadata(Metadata, #{}).

-spec opaque_to_metadata([mg_storage:opaque()], metadata()) ->
    metadata().
opaque_to_metadata([<<"fv">>, Vsn | Rest], Metadata) ->
    opaque_to_metadata(Rest, Metadata#{format_version => Vsn});
opaque_to_metadata([], Metadata) ->
    Metadata.

-spec history_range_to_opaque(history_range()) ->
    mg_storage:opaque().
history_range_to_opaque({After, Limit, Direction}) ->
    [1,
        maybe_to_opaque(After, fun identity/1),
        maybe_to_opaque(Limit, fun identity/1),
        direction_to_opaque(Direction)
    ].

-spec opaque_to_history_range(mg_storage:opaque()) ->
    history_range().
opaque_to_history_range([1, After, Limit, Direction]) ->
    {
        maybe_from_opaque(After, fun identity/1),
        maybe_from_opaque(Limit, fun identity/1),
        opaque_to_direction(Direction)
    }.

-spec direction_to_opaque(direction()) ->
    mg_storage:opaque().
direction_to_opaque(forward ) -> 1;
direction_to_opaque(backward) -> 2.

-spec opaque_to_direction(mg_storage:opaque()) ->
    direction().
opaque_to_direction(1) -> forward ;
opaque_to_direction(2) -> backward.

-spec maybe_to_opaque(undefined | T0, fun((T0) -> T1)) ->
    T1.
maybe_to_opaque(undefined, _) ->
    null;
maybe_to_opaque(T0, ToT1) ->
    ToT1(T0).

-spec maybe_from_opaque(null | T0, fun((T0) -> T1)) ->
    T1.
maybe_from_opaque(null, _) ->
    undefined;
maybe_from_opaque(T0, ToT1) ->
    ToT1(T0).

-spec identity(T) ->
    T.
identity(V) ->
    V.


-spec add_machine_id
    (mg:id(), T) -> T when T :: mg_storage:kv();
    (mg:id(), T) -> T when T :: mg_storage:key();
    (mg:id(), T) -> T when T :: [mg_storage:kv() | mg_storage:key()].
add_machine_id(MachineID, List) when is_list(List) ->
    [add_machine_id(MachineID, Element) || Element <- List];
add_machine_id(MachineID, {K, V}) ->
    {add_machine_id(MachineID, K), V};
add_machine_id(MachineID, K) ->
    <<MachineID/binary, $_, K/binary>>.

-spec remove_machine_id
    (mg:id(), T) -> T when T :: mg_storage:kv();
    (mg:id(), T) -> T when T :: mg_storage:key();
    (mg:id(), T) -> T when T :: [mg_storage:kv() | mg_storage:key()].
remove_machine_id(MachineID, List) when is_list(List) ->
    [remove_machine_id(MachineID, Element) || Element <- List];
remove_machine_id(MachineID, {K, V}) ->
    {remove_machine_id(MachineID, K), V};
remove_machine_id(MachineID, K) ->
    Size = erlang:byte_size(MachineID),
    <<MachineID:Size/binary, $_, NewK/binary>> = K,
    NewK.
