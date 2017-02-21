%%%
%%% Базовые вещи связанные с эвентами.
%%%
-module(mg_events).

%% API
-export_type([id           /0]).
-export_type([body         /0]).
-export_type([event        /0]).
-export_type([event        /1]).
-export_type([history_range/0]).
-export_type([events_range /0]).

%% events ranges intersection
-export([get_event_ids/2]).

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
-export([body_to_opaque        /1]).
-export([opaque_to_body        /1]).
-export([add_machine_id        /2]).
-export([remove_machine_id     /2]).

%%
%% API
%%
-type id   () :: pos_integer().
-type body () :: mg:opaque().
-type event() :: event(body()).
-type event(T) :: #{
    id         => id(),
    created_at => genlib_time:ts(),
    body       => T
}.
-type direction() :: forward | backward.
-type history_range() :: {After::id() | undefined, Limit::non_neg_integer() | undefined, direction()}.

%% не очень удобно, что получилось 2 формата range'а
%% надо подумать, как это исправить
-type events_range() :: {First::id(), Last::id()} | undefined.

%%
%% events ranges intersection
%%
-spec get_event_ids(events_range(), history_range()) ->
    [id()].
get_event_ids(undefined, _) ->
    [];
get_event_ids(R0, {Ef, N, Direction}) ->
    R1 = intersect_range(R0, Ef, Direction),
    R2 = limit_range(R1, N, Direction),
    enumerate_range(R2, Direction).

-spec intersect_range({id(), id()}, undefined | id(), direction()) ->
    {id(), id()}.
intersect_range(R, undefined, _) ->
    R;
intersect_range({A, B}, Ef, forward) when Ef >= A, Ef =< B ->
    {Ef + 1, B};
intersect_range({A, B}, Ef, backward) when Ef >= A, Ef =< B ->
    {A, Ef - 1}.

-spec limit_range({id(), id()}, undefined | non_neg_integer(), direction()) ->
    {id(), id()}.
limit_range(R, undefined, _) ->
    R;
limit_range({A, B}, N, forward) ->
    {A, min(A + N - 1, B)};
limit_range({A, B}, N, backward) ->
    {max(B - N + 1, A), B}.

-spec enumerate_range({id(), id()}, direction()) ->
    [id()].
enumerate_range({A, B}, forward) ->
    lists:seq(A, B, 1);
enumerate_range({A, B}, backward) ->
    lists:seq(B, A, -1).

%%
%% events generation
%%
-spec generate_events_with_range([body()], events_range()) ->
    {[event()], events_range()}.
generate_events_with_range(EventsBodies, EventsRange) ->
    {Events, NewLastEventID} = generate_events(EventsBodies, get_last_event_id(EventsRange)),
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
            created_at => erlang:system_time(),
            body       => EventBody
        },
    {Event, ID}.

-spec get_last_event_id(events_range()) ->
    id() | undefined.
get_last_event_id(undefined) ->
    undefined;
get_last_event_id({_, LastID}) ->
    LastID.

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
    {get_next_event_id(undefined), NewLastEventID};
update_events_range({FirstEventID, _}, NewLastEventID) ->
    {FirstEventID, NewLastEventID}.


%%
%% packer to opaque
%%
%% events range
% TODO version
-spec events_range_to_opaque(events_range()) ->
    mg:opaque().
events_range_to_opaque(undefined) ->
    null;
events_range_to_opaque({First, Last}) ->
    [First, Last].

-spec opaque_to_events_range(mg:opaque()) ->
    events_range().
opaque_to_events_range(null) ->
    undefined;
opaque_to_events_range([First, Last]) ->
    {First, Last}.

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
    event_to_kv(Event, fun body_to_opaque/1).

-spec kv_to_event(mg_storage:kv()) ->
    event().
kv_to_event(Event) ->
    kv_to_event(Event, fun body_to_opaque/1).

-spec events_to_kvs([event()]) ->
    [mg_storage:kv()].
events_to_kvs(Events) ->
    [mg_events:event_to_kv(Event) || Event <- Events].

-spec kvs_to_events([mg_storage:kv()]) ->
    [event()].
kvs_to_events(Kvs) ->
    [mg_events:kv_to_event(Attr) || Attr <- Kvs].

-spec event_to_kv(event(T), fun((T) -> mg:opaque())) ->
    mg_storage:kv().
event_to_kv(#{id := EventID, created_at := Date, body := Body}, BodyToOpaque) ->
    {
        event_id_to_key(EventID),
        [1, Date, BodyToOpaque(Body)]
    }.

-spec kv_to_event(mg_storage:kv(), fun((mg:opaque()) -> T)) ->
    event(T).
kv_to_event({EventID, [1, Date, OpaqueBody]}, OpaqueToBody) ->
    #{
        id         => key_to_event_id(EventID),
        created_at => Date,
        body       => OpaqueToBody(OpaqueBody)
    }.

-spec events_to_kvs([event(T)], fun((T) -> mg:opaque())) ->
    [mg_storage:kv()].
events_to_kvs(Events, BodyToOpaque) ->
    [mg_events:event_to_kv(Event, BodyToOpaque) || Event <- Events].

-spec kvs_to_events([mg_storage:kv()], fun((mg:opaque()) -> T)) ->
    [event(T)].
kvs_to_events(Kvs, OpaqueToBody) ->
    [mg_events:kv_to_event(Attr, OpaqueToBody) || Attr <- Kvs].

-spec event_to_opaque(event()) ->
    mg:opaque().
event_to_opaque(Event) ->
    event_to_opaque(Event, fun body_to_opaque/1).

-spec opaque_to_event(mg:opaque()) ->
    event().
opaque_to_event(Event) ->
    opaque_to_event(Event, fun opaque_to_body/1).

-spec event_to_opaque(event(T), fun((T) -> mg:opaque())) ->
    mg:opaque().
event_to_opaque(#{id := EventID, created_at := Date, body := Body}, BodyPacker) ->
    [1, EventID, Date, BodyPacker(Body)].

-spec opaque_to_event(mg:opaque(), fun((mg:opaque()) -> T)) ->
    event(T).
opaque_to_event([1, EventID, Date, Body], BodyUnpacker) ->
    #{
        id         => EventID,
        created_at => Date,
        body       => BodyUnpacker(Body)
    }.

-spec events_to_opaques([event()]) ->
    [mg_storage:opaque()].
events_to_opaques(Events) ->
    [event_to_opaque(Event) || Event <- Events].

-spec opaques_to_events([mg_storage:opaque()]) ->
    [event()].
opaques_to_events(Opaques) ->
    [opaque_to_event(Opaque) || Opaque <- Opaques].

-spec body_to_opaque(body()) ->
    mg:opaque().
body_to_opaque(Body) ->
    Body.

-spec opaque_to_body(mg:opaque()) ->
    body().
opaque_to_body(Body) ->
    Body.

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
