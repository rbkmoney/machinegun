%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% Умеет обрабатывать call/cast, хибернейтиться и выгружаться по таймауту.
%%% В будущем планируется реплицировать эвенты между своими инстансами на разных нодах.
%%% Не падает при получении неожиданных запросов, и пишет их в error_logger.
%%%
%%% Возможно стоит ещё сделать
%%%  - format_state
%%% ?
%%%
%%% Реализует понятие тэгов.
%%% При падении хендлера переводит машину в error состояние.
%%%
-module(mg_machine).

%% API
-export_type([options/0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([start      /3]).
-export([repair     /3]).
-export([call       /3]).
-export([get_history/3]).

%% Internal API
-export([handle_timeout/2]).
-export([touch         /3]).

%% supervisor
-behaviour(supervisor).
-export([init      /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/2, handle_call/2, handle_cast/2, handle_unload/1]).

%%
%% API
%%
-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_utils:mod_opts(),
    processor => mg_utils:mod_opts(),
    observer  => mg_utils:mod_opts() | undefined
}.

-define(safe(Expr),
    try
        Expr
    catch
        Class:Reason ->
            Stacktrace = erlang:get_stacktrace(),
            handle_error({Class, Reason, Stacktrace})
    end
).

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.


-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

-spec start(options(), mg:id(), mg:args()) ->
    ok.
start(Options, ID, Args) ->
    ?safe(
        begin
            % создать в бд
            ok = mg_storage:create(get_options(storage, Options), ID, Args),
            % зафорсить загрузку
            ok = touch(Options, ID, sync)
        end
    ).

-spec repair(options(), mg:ref(), mg:args()) ->
    ok.
repair(Options, Ref, Args) ->
    ?safe(
        reraise(
            mg_workers_manager:call(
                manager_options(Options),
                ref2id(Options, Ref),
                {repair, Args}
            )
        )
    ).

-spec call(options(), mg:ref(), mg:args()) ->
    _Resp.
call(Options, Ref, Call) ->
    ?safe(
        reraise(
            mg_workers_manager:call(
                manager_options(Options),
                ref2id(Options, Ref),
                {call, Call}
            )
        )
    ).

-spec get_history(options(), mg:ref(), mg:history_range()) ->
    mg:history().
get_history(Options, Ref, Range) ->
    ?safe(
        mg_storage:get_history(
            get_options(storage, Options),
            ref2id(Options, Ref),
            Range
        )
    ).

%%
%% Internal API
%%
-spec handle_timeout(options(), _ID) ->
    ok.
handle_timeout(Options, ID) ->
    ok = mg_workers_manager:cast(manager_options(Options), ID, timeout).

-spec touch(options(), _ID, sync | async) ->
    ok.
touch(Options, ID, async) ->
    ok = mg_workers_manager:cast(manager_options(Options), ID, touch);
touch(Options, ID, sync) ->
    ok = mg_workers_manager:call(manager_options(Options), ID, touch).

%%
%% supervisor
%%
-spec init(options()) ->
    mg_utils:supervisor_ret().
init(Options) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_workers_manager:child_spec(manager, manager_options(Options)),
        mg_storage:child_spec(get_options(storage, Options), storage, {?MODULE, handle_timeout, [Options]})
    ]}}.

%%
%% mg_worker callbacks
%%
-type state() :: #{
    id      => mg:id(),
    options => options(),
    status  => mg_storage:status(),
    history => mg:history(),
    tag_to_add   => undefined | mg:tag(),
    event_to_add => [mg:event()]
}.

-spec handle_load(_ID, module()) ->
    {ok, state()} | {error, mg_storage:error()}.
handle_load(ID, Options) ->
    try
        Status =
            case mg_storage:get_status(get_options(storage, Options), ID) of
                undefined ->
                    % FIXME
                    throw({storage, machine_not_found});
                Status_ ->
                    Status_
            end,
        State =
            #{
                id      => ID,
                options => Options,
                status  => Status,
                history => mg_storage:get_history(get_options(storage, Options), ID, undefined),
                tag_to_add   => undefined,
                event_to_add => []
            },
        {ok, transit_state(State, handle_load_(State))}
    catch throw:DBError ->
        {error, DBError}
    end.

-spec handle_call(_Call, state()) ->
    {_Resp, state()}.
handle_call(Call, State) ->
    {Resp, NewState} = handle_call_(Call, State),
    {Resp, transit_state(State, NewState)}.

-spec handle_cast(_Cast, state()) ->
    state().
handle_cast(Cast, State) ->
    transit_state(State, handle_cast_(Cast, State)).

-spec handle_unload(state()) ->
    ok.
handle_unload(_) ->
    ok.

%%

-spec transit_state(state(), state()) ->
    state().
transit_state(#{id:=ID}, NewState=#{id:=NewID}) when NewID =:= ID ->
    #{options:=Options, status:=NewStatus, event_to_add:=NewEvents, tag_to_add:=NewTag} = NewState,
    ok = mg_storage:update(get_options(storage, Options), ID, NewStatus, NewEvents, NewTag),
    NewState#{event_to_add:=[], tag_to_add:=undefined}.

%%

-spec handle_load_(state()) ->
    state().
handle_load_(State=#{id:=ID, status:={created, Args}}) ->
    process_signal({init, ID, Args}, State);
handle_load_(State) ->
    State.

-spec handle_call_(_Call, state()) ->
    {_Replay, state()}.
handle_call_({call, Call}, State=#{status:={working, _}}) ->
    process_call(Call, State);
handle_call_({call, _Call}, State=#{status:={error, _}}) ->
    {{throw, {internal, {machine_failed, bad_machine_state}}}, State};
handle_call_({repair, Args}, State=#{status:={error, _}}) ->
    %% TODO кидать machine_failed при падении запроса
    {ok, process_signal({repair, Args}, State)};
handle_call_(touch, State) ->
    {ok, State};
handle_call_(Call, State) ->
    ok = error_logger:error_msg("unexpected mg_worker call received: ~p", [Call]),
    {{error, badarg}, State}.

-spec handle_cast_(_Cast, state()) ->
    state().
handle_cast_(timeout, State=#{status:={working, _}}) ->
    process_signal(timeout, State);
handle_cast_(touch, State) ->
    State;
handle_cast_(Cast, State) ->
    ok = error_logger:error_msg("unexpected mg_worker cast received: ~p", [Cast]),
    State.

%%

-spec process_call(_Call, state()) ->
    {{ok, _Resp}, state()}.
process_call(Call, State=#{options:=Options, history:=History}) ->
    try
        mg_processor:process_call(
            get_options(processor, Options),
            {Call, History}
        )
    of
        CallResult ->
            {Response, EventsBodies, ComplexAction} = CallResult,
            {{ok, Response}, handle_processor_result(EventsBodies, ComplexAction, State)}
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        {{error, Exception}, handle_processor_error(Exception, State)}
    end.


-spec process_signal(mg:signal(), state()) ->
    state().
process_signal(Signal, State=#{options:=Options, history:=History}) ->
    try
        mg_processor:process_signal(
            get_options(processor, Options),
            {Signal, History}
        )
    of
        {EventsBodies, ComplexAction} ->
            handle_processor_result(EventsBodies, ComplexAction, State)
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        handle_processor_error(Exception, State)
    end.

%%

-spec handle_processor_result([mg:event_body()], mg:complex_action(), state()) ->
    state().
handle_processor_result(EventsBodies, ComplexAction, State) ->
    Events = generate_events(EventsBodies, get_last_event_id(State)),
    ok = notify_observer(Events, State),
    do_complex_action(ComplexAction, append_events_to_history(Events, State)).

-spec handle_processor_error(_Reason, state()) ->
    state().
handle_processor_error(Reason, State) ->
    ok = error_logger:error_msg("processor call error: ~p", [Reason]),
    set_status({error, Reason}, State).

-spec notify_observer([mg:event()], state()) ->
    ok.
notify_observer(Events, #{id:=SourceID, options:=Options}) ->
    case get_options(observer, Options) of
        undefined ->
            ok;
        Observer ->
            ok = mg_observer:handle_events(Observer, SourceID, Events)
    end.

-spec append_events_to_history([mg:event()], state()) ->
    state().
append_events_to_history(Events, State=#{history:=History}) ->
    State#{history := History ++ Events, event_to_add:=Events}.

-spec generate_events([mg:event_body()], mg:event_id()) ->
    [mg:event()].
generate_events(EventsBodies, LastID) ->
    {Events, _} =
        lists:mapfoldl(
            fun generate_event/2,
            LastID,
            EventsBodies
        ),
    Events.

-spec generate_event(mg:event_body(), mg:event_id()) ->
    {mg:event(), mg:event_id()}.
generate_event(EventBody, LastID) ->
    ID = get_next_event_id(LastID),
    Event =
        #{
            id         => ID,
            created_at => erlang:system_time(),
            body       => EventBody
        },
    {Event, ID}.

-spec get_last_event_id(state()) ->
    mg:event_id() | undefined.
get_last_event_id(#{history:=[]}) ->
    undefined;
get_last_event_id(#{history:=History}) ->
    #{id:=ID} = lists:last(History),
    ID.

-spec get_next_event_id(undefined | mg:event_id()) ->
    mg:event_id().
get_next_event_id(undefined) ->
    1;
get_next_event_id(N) ->
    N + 1.

-spec do_complex_action(mg:complex_action(), state()) ->
    state().
do_complex_action(#{timer := SetTimerAction, tag := TagAction}, State) ->
    do_set_timer_action(SetTimerAction, do_tag_action(TagAction, State)).

%%
%% utils
%%
-spec manager_options(options()) ->
    mg_workers_manager:options().
manager_options(Options) ->
    #{
        name           => maps:get(namespace, Options),
        worker_options => {?MODULE, Options}
    }.

-spec do_tag_action(undefined | mg:tag_action(), state()) ->
    state().
do_tag_action(undefined, State) ->
    State;
do_tag_action(Tag, State) ->
    State#{tag_to_add:=Tag}.

-spec do_set_timer_action(undefined | mg:set_timer_action(), state()) ->
    state().
do_set_timer_action(TimerAction, State) ->
    set_status({working, get_timeout_datetime(TimerAction)}, State).

-spec set_status(mg_storage:status(), state()) ->
    state().
set_status(NewStatus, State) ->
    State#{status:=NewStatus}.

-spec ref2id(options(), mg:ref()) ->
    _ID.
ref2id(Options, {tag, Tag}) ->
    case mg_storage:resolve_tag(get_options(storage, Options), Tag) of
        undefined ->
            throw({storage, machine_not_found});
        ID ->
            ID
    end;
ref2id(_, {id, ID}) ->
    ID.

-spec get_options(processor | storage | observer, options()) ->
    mg_utils:mod_opts().
get_options(Subj=observer, Options) ->
    maps:get(Subj, Options, undefined);
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec get_timeout_datetime(undefined | mg:timer()) ->
    calendar:datetime() | undefined.
get_timeout_datetime(undefined) ->
    undefined;
get_timeout_datetime({deadline, Daytime}) ->
    Daytime;
get_timeout_datetime({timeout, Timeout}) ->
    calendar:gregorian_seconds_to_datetime(
        calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + Timeout
    ).

%%
%% error handling
%%
%% TODO поправить спеки
-spec handle_error(_) ->
    _.
handle_error(Error={Class=throw, Reason, _}) ->
    _ = log_error(Error),
    erlang:throw(map_error(Class, Reason));
handle_error({Class, Reason, Stacktrace}) ->
    erlang:raise(Class, Reason, Stacktrace).

-spec map_error(_, _) ->
    _.
map_error(throw, {workers, {loading, Error={storage, _}}}) ->
    map_error(throw, Error);
map_error(throw, {storage, machine_not_found}) ->
    machine_not_found;
map_error(throw, {storage, machine_already_exist}) ->
    machine_already_exist;
map_error(throw, {storage, _}) ->
    % TODO
    exit(todo);
map_error(throw, {processor, _}) ->
    machine_failed;
map_error(throw, {internal, {machine_failed, _}}) ->
    machine_failed.

-spec log_error({_, _, _}) ->
    _.
log_error({Class, Reason, Stacktrace}) ->
    ok = error_logger:error_msg("machine error ~p:~p ~p", [Class, Reason, Stacktrace]).

-spec reraise(_) ->
    _.
reraise(ok) ->
    ok;
reraise({ok, R}) ->
    R;
reraise({throw, E}) ->
    erlang:throw(E);
reraise({error, {Class, Reason, Stacktrace}}) ->
    erlang:raise(Class, Reason, Stacktrace);
reraise({error, Reason}) ->
    erlang:error(Reason).
