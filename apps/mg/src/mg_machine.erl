%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% Умеет обрабатывать call/cast, хибернейтиться и выгружаться по таймауту.
%%% В будущем планируется реплицировать эвенты между своими инстансами на разных нодах.
%%% Не падает при получении неожиданных запросов, и пишет их в error_logger.
%%%
%%% Возможно стоит ещё сделать
%%%  - прокинуть ID во все запросы
%%%  - attach/detach
%%%  - init/teminate
%%%  - handle_info
%%%  - format_state
%%%  - подписку на эвенты
%%%  - время жизни
%%% ?
%%%
%%% Реализует понятие тэгов.
%%% При падении хендлера переводит машину в error состояние.
%%%
-module(mg_machine).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([options/0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([start      /3]).
-export([repair     /3]).
-export([call       /4]).
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
    db        => mg_utils:mod_opts(),
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
            ok = mg_db:create_machine(get_options(db, Options), ID, Args),
            % зафорсить загрузку
            ok = touch(Options, ID, sync)
        end
    ).

-spec repair(options(), mg:ref(), mg:args()) ->
    ok.
repair(Options, Ref, Args) ->
    ?safe(
        ok = mg_workers_manager:cast(
                manager_options(Options),
                ref2id(Options, Ref),
                {repair, Args}
            )
    ).

-spec call(options(), mg:ref(), mg:args(), mg:call_context()) ->
    {_Resp, mg:call_context()}.
call(Options, Ref, Call, Context) ->
    ?safe(
        reraise(
            mg_workers_manager:call(
                manager_options(Options),
                ref2id(Options, Ref),
                {call, Call, Context}
            )
        )
    ).

-spec get_history(options, mg:ref(), mg:history_range()) ->
    mg:history().
get_history(Options, Ref, Range) ->
    {_, _, History, _} =
        ?safe(
            check_machine_status(
                mg_db:get_machine(
                    get_options(db, Options),
                    ref2id(Options, Ref),
                    Range
                )
            )
        ),
    History.

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
        mg_db:child_spec(get_options(db, Options), db)
    ]}}.

%%
%% mg_worker callbacks
%%
-type state() :: #{
    id      => mg:id(),
    options => options(),
    status  => mg_db:status(),
    history => mg:history(),
    tags    => [mg:tag()]
}.

-spec handle_load(_ID, module()) ->
    {ok, state()} | {error, mg_db:error()}.
handle_load(ID, Options) ->
    try
        {ID, Status, History, Tags} = mg_db:get_machine(get_options(db, Options), ID, undefined),
        State =
            #{
                id      => ID,
                options => Options,
                status  => Status,
                history => History,
                tags    => Tags
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
transit_state(OldState=#{id:=OldID}, NewState=#{id:=NewID, options:=Options}) when NewID =:= OldID ->
    ok = mg_db:update_machine(
            get_options(db, Options),
            state_to_machine(OldState),
            state_to_machine(NewState),
            {?MODULE, handle_timeout, [Options]}
        ),
    NewState.

-spec state_to_machine(state()) ->
    mg_db:machine().
state_to_machine(#{id:=ID, status:=Status, history:=History, tags:=Tags}) ->
    {ID, Status, History, Tags}.

%%

-spec handle_load_(state()) ->
    state().
handle_load_(State=#{id:=ID, status:={created, Args}}) ->
    process_signal({init, #'InitSignal'{id=ID, arg=Args}}, State);
handle_load_(State) ->
    State.

-spec handle_call_(_Call, state()) ->
    {_Replay, state()}.
handle_call_({call, Call, Context}, State=#{status:={working, _}}) ->
    process_call(Call, Context, State);
handle_call_({call, _Call, _Context}, State=#{status:={error, _}}) ->
    {{throw, {internal, {machine_failed, bad_machine_state}}}, State};
handle_call_(touch, State) ->
    {ok, State};
handle_call_(Call, State) ->
    ok = error_logger:error_msg("unexpected mg_worker call received: ~p", [Call]),
    {{error, badarg}, State}.

-spec handle_cast_(_Cast, state()) ->
    state().
handle_cast_(timeout, State=#{status:={working, _}}) ->
    process_signal({timeout, #'TimeoutSignal'{}}, State);
handle_cast_({repair, Args}, State=#{status:={error, _}}) ->
    process_signal({repair, #'RepairSignal'{arg=Args}}, State);
handle_cast_(touch, State) ->
    State;
handle_cast_(Cast, State) ->
    ok = error_logger:error_msg("unexpected mg_worker cast received: ~p", [Cast]),
    State.

%%

-spec process_call(_Call, mg:call_context(), state()) ->
    {{ok, {_Resp, mg:call_context()}}, state()}.
process_call(Call, Context, State=#{options:=Options, history:=History}) ->
    try
        {CallResult, NewContext} =
            mg_processor:process_call(
                get_options(processor, Options),
                #'CallArgs'{call=Call, history=History},
                Context
            ),
        #'CallResult'{
            events   = EventsBodies,
            action   = ComplexAction,
            response = Response
        } = CallResult,
        {{ok, {Response, NewContext}}, handle_processor_result(EventsBodies, ComplexAction, State)}
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        {{error, Exception}, handle_processor_error(Exception, State)}
    end.

-spec process_signal(mg:signal(), state()) ->
    state().
process_signal(Signal, State=#{options:=Options, history:=History}) ->
    try
        #'SignalResult'{
            events   = EventsBodies,
            action   = ComplexAction
        } =
            mg_processor:process_signal(
                get_options(processor, Options),
                #'SignalArgs'{signal=Signal, history=History}
            ),
        handle_processor_result(EventsBodies, ComplexAction, State)
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        handle_processor_error(Exception, State)
    end.

%%

-spec handle_processor_result(mg:events_bodies(), mg:complex_action(), state()) ->
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
    State#{history := Events ++ History}.

-spec generate_events([mg:event_body()], mg:event_id()) ->
    [mg:event()].
generate_events(EventsBodies, LastID) ->
    {Events, _} =
        lists:mapfoldr(
            fun generate_event/2,
            LastID,
            EventsBodies
        ),
    Events.

-spec generate_event(mg:event_body(), mg:event_id()) ->
    mg:event().
generate_event(EventBody, LastID) ->
    ID = get_next_event_id(LastID),
    {ok, CreatedAt} = rfc3339:format(erlang:system_time()),
    Event =
        #'Event'{
            id            = ID,
            created_at    = CreatedAt,
            event_payload = EventBody
        },
    {Event, ID}.

-spec get_last_event_id(state()) ->
    mg:event_id() | undefined.
get_last_event_id(#{history:=[]}) ->
    undefined;
get_last_event_id(#{history:=[#'Event'{id=ID}|_]}) ->
    ID.

-spec get_next_event_id(undefined | mg:event_id()) ->
    mg:event_id().
get_next_event_id(undefined) ->
    1;
get_next_event_id(N) ->
    N + 1.

-spec do_complex_action(mg:complex_action(), state()) ->
    state().
do_complex_action(#'ComplexAction'{set_timer=SetTimerAction, tag=TagAction}, State) ->
    do_set_timer_action(SetTimerAction, do_tag_action(TagAction, State)).

%%
%% utils
%%
-spec manager_options(options()) ->
    _TODO.
manager_options(Options) ->
    {?MODULE, Options}.

-spec do_tag_action(undefined | mg:tag_action(), state()) ->
    state().
do_tag_action(undefined, State) ->
    State;
do_tag_action(#'TagAction'{tag=Tag}, State=#{tags:=Tags}) ->
    % TODO детектор коллизий тэгов
    State#{tags:=[Tag|Tags]}.


-spec do_set_timer_action(undefined | mg:set_timer_action(), state()) ->
    state().
do_set_timer_action(TimerAction, State) ->
    set_status({working, get_timeout_datetime(TimerAction)}, State).

-spec set_status(mg_db:status(), state()) ->
    state().
set_status(NewStatus, State) ->
    State#{status:=NewStatus}.

-spec check_machine_status(mg_db:machine()) ->
    mg_db:machine() | no_return().
check_machine_status(Machine) ->
    % TODO error handling
    Machine.

-spec ref2id(options(), mg:ref()) ->
    _ID.
ref2id(Options, {tag, Tag}) ->
    mg_db:resolve_tag(get_options(db, Options), Tag);
ref2id(_, {id, ID}) ->
    ID.

-spec get_options(processor | db | observer, options()) ->
    mg_utils:mod_opts().
get_options(Subj=observer, Options) ->
    maps:get(Subj, Options, undefined);
get_options(Subj, Options) ->
    maps:get(Subj, Options).

-spec get_timeout_datetime(undefined | mg:timer()) ->
    calendar:datetime() | undefined.
get_timeout_datetime(undefined) ->
    undefined;
get_timeout_datetime(#'SetTimerAction'{timer=Timer}) ->
    case Timer of
        {deadline, Timestamp} ->
            parse_rfc3339_timespamp(Timestamp);
        {timeout, Timeout} ->
            calendar:gregorian_seconds_to_datetime(
                calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + Timeout
            )
    end.

%% rfc3339:parse имеет некорретный спек, поэтому диалайзер всегда ругается
-dialyzer({nowarn_function, parse_rfc3339_timespamp/1}).
-spec parse_rfc3339_timespamp(binary()) ->
    calendar:datetime() | no_return().
parse_rfc3339_timespamp(Timestamp) ->
    case rfc3339:parse(Timestamp) of
        {ok, {Date, Time, _, undefined}} ->
            {Date, Time};
        InvalidReturn ->
            erlang:throw({internal, {machine_failed, {bad_timestamp, Timestamp, InvalidReturn}}})
    end.

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
map_error(throw, {workers, {loading, Error={db, _}}}) ->
    map_error(throw, Error);
map_error(throw, {db, not_found}) ->
    #'MachineNotFound'{};
map_error(throw, {db, already_exist}) ->
    #'MachineAlreadyExists'{};
map_error(throw, {db, _}) ->
    % TODO
    exit(todo);
map_error(throw, {processor, _}) ->
    #'MachineFailed'{};
map_error(throw, {internal, {machine_failed, _}}) ->
    #'MachineFailed'{}.

-spec log_error({_, _, _}) ->
    _.
log_error({Class, Reason, Stacktrace}) ->
    ok = error_logger:error_msg("machine error ~p:~p ~p", [Class, Reason, Stacktrace]).

-spec reraise(_) ->
    _.
reraise({ok, R}) ->
    R;
reraise({throw, E}) ->
    erlang:throw(E);
reraise({error, {Class, Reason, Stacktrace}}) ->
    erlang:raise(Class, Reason, Stacktrace);
reraise({error, Reason}) ->
    erlang:error(Reason).
