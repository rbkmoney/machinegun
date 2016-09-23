%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% Реализует понятие тэгов.
%%% При падении хендлера переводит машину в error состояние.
%%%
%%% Эвенты в машине всегда идут в таком порядке, что слева самые старые.
%%%
-module(mg_machine).

%%
%% Логика работы с ошибками.
%%
%% Возможные ошибки:
%%  - ожиданные -- throw()
%%   - бизнес-логические
%%    - машина не найдена -- machine_not_found
%%    - машина уже существует -- machine_already_exist
%%    - машина находится в упавшем состоянии -- machine_failed
%%    - что-то ещё?
%%   - временные -- temporary
%%    - сервис перегружен —- overload
%%    - хранилище недоступно -- storage_unavailable
%%    - процессор недоступн -- processor_unavailable
%%  - неожиданные
%%   - что-то пошло не так -- падение с любой другой ошибкой
%%
%% Например: throw:machine_not_found, throw:{temporary, storage_unavailable}, error:badarg
%%
%% Если в процессе обработки внешнего запроса происходит ожидаемая ошибка, она мапится код ответа,
%%  если неожидаемая ошибка, то запрос падает с internal_error, ошибка пишется в лог.
%%
%% Если в процессе обработки запроса машиной происходит ожидаемая ошибка, то она прокидывается вызывающему коду,
%%  если неожидаемая, то машина переходит в error состояние и в ответ возникает ошибка machine_failed.
%%
%% Хранилище и процессор кидают либо ошибку о недоступности, либо падают.
%%

%% API
-export_type([options     /0]).
-export_type([thrown_error/0]).

-export([child_spec /2]).
-export([start_link /1]).
-export([start      /3]).
-export([repair     /3]).
-export([call       /3]).
-export([get_history/3]).

%% Internal API
-export([handle_timeout/2]).

%% supervisor
-behaviour(supervisor).
-export([init      /1]).

%% mg_worker
-behaviour(mg_worker).
-export([handle_load/2, handle_call/2, handle_unload/1]).

%%
%% API
%%
-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_storage:storage(),
    processor => mg_utils:mod_opts(),
    observer  => mg_utils:mod_opts()   % опционально
}.

-type thrown_error() :: logic_error() | {temporary, temporary_error()}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed.
-type temporary_error() :: overload | storage_unavailable | processor_unavailable.

-type throws() :: no_return().

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
    ok | throws().
start(Options, ID, Args) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, {create, Args})).

-spec repair(options(), mg:ref(), mg:args()) ->
    ok | throws().
repair(Options, Ref, Args) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ref2id(Options, Ref), {repair, Args})).

-spec call(options(), mg:ref(), mg:args()) ->
    _Resp | throws().
call(Options, Ref, Call) ->
    mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ref2id(Options, Ref), {call, Call})).

-spec get_history(options(), mg:ref(), mg:history_range() | undefined) ->
    mg:history() | throws().
get_history(Options, Ref, Range) ->
    ID = ref2id(Options, Ref),
    case mg_storage:get_machine(get_options(storage, Options), get_options(namespace, Options), ID) of
        undefined ->
            throw(machine_not_found);
        Machine ->
            get_history_by_id(Options, ID, Machine, Range)
    end.

%%
%% Internal API
%%
-spec handle_timeout(options(), _ID) ->
    ok | throws().
handle_timeout(Options, ID) ->
    ok = mg_utils:throw_if_error(mg_workers_manager:call(manager_options(Options), ID, timeout)).

%%
%% supervisor
%%
-spec init(options()) ->
    mg_utils:supervisor_ret().
init(Options) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_workers_manager:child_spec(manager, manager_options(Options)),
        mg_storage:child_spec(get_options(storage, Options), get_options(namespace, Options),
            storage, {?MODULE, handle_timeout, [Options]})
    ]}}.

%%
%% mg_worker callbacks
%%
-type state() :: #{
    id      => mg:id(),
    options => options(),
    machine => mg_storage:machine() | undefined,
    update  => mg_storage:update()
}.

-spec handle_load(_ID, module()) ->
    {ok, state()} | {error, _}.
handle_load(ID, Options) ->
    try
        State =
            #{
                id      => ID,
                options => Options,
                machine => mg_storage:get_machine(get_options(storage, Options), get_options(namespace, Options), ID),
                update  => #{}
            },
        % есть подозрения, что это не очень хорошо, но нет понимания, что именно
        {ok, transit_state(handle_load_(State))}
    catch
        throw:Reason ->
            {error, Reason};
        Class:Reason ->
            Exception = {Class, Reason, erlang:get_stacktrace()},
            ok = log_machine_error(ID, Exception),
            {error, machine_failed}
    end.

-spec handle_call(_Call, state()) ->
    {_Resp, state()}.
handle_call(Call, State) ->
    try
        {Resp, Update} = handle_call_(Call, State),
        {Resp, transit_state(Update)}
    catch
        throw:Reason ->
            {{error, Reason}, State};
        Class:Reason ->
            {
                {error, machine_failed},
                transit_state(handle_exception({Class, Reason, erlang:get_stacktrace()}, State))
            }
    end.

-spec handle_unload(state()) ->
    ok.
handle_unload(_) ->
    ok.

%%

-spec handle_exception(mg_utils:exception(), state()) ->
    state().
handle_exception(Exception, State=#{id:=ID}) ->
    ok = log_machine_error(ID, Exception),
    Update =
        #{
            status => {error, Exception}
        },
    State#{update:=Update}.

-spec transit_state(state()) ->
    state().
transit_state(State=#{machine:=Machine, update:=Update}) when erlang:map_size(Update) =:= 0; Machine =:= undefined ->
    State;
transit_state(State=#{id:=ID, options:=Options, machine:=Machine, update:=Update}) ->
    NewMachine =
        mg_storage:update_machine(get_options(storage, Options), get_options(namespace, Options), ID, Machine, Update),
    State#{machine:=NewMachine, update:=#{}}.

%%

-spec handle_load_(state()) ->
    state().
handle_load_(State=#{id:=ID, machine:=#{status:={created, Args}}}) ->
    process_signal({init, ID, Args}, State);
handle_load_(State) ->
    State.

-spec handle_call_(_Call, state()) ->
    {_Resp, state()}.
handle_call_(Call, State=#{machine:=Machine}) ->
    case {Call, Machine} of
        {{create, Args   }, undefined              } -> {ok, process_creation(Args, State)};
        {{create, _      }, #{status:=           _}} -> {{error, machine_already_exist}, State};
        {{call  , SubCall}, #{status:={working, _}}} -> process_call(SubCall, State);
        {{call  , _      }, #{status:={error  , _}}} -> {{error, machine_failed       }, State};
        {{call  , _      }, undefined              } -> {{error, machine_not_found    }, State};
        {{repair, Args   }, #{status:={error  , _}}} -> { ok, process_signal({repair, Args}, State)};
        {{repair, _      }, #{status:={working, _}}} -> { ok,                            State};
        {{repair, _      }, undefined              } -> {{error, machine_not_found    }, State};
        { timeout         , #{status:={working, _}}} -> { ok, process_signal(timeout, State)};
        { timeout         , #{status:=_           }} -> { ok,                            State};

        % если машина в статусе _created_, а ей пришел запрос,
        % то это значит, что-то пошло не так, такого быть не должно
        { _               , #{status:={created, _}}} -> exit({something_went_wrong, Call, Machine})
    end.

%%

-spec process_creation(_Args, state()) ->
    state().
process_creation(Args, State=#{id:=ID, options:=Options}) ->
    Machine = mg_storage:create_machine(get_options(storage, Options), get_options(namespace, Options), ID, Args),
    handle_load_(State#{machine:=Machine}).

-spec process_call(_Call, state()) ->
    {{ok, _Resp}, state()}.
process_call(Call, State=#{options:=Options, id:=ID, machine:=Machine}) ->
    % TODO прокидывать range снаружи
    History = get_history_by_id(Options, ID, Machine, undefined),
    {Response, EventsBodies, ComplexAction} =
        mg_processor:process_call(get_options(processor, Options), {Call, History}),
    {{ok, Response}, handle_processor_result(EventsBodies, ComplexAction, State)}.

-spec process_signal(mg:signal(), state()) ->
    state().
process_signal(Signal, State=#{options:=Options, id:=ID, machine:=Machine}) ->
    % TODO прокидывать range снаружи
    History = get_history_by_id(Options, ID, Machine, undefined),
    {EventsBodies, ComplexAction} =
        mg_processor:process_signal(get_options(processor, Options), {Signal, History}),
    handle_processor_result(EventsBodies, ComplexAction, State).

%%

-spec handle_processor_result([mg:event_body()], mg:complex_action(), state()) ->
    state().
handle_processor_result(EventsBodies, ComplexAction, State) ->
    Events = generate_events(EventsBodies, get_last_event_id(State)),
    TimerAction = maps:get(timer, ComplexAction, undefined),
    TagAction   = maps:get(tag  , ComplexAction, undefined),
    ok = notify_observer(Events, State),
    Update = #{status => {working, get_timeout_datetime(TimerAction)}},
    State#{ update := add_events_to_update(Events, add_tag_to_update(TagAction, Update))}.

-spec add_events_to_update([mg:event()], mg_storage:update()) ->
    mg_storage:update().
add_events_to_update([], Update) ->
    Update;
add_events_to_update(Events, Update) ->
    Update#{new_events => Events}.

-spec add_tag_to_update(mg:tag() | undefined, mg_storage:update()) ->
    mg_storage:update().
add_tag_to_update(undefined, Update) ->
    Update;
add_tag_to_update(Tag, Update) ->
    Update#{new_tag => Tag}.

-spec notify_observer([mg:event()], state()) ->
    ok.
notify_observer(Events, #{id:=SourceID, options:=Options}) ->
    case get_options(observer, Options) of
        undefined ->
            ok;
        Observer ->
            ok = mg_observer:handle_events(Observer, SourceID, Events)
    end.

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
get_last_event_id(#{machine:=#{events_ids:=[]}}) ->
    undefined;
get_last_event_id(#{machine:=#{events_ids:=EventsIDs}}) ->
    lists:last(EventsIDs).

-spec get_next_event_id(undefined | mg:event_id()) ->
    mg:event_id().
get_next_event_id(undefined) ->
    1;
get_next_event_id(N) ->
    N + 1.

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

-spec ref2id(options(), mg:ref()) ->
    _ID.
ref2id(Options, {tag, Tag}) ->
    case mg_storage:resolve_tag(get_options(storage, Options), get_options(namespace, Options), Tag) of
        undefined ->
            throw(machine_not_found);
        ID ->
            ID
    end;
ref2id(_, {id, ID}) ->
    ID.

-spec get_history_by_id(options(), mg:id(), mg_storage:machine(), mg:history_range() | undefined) ->
    mg:history().
get_history_by_id(Options, ID, Machine, Range) ->
    mg_storage:get_history(get_options(storage, Options), get_options(namespace, Options), ID, Machine, Range).

-spec get_options(namespace | processor | storage | observer, options()) ->
    _.
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

-spec log_machine_error(mg:id(), mg_utils:exception()) ->
    ok.
log_machine_error(ID, Exception) ->
    ok = error_logger:error_msg("[~p] machine failed ~s", [ID, mg_utils:format_exception(Exception)]).
