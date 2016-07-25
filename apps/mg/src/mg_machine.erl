%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% Умеет обрабатывать call/cast, хибернейтиться и выгружаться по таймауту.
%%% В будущем планируется реплицировать эвенты между своими инстансами на разных нодах.
%%% Не падает при получении неожиданных запросов, и пишет их в error_logger.
%%%
%%% Возможно стоит ещё:
%%%  - прокинуть ID во все запросы
%%%  - attach/detach
%%%  - init/teminate
%%%  - handle_info
%%%  - format_state
%%% ?
%%%
%%% Реализует понятие тэгов.
%%% При падении хендлера переводит машину в error состояние.
%%%
%%% Что ещё тут хочется сделать:
%%%
%%%  - подписку на эвенты
%%%  - время жизни
%%%
-module(mg_machine).
-behaviour(supervisor).
-behaviour(mg_machine_server).

%% API
-export_type([event_id/0]).
-export_type([history /0]).
-export_type([status  /0]).
-export_type([tags    /0]).
-export_type([timeout_/0]).
-export_type([ref     /0]).
-export_type([signal  /0]).
-export_type([actions /0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([init      /1]).

-export([start         /2]).
-export([start         /3]).
-export([call          /3]).
-export([repair        /3]).
-export([handle_timeout/2]).
-export([touch         /3]).

%% mg_machine_server callbacks
-export([handle_load/2, handle_call/2, handle_cast/2, handle_unload/1]).

%%
%% API
%%
-type options () :: {mg_utils:mod_opts(), mg_utils:mod_opts()}.
-type event_id() :: mg_machine_db:event_id().
-type history () :: mg_machine_db:history ().
-type status  () :: mg_machine_db:status  ().
-type tags    () :: mg_machine_db:tags    ().
-type timeout_() :: {absolute, calendar:datetime()} | {relative, Seconds::non_neg_integer()} | undefined.
-type ref     () :: {id, _ID} | {tag, _Tag}.
-type signal  () :: timeout | {init, _Args} | {repair, _Args}.
-type actions () ::
    #{
        timeout => timeout_(),
        tag     => _Tag
    }.

%%
%% behaviour
%%
-callback process_signal(signal(), history()) ->
    {_Event, actions()}.
-callback process_call(_Call, history()) ->
    {_Response, _Event, actions()}.

%%

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor,
        shutdown => brutal_kill
    }.


-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

%%

-spec init(options()) ->
    mg_utils:supervisor_ret().
init(Options) ->
    {DBMod, DBOpts} = mg_utils:separate_mod_opts(get_mod(db, Options)),
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_machine_manager:child_spec(manager, manager_options(Options)),
        DBMod:child_spec(db, DBOpts)
    ]}}.

%%

-spec start(options(), _Args) ->
    _ID.
start(Options, Args) ->
    start(Options, Args, sync).

-spec start(options(), _Args, sync | async) ->
    _ID.
start(Options, Args, Type) ->
    % создать в бд
    % TODO перенести в сам процесс, иначе при коллизии может быть гонка (?)
    ID = call_db(create_machine, [Args], Options),
    % зафорсить загрузку
    ok = touch(Options, ID, Type),
    ID.

% sync
-spec call(options(), ref(), _Args) ->
    _Resp.
call(Options, Ref, Args) ->
    case mg_machine_manager:call(manager_options(Options), ref2id(Options, Ref), {call, Args}) of
        {error, {Class, Reason, Stacktrace}} ->
            erlang:raise(Class, Reason, Stacktrace);
        {error, Reason} ->
            erlang:error(Reason);
        {ok, R} ->
            R
    end.

% async (?)
-spec repair(options(), ref(), _Args) ->
    ok.
repair(Options, Ref, Args) ->
    ok = mg_machine_manager:cast(manager_options(Options), ref2id(Options, Ref), {repair, Args}).

% -spec get_history(mg_fsm:ref(), _From, _To) ->
%     mg_fsm:history().
% get_history(Ref, From, To) ->
%     mg_db:get_history(ref2pid(Ref), From, To).

%%
%% Internal API
%%
-spec handle_timeout(options(), _ID) ->
    ok.
handle_timeout(Options, ID) ->
    ok = mg_machine_manager:cast(manager_options(Options), ID, timeout).

-spec touch(_ID, options(), sync | async) ->
    ok.
touch(Options, ID, async) ->
    ok = mg_machine_manager:cast(manager_options(Options), ID, touch);
touch(Options, ID, sync) ->
    ok = mg_machine_manager:call(manager_options(Options), ID, touch).

%%
%% mg_machine callbacks
%%
-type state() :: #{
    id       => _,
    options => _,
    status   => status(),
    history  => history(),
    tags     => tags()
}.

-spec handle_load(_ID, module()) ->
    state().
handle_load(ID, Options) ->
    {ID, Status, History, Tags} = call_db(get_machine, [ID], Options),
    State =
        #{
            id       => ID,
            options => Options,
            status   => Status,
            history  => History,
            tags     => Tags
        },
    transit_state(State, handle_load_(State)).

-spec handle_call(_Call, state()) ->
    {_Resp, state()}.
handle_call(Call, State) ->
    {Resp, NewState} = handle_call_(Call, State),
    {Resp, transit_state(State, NewState)}.

-spec handle_cast(_Cast, state()) ->
    state().
handle_cast(Cast, State) ->
    NewState = handle_cast_(Cast, State),
    transit_state(State, NewState).

-spec handle_unload(state()) ->
    ok.
handle_unload(_) ->
    ok.

%%

-spec transit_state(state(), state()) ->
    state().
transit_state(OldState=#{id:=OldID}, NewState=#{id:=NewID, options:=Options}) when NewID =:= OldID ->
    ok = call_db(
            update_machine,
            [state_to_machine(OldState), state_to_machine(NewState), {?MODULE, handle_timeout, [Options]}],
            Options
        ),
    NewState.

-spec state_to_machine(state()) ->
    mg_machine_db:machine().
state_to_machine(#{id:=ID, status:=Status, history:=History, tags:=Tags}) ->
    {ID, Status, History, Tags}.

%%

-spec handle_load_(state()) ->
    state().
handle_load_(State=#{status:={created, Args}}) ->
    process_signal({init, Args}, State);
handle_load_(State) ->
    State.

-spec handle_call_(_Call, state()) ->
    {_Replay, state()}.
handle_call_({call, Call}, State=#{status:={working, _}}) ->
    process_call(Call, State);
handle_call_(touch, State) ->
    {ok, State};
handle_call_(Call, State) ->
    ok = error_logger:error_msg("unexpected mg_machine_server call received: ~p", [Call]),
    {{error, badarg}, State}.

-spec handle_cast_(_Cast, state()) ->
    state().
handle_cast_(Cast=timeout, State=#{status:={working, _}}) ->
    process_signal(Cast, State);
handle_cast_(Cast={repair, _}, State=#{status:={error, _}}) ->
    process_signal(Cast, State);
handle_cast_(touch, State) ->
    State;
handle_cast_(Cast, State) ->
    ok = error_logger:error_msg("unexpected mg_machine_server cast received: ~p", [Cast]),
    State.

%%

-spec process_call(_Call, state()) ->
    {_Resp, state()}.
process_call(Call, State=#{options:=Options, history:=History}) ->
    try
        {Resp, Event, Actions} = call_processing(Options, process_call, [Call, History]),
        {{ok, Resp}, handle_processing_result(Event, Actions, State)}
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        {{error, Exception}, handle_processing_error({error, Exception}, State)}
    end.

-spec process_signal(signal(), state()) ->
    state().
process_signal(Signal, State=#{options:=Options, history:=History}) ->
    try
        {Event, Actions} = call_processing(Options, process_signal, [Signal, History]),
        handle_processing_result(Event, Actions, State)
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        handle_processing_error({error, Exception}, State)
    end.

%%

-spec handle_processing_result(_Event, actions(), state()) ->
    state().
handle_processing_result(Event, Actions, State) ->
    do_actions(Actions, append_event_to_history(Event, State)).

-spec handle_processing_error(_Reason, state()) ->
    state().
handle_processing_error(Reason, State) ->
    set_status({error, Reason}, State).

-spec append_event_to_history(_Event, state()) ->
    state().
append_event_to_history(Event, State=#{history:=History}) ->
    State#{history:=maps:put(make_event_id(History), Event, History)}.

-spec make_event_id(history()) ->
    event_id().
make_event_id(History) when erlang:map_size(History) =:= 0 ->
    1;
make_event_id(History) ->
    lists:max(maps:keys(History)) + 1.

-spec do_actions(actions(), state()) ->
    state().
do_actions(Actions, State) ->
    Timeout = maps:get(timeout, Actions, undefined),
    Tag     = maps:get(tag    , Actions, undefined),
    set_tag(Tag, set_status({working, get_timeout_datetime(Timeout)}, State)).

%%
%% utils
%%
-spec manager_options(options()) ->
    _TODO.
manager_options(Options) ->
    {?MODULE, Options}.

-spec set_tag(undefined | _Tag, state()) ->
    state().
set_tag(undefined, State) ->
    State;
set_tag(Tag, State=#{tags:=Tags}) ->
    % TODO детектор коллизий тэгов
    State#{tags:=[Tag|Tags]}.

-spec set_status(status(), state()) ->
    state().
set_status(NewStatus, State) ->
    State#{status:=NewStatus}.

-spec ref2id(options(), ref()) ->
    _ID.
ref2id(Options, {tag, Tag}) ->
    call_db(resolve_tag, [Tag], Options);
ref2id(_, {id, ID}) ->
    ID.

-spec call_processing(options(), atom(), list(_Arg)) ->
    _Result.
call_processing(Options, Function, Args) ->
    {Mod, _Args} = mg_utils:separate_mod_opts(get_mod(machine, Options)),
    erlang:apply(Mod, Function, Args).

-spec call_db(atom(), list(_Arg), options()) ->
    _Result.
call_db(FunName, Args, Options) ->
    mg_utils:apply_mod_opts(get_mod(db, Options), FunName, Args).

-spec get_mod(machine | db, options()) ->
    mg_utils:mod_opts().
get_mod(machine, {ModOpts, _}) -> ModOpts;
get_mod(db     , {_, ModOpts}) -> ModOpts.

-spec get_timeout_datetime(timeout_()) ->
    calendar:datetime() | undefined.
get_timeout_datetime(undefined) ->
    undefined;
get_timeout_datetime({absolute, DateTime}) ->
    DateTime;
get_timeout_datetime({relative, Period}) ->
    calendar:gregorian_seconds_to_datetime(
        calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + Period
    ).
