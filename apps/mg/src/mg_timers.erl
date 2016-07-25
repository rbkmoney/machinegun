-module(mg_timers).
-behaviour(gen_server).

%% API
-export([child_spec/2]).
-export([start_link/1]).
-export([set       /4]).
-export([cancel    /2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

% Интервал проверки наличия истекших таймеров
% должен быть меньше 16#0800000, см. timer.erl
% Если обработка сработавших таймеров заняла больше времени, то интервал увеличивается.
-define(WAKEUP_INTERVAL, 1000).

%%
%% API
%%
-spec child_spec(atom(), _Name) ->
    supervisor:child_spec().
child_spec(ChildID, Name) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Name]},
        restart  => permanent,
        shutdown => brutal_kill
    }.

-spec start_link(_Name) ->
    mg_utils:gen_start_ret().
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, undefined, []).

-spec set(_Name, _ID, calendar:datetime(), _MFA) ->
    ok.
set(Name, ID, DateTime, MFA) ->
    gen_server:call(Name, {set, ID, DateTime, MFA}).

-spec cancel(_Name, _ID) ->
    ok.
cancel(Name, ID) ->
    gen_server:call(Name, {cancel, ID}).

%%
%% gen_server callbacks
%%
-type state() :: #{
    next_wakeup  => pos_integer() | undefined, % TODO может есть готовый тип?
    timers_table => ets:tid(),
    ids_table    => ets:tid()
}.

-spec init(undefined) ->
    mg_utils:gen_server_init_ret(state()).
init(undefined) ->
    % если нас с кем-то слинкуют, мы не должны падать
    _ = erlang:process_flag(trap_exit, true),
    State = do_create_tables(#{next_wakeup => undefined}),
    {ok, State, get_timeout(State)}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({set, ID, DateTime, MFA}, _From, State) ->
    ok = do_set(ID, DateTime, MFA, State),
    NewState = handle_timeout(State),
    gen_server_result({reply, ok}, NewState);
handle_call({cancel, ID}, _From, State) ->
    ok = do_cancel(ID, State),
    gen_server_result({reply, ok}, State);
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected call received: ~p from ~p", [Call, From]),
    gen_server_result(noreply, State).

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected cast received: ~p", [Cast]),
    gen_server_result(noreply, State).

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = handle_timeout(State),
    gen_server_result(noreply, NewState);
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected info ~p", [Info]),
    gen_server_result(noreply, State).

-spec code_change(_, state(), _) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% local
%%
-spec gen_server_result(noreply | {reply, _Resp}, state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
gen_server_result(noreply, State) ->
    {noreply, State, get_timeout(State)};
gen_server_result({reply, Resp}, State) ->
    {reply, Resp, State, get_timeout(State)}.

-spec do_create_tables(state()) ->
    state().
do_create_tables(State) ->
    State#{
        timers_table => ets:new(timers_table, [ordered_set, protected]),
        ids_table    => ets:new(ids_table   , [set        , protected])
    }.

-spec do_set(_ID, calendar:datetime(), _MFA, state()) ->
    ok.
do_set(ID, DateTime, MFA, State) ->
    ok = do_cancel(ID, State),
    TRef = {DateTime, erlang:make_ref()},
    true = ets:insert(ets_tid(timers, State), {TRef, ID, DateTime, MFA}),
    true = ets:insert(ets_tid(ids   , State), {ID, TRef}),
    ok.

-spec do_cancel(_ID, state()) ->
    ok.
do_cancel(ID, State) ->
    case ets:lookup(ets_tid(ids, State), ID) of
        [{ID, TRef}] ->
            _ = ets:delete(ets_tid(timers, State), TRef),
            _ = ets:delete(ets_tid(ids   , State), ID  ),
            ok;
        [] ->
            ok
    end.

-spec current_datetime() ->
    calendar:datetime().
current_datetime() ->
    calendar:universal_time().

-spec monotonic_time() ->
    pos_integer().
monotonic_time() ->
    erlang:monotonic_time(1000). % ms

-spec handle_timeout(state()) ->
    state().
handle_timeout(State) ->
    handle_next_timer_timeout(State, current_datetime(), monotonic_time()).

-spec handle_next_timer_timeout(state(), calendar:datetime(), pos_integer()) ->
    state().
handle_next_timer_timeout(State, CurrentDateTime, TimerHandlingStartTime) ->
    case ets:first(ets_tid(timers, State)) of
    '$end_of_table' ->
        schedule_next_wakeup(State, TimerHandlingStartTime - monotonic_time(), infinity);
    {TimerDateTime, _} when TimerDateTime > CurrentDateTime ->
        schedule_next_wakeup(State, TimerHandlingStartTime - monotonic_time(), ?WAKEUP_INTERVAL);
    TRef ->
        [{TRef, ID, _, MFA}] = ets:lookup(ets_tid(timers, State), TRef),
        ok = do_cancel(ID, State),
        _ = apply_handler(ID, MFA),
        handle_next_timer_timeout(State, CurrentDateTime, TimerHandlingStartTime)
    end.

-spec schedule_next_wakeup(state(), pos_integer(), pos_integer() | infinity) ->
    state().
schedule_next_wakeup(State, _TimerHandlingTime, infinity) ->
    State#{next_wakeup:=undefined};
schedule_next_wakeup(State, TimerHandlingTime, Interval) ->
    Delay = erlang:max(Interval - TimerHandlingTime, 0),
    State#{next_wakeup:=monotonic_time() + Delay}.

-spec get_timeout(state()) ->
    timeout().
get_timeout(#{next_wakeup := undefined}) ->
    infinity;
get_timeout(#{next_wakeup := NextWakeupTime}) ->
    erlang:max(NextWakeupTime - monotonic_time(), 0).

-spec apply_handler(_ID, {module(), atom(), [_Arg]}) ->
    _Result.
apply_handler(ID, MFA = {M, F, A}) ->
    try
        erlang:apply(M, F, A ++ [ID])
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        ok = error_logger:error_msg("unexpected error while applying handler:~n~p~n~p", [MFA, Exception])
    end.

-spec ets_tid(timers | ids, state()) ->
    ets:tid().
ets_tid(timers, #{timers_table:=Tid}) -> Tid;
ets_tid(ids   , #{ids_table   :=Tid}) -> Tid.
