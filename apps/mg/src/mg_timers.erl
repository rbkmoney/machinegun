-module(mg_timers).
-behaviour(gen_server).

%% API
-export_type([timer_handler/0]).

-export([child_spec/3]).
-export([start_link/2]).
-export([set       /3]).
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
-type timer_handler() :: {module(), atom(), [_Arg]}.

-spec child_spec(atom(), atom(), timer_handler()) ->
    supervisor:child_spec().
child_spec(ChildID, Name, TimerHandler) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Name, TimerHandler]},
        restart  => permanent,
        shutdown => brutal_kill
    }.

-spec start_link(atom(), timer_handler()) ->
    mg_utils:gen_start_ret().
start_link(Name, TimerHandler) ->
    gen_server:start_link(self_reg_name(Name), ?MODULE, TimerHandler, []).

-spec set(atom(), _ID, calendar:datetime()) ->
    ok.
set(Name, ID, DateTime) ->
    gen_server:call(self_ref(Name), {set, ID, DateTime}).

-spec cancel(atom(), _ID) ->
    ok.
cancel(Name, ID) ->
    gen_server:call(self_ref(Name), {cancel, ID}).

%%
%% gen_server callbacks
%%
-type state() :: #{
    next_wakeup  => pos_integer() | undefined, % TODO может есть готовый тип?
    timers_table => ets:tid(),
    ids_table    => ets:tid(),
    handler      => timer_handler()
}.

-spec init(timer_handler()) ->
    mg_utils:gen_server_init_ret(state()).
init(TimerHandler) ->
    % если нас с кем-то слинкуют, мы не должны падать
    _ = erlang:process_flag(trap_exit, true),
    State = do_create_tables(#{next_wakeup => undefined, handler => TimerHandler}),
    {ok, State, get_timeout(State)}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({set, ID, DateTime}, _From, State) ->
    ok = do_set(ID, DateTime, State),
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

-spec do_set(_ID, calendar:datetime(), state()) ->
    ok.
do_set(ID, DateTime, State) ->
    ok = do_cancel(ID, State),
    % TODO не очень поятно зачем тут ещё раз DateTime, а память кушает
    TRef = {DateTime, erlang:make_ref()},
    true = ets:insert(ets_tid(timers, State), {TRef, ID, DateTime}),
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
handle_next_timer_timeout(State=#{handler:=TimerHandler}, CurrentDateTime, TimerHandlingStartTime) ->
    case next_timer(State) of
        undefined ->
            % нет таймеров
            schedule_next_wakeup(State, TimerHandlingStartTime - monotonic_time(), infinity);
        {_ID, TimerDateTime} when TimerDateTime > CurrentDateTime ->
            % таймеры слишком далеко в будущем
            schedule_next_wakeup(State, TimerHandlingStartTime - monotonic_time(), ?WAKEUP_INTERVAL);
        {ID, _DateTime} ->
            % есть ближайший таймер
            ok = do_cancel(ID, State),
            _ = apply_handler(ID, TimerHandler),
            handle_next_timer_timeout(State, CurrentDateTime, TimerHandlingStartTime)
    end.

-spec schedule_next_wakeup(state(), pos_integer(), pos_integer() | infinity) ->
    state().
schedule_next_wakeup(State, _TimerHandlingTime, infinity) ->
    State#{next_wakeup:=undefined};
schedule_next_wakeup(State, TimerHandlingTime, Interval) ->
    Delay = erlang:max(Interval - TimerHandlingTime, 0),
    State#{next_wakeup:=monotonic_time() + Delay}.

-spec next_timer(state()) ->
    undefined | {_ID, calendar:datetime()}.
next_timer(State) ->
    case ets:first(ets_tid(timers, State)) of
        '$end_of_table' ->
            undefined;
        TRef ->
            [{TRef, ID, DateTime}] = ets:lookup(ets_tid(timers, State), TRef),
            {ID, DateTime}
    end.

-spec get_timeout(state()) ->
    timeout().
get_timeout(#{next_wakeup := undefined}) ->
    infinity;
get_timeout(#{next_wakeup := NextWakeupTime}) ->
    erlang:max(NextWakeupTime - monotonic_time(), 0).

-spec apply_handler(_ID, timer_handler()) ->
    ok.
apply_handler(ID, MFA = {M, F, A}) ->
    catch
        erlang:spawn(
            fun() ->
                try
                    erlang:apply(M, F, A ++ [ID])
                catch Class:Reason ->
                    Exception = {Class, Reason, erlang:get_stacktrace()},
                    ok = error_logger:error_msg("unexpected error while applying handler:~n~p~n~p", [MFA, Exception])
                end
            end
        ),
    ok.

-spec ets_tid(timers | ids, state()) ->
    ets:tid().
ets_tid(timers, #{timers_table:=Tid}) -> Tid;
ets_tid(ids   , #{ids_table   :=Tid}) -> Tid.

-spec self_ref(atom()) ->
    mg_utils:gen_ref().
self_ref(Name) ->
    wrap_name(Name).

-spec self_reg_name(atom()) ->
    mg_utils:gen_reg_name().
self_reg_name(Name) ->
    {local, wrap_name(Name)}.

-spec wrap_name(atom()) ->
    atom().
wrap_name(Name) ->
    erlang:list_to_atom(?MODULE_STRING ++ "_" ++ erlang:atom_to_list(Name)).
