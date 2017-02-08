%%%
%%% Процесс (сейчас gen_server), который переодически (раз в interval секунд) запускает кастомный функтор.
%%% Может это и велосипед, но очень простой и точно под наши нужды.
%%% Тащить ради этого лишнюю зависимость смысла не вижу.
%%%
-module(mg_cron).

%% API
-export_type([options/0]).

-export([child_spec/2]).
-export([start_link/1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type options() :: #{
    interval => pos_integer(), % ms
    job      => {module(), atom(), list()} % fun(() -> _)
}.

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => brutal_kill
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).


%%
%% gen_server callbacks
%%
-type state() :: #{
    options       => options     (),
    next_job_date => timestamp_ms()
}.
-type timestamp_ms() :: pos_integer().

-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    State = #{
        options       => Options,
        next_job_date => now_ms()
    },
    {ok, State, schedule_timer(State)}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State, schedule_timer(State)}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State, schedule_timer(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = do_job(State),
    {noreply, NewState, schedule_timer(NewState)};
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected gen_server info ~p", [Info]),
    {noreply, State, schedule_timer(State)}.

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
-spec do_job(state()) ->
    state().
do_job(State=#{options := Options, next_job_date := CurrentWorkDate}) ->
    ok = apply_job_functor(Options),
    State#{next_job_date := next_job_date(Options, CurrentWorkDate)}.

-spec next_job_date(options(), timestamp_ms()) ->
    timestamp_ms().
next_job_date(#{interval := Interval}, CurrentWorkDate) ->
    NextWorkDate = CurrentWorkDate + Interval,
    Now = now_ms(),
    % если мы уже опаздываем
    % может писать тут варнинг, что всё тормозит?
    case NextWorkDate < Now of
        true  -> Now;
        false -> NextWorkDate
    end.

-spec apply_job_functor(options()) ->
    ok.
apply_job_functor(#{job := {M, F, A}=MFA}) ->
    try
        _ = erlang:apply(M, F, A),
        ok
    catch Class:Reason ->
        Exception = {Class, Reason, erlang:get_stacktrace()},
        ok = error_logger:error_msg("unexpected error while applying handler:~n~p~n~p", [MFA, Exception])
    end.

-spec schedule_timer(state()) ->
    timeout().
schedule_timer(#{next_job_date := NextWorkDate}) ->
    Delay = NextWorkDate - now_ms(),
    case Delay < 0 of
        true  -> 0;
        false -> Delay
    end.

-spec now_ms() ->
    timestamp_ms().
now_ms() ->
    erlang:system_time(1000).
