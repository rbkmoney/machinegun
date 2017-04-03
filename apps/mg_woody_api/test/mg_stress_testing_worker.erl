%%% Behaviour воркера, отвечающего за сессию стресс теста.
%%%
%%% start_session - что происходит при старте воркера
%%% do_action - описывает, что воркер должен делать
%%% finish_session - окончание сессии

-module(mg_stress_testing_worker).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([child_spec/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% Callbacks
%%
-callback start(state()) ->
    state().

-callback action(state()) ->
    state().

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    action_delay     := integer(),
    session_duration := integer()
}.

-type state() :: #{
    local_state  := term(),
    action_delay := integer(),
    finish_time  := integer()
}.

-export_type([worker/0]).
-type worker() :: mg_utils:mod_opts().

-spec child_spec(atom(), worker()) ->
    supervisor:child_spec().
child_spec(ChildId, Worker) ->
    #{
        id      => ChildId,
        start   => {?MODULE, start_link, [Worker]},
        restart => temporary
    }.

-spec start_link(worker(), atom(), options()) ->
    mg_utils:gen_start_ret().
start_link(Worker, _Name, Options) ->
    gen_server:start_link(?MODULE, {Worker, Options}, []).

%%
%% gen_server callbacks
%%
-spec init({worker(), options()}) ->
    {ok, state()}.
init({Worker, Options}) ->
    ActionDelay     = maps:get(action_delay, Options),
    SessionDuration = maps:get(session_duration, Options),

    S = #{
        worker       => Worker,
        action_delay => ActionDelay,
        finish_time  => calculate_finish_time(SessionDuration),
        local_state  => undefined
    },

    self() ! init,
    {ok, S}.

-spec handle_call(term(), _, state()) ->
    {noreply, state()}.
handle_call(Call, _, S) ->
    exit({'unknown call', Call}),
    {noreply, S}.

-spec handle_cast(term(), state()) ->
    {noreply, state()}.
handle_cast(Cast, S) ->
    exit({'unknown cast', Cast}),
    {noreply, S}.

-spec handle_info(term(), state()) ->
    {stop, normal, state()} | {noreply, state(), integer()}.
handle_info(init, S0) ->
    S = call_worker(start, S0),
    {noreply, S, 1000};
handle_info(timeout, S) ->
    case is_finished(S) of
        false ->
            S1 = call_worker(action, S),
            {noreply, S1, action_delay(S1)};
        true ->
            {stop, normal, S}
    end.

-spec code_change(term(), state(), term()) ->
    {ok, state()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.


%%
%% Implementation calling
%%
-spec call_worker(atom(), state()) ->
    state().
call_worker(Fun, S) ->
    {Mod, _} = worker(S),
    Mod:Fun(S).

%%
%% Utils
%%
-spec worker(state()) ->
    worker().
worker(S) ->
    maps:get(worker, S).

-spec finish_time(state()) ->
    integer().
finish_time(S) ->
    maps:get(finish_time, S).

-spec action_delay(state()) ->
    integer().
action_delay(S) ->
    maps:get(action_delay, S).

-spec calculate_finish_time(integer()) ->
    pos_integer().
calculate_finish_time(SessionDuration) ->
    SessionDuration + mg_utils:now_ms().

-spec is_finished(state()) ->
    boolean().
is_finished(S) ->
    finish_time(S) < mg_utils:now_ms().

