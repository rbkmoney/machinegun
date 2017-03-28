%%% Behaviour воркера, отвечающего за сессию стресс теста.
%%%
%%% start_session - что происходит при старте воркера
%%% do_action - описывает, что воркер должен делать
%%% finish_session - окончание сессии

-module(mg_stress_testing_worker).
-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([child_spec/0]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% Callbacks
%%
-callback do_action(state()) ->
    state().

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    action_delay     := pos_integer(),
    session_duration := pos_integer()
}.

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildId, #{action_delay:=ActionDelay, session_duration:=SessionDuration}) ->
    #{
        id    => ChildId,
        start => {?MODULE, start_link, [ActionDelay, SessionDuration]}
    }.

-spec start_link(atom(), options()) ->
    mg_utils:gen_start_ret().
start_link(Name, ActionDelay, SessionDuration) ->
    gen_server:start_link(Name, ?MODULE, [], [ActionDelay, SessionDuration]).

%%
%% gen_server callbacks
%%
-type state() :: #{
    action_delay := term(),
    finish_time  := term()
}.

-spec init(options()) ->
    {ok, state()}.
init(ActionDelay, SessionDuration) ->
    S = #{
        action_delay => ActionDelay,
        finish_time  => calculate_finish_time(SessionDuration)
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
    {noreply, state()} | {noreply, state(), integer()}.
handle_info(init, S) ->
    {noreply, S, 1000};
handle_info(timeout, S) ->
    case is_finished(S) of
        false ->
            S1 = mg_utils:apply_mod_opts(
                mg_stress_testing_worker_impl,
                do_action,
                [S]
            ),
            {noreply, S1, ActionDelay};
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
%% Utils
%%
-spec finish_time(state()) ->
    pos_integer().
finish_time(S) ->
    maps:get(finish_time, S).

-spec calculate_finish_time(pos_integer()) ->
    pos_integer().
calculate_finish_time(SessionDuration) ->
    SessionDuration + mg_utils:now_ms().

-spec is_finished(state()) ->
    boolean().
is_finished(S) ->
    finish_time(S) > mg_utils:now_ms().
