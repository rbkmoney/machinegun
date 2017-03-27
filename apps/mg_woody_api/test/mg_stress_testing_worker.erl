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
-type options() :: term().

-spec child_spec() ->
    supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []}
    }.

-spec start_link(_,_,options()) ->
    mg_utils:gen_start_ret().
start_link(Name, SessionDuration, ActionDelay) ->
    gen_server:start_link(?MODULE, [], [{Name, SessionDuration, ActionDelay}]).

%%
%% gen_server callbacks
%%
-type state() :: #{
    options      => term(),
    action_delay => term(),
    session_duration => term()
}.

-spec init(options()) ->
    {ok, state()}.
init({_Name, SessionDuration, ActionDelay}) ->
    S = #{
        action_delay => ActionDelay,
        session_duration => SessionDuration
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
handle_info(timeout, S=#{action_delay:=ActionDelay}) ->
    _S1 = mg_utils:apply_mod_opts(
        mg_stress_testing_worker_impl,
        do_action,
        []
    ),
    {noreply, S, ActionDelay}.

-spec code_change(term(), state(), term()) ->
    {ok, state()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.
