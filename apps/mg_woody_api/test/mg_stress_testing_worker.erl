%%% Behaviour воркера, отвечающего за сессию стресс теста.
%%%
%%% start_session - что происходит при старте воркера
%%% do_action - описывает, что воркер должен делать
%%% finish_session - окончание сессии

-module(mg_stress_testing_worker).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([child_spec/1]).
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

-spec child_spec(options()) ->
    supervisor:child_spec().
child_spec(Options) ->
    #{
        id => 1,
        start => {?MODULE, start_link, [Options]}
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

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
init(Options) ->
    S = #{
        options => Options,
        state   => undefined
    },
    gen_server:cast(self(), init),
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
handle_info(timeout, S=#{action_delay:=ActionDelay}) ->
    S1 = mg_utils:apply_mod_opts(
        do_action,
        mg_stress_testing_worker_impl,
        [S]
    ),
    {noreply, S1, ActionDelay}.

-spec code_change(term(), state(), term()) ->
    {ok, state()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.

