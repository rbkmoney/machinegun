%% Behaviour воркера, отвечающего за сессию стресс теста.
%%
%% start_session - что происходит при старте воркера
%% do_action - описывает, что воркер должен делать
%% finish_session - окончание сессии

-module(mg_stress_testing_worker).
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% Callbacks
%%
-callback start_session(state()) ->
    {noreply, term()} | {noreply, term(), term()}.
-callback do_action(atom(), state()) ->
    state().
-callback finish_session(state()) ->
    {stop, term(), state()}.

%%
%% API
%%
-spec start_link(mg_stress_testing:worker_options()) ->
    startlink_ret().
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%%
%% gen_server callbacks
%%
-type state() :: #{
    options => term(),
    action_delay => term(),
    state   => term()
}.

-spec init(mg_stress_testing:worker_options()) ->
    {ok, term()}.
init(Options) ->
    S = #{
        options => Options,
        state   => undefined
    },
    gen_server:cast(self(), init),
    {ok, S}.

-spec handle_call(term(), _, term()) ->
    {noreply, term()}.
handle_call(Call, _, S) ->
    exit({'unknown call', Call}),
    {noreply, S}.

-spec handle_cast(term(), term()) ->
    {noreply, term()}.
handle_cast(init, S) ->
    start_session(S);
handle_cast(Cast, S) ->
    exit({'unknown cast', Cast}),
    {noreply, S}.

-spec handle_info(term(), term()) ->
    {noreply, term()} | {noreply, term(), integer()}.
handle_info(timeout, S=#{action_delay:=ActionDelay}) ->
    case is_finished(S, utils_time:universal_time()) of
        true ->
            {stop, normal, S};
        false ->
            S1 = do_action(action, S),
            {noreply, S1, ActionDelay}
    end;
handle_info(finish_session, S) ->
    finish_session(S);
handle_info(Info, S=#{action_delay:=ActionDelay, state:=WorkerState})
    when WorkerState =/= undefined
    ->
    S1 = do_action(Info, S),
    {noreply, S1, ActionDelay}.

-spec code_change(term(), term(), term()) ->
    {ok, term()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% Utils
%%
is_finished(_S, _Time) ->
    true.

