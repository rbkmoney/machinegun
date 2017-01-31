-module(mg_stress_testing_worker_manager).
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-spec start_link(mg_stress_testing:manager_options()) ->
    startlink_ret().
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%%
%% gen_server callbacks
%%
-type state() :: #{
    workers_sup => pid(),
    ccu => integer(),
    rps => integer(),
    cps => integer(),
    session_duration => integer(),
    stop_date => integer()
}.

-spec init(mg_stress_testing:worker_options()) ->
    {ok, term()}.
init(_Options) ->
    S = #{},
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
    % Готовимся и вычисляем нужные значения для последующего старта воркеров
    % Находим в gproc супервизор воркеров
    {noreply, S}.

-spec handle_info(loop, state()) ->
    {noreply, state()} | {noreply, state(), integer()}.
handle_info(loop, S) ->
    % Стартуем воркеров
    {noreply, S}.

-spec code_change(term(), state(), term()) ->
    {ok, term()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.

