%%%
%%% Менеджер, управляющий воркерами, и, соответственно, нагрузкой
%%%
-module(mg_stress_testing_worker_manager).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([child_spec/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-export_type([options/0]).
-type options() :: #{
    name             => atom   (),
    ccu              => integer(),
    session_duration => integer(),
    total_duration   => integer(),
    cps              => integer(),
    rps              => integer()
}.

-spec start_link(options()) ->
    startlink_ret().
start_link(Options) ->
    Name = maps:get(name, Options),
    gen_server:start_link({via, gproc, Name}, Options, []).

-spec child_spec(atom(), options) ->
    supervisor:child_spec().
child_spec(ChildId, Options) ->
    #{
        id       => ChildId,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill
    }.

%%
%% gen_server callbacks
%%
-type state() :: #{
    workers_sup      => pid    (),
    ccu              => integer(),
    rps              => integer(),
    cps              => integer(),
    session_duration => integer(),
    stop_date        => integer()
}.

-spec init(options()) ->
    {ok, state()}.
init(_Options) ->
    S = #{},
    gen_server:cast(self(), init),
    {ok, S}.

-spec handle_call(term(), _, state()) ->
    {noreply, state()}.
handle_call(Call, _, S) ->
    exit({'unknown call', Call}),
    {noreply, S}.

-spec handle_cast(term(), state()) ->
    {noreply, state()}.
handle_cast(init, S) ->
    % Готовимся и вычисляем нужные значения для последующего старта воркеров
    % Находим в gproc супервизор воркеров
    % Заходим в луп старта этих самых воркеров
    {noreply, S}.

-spec handle_info(loop, state()) ->
    {noreply, state()} | {noreply, state(), integer()}.
handle_info(loop, S) ->
    % Стартуем воркеров
    {noreply, S}.

-spec code_change(term(), state(), term()) ->
    {ok, state()}.
code_change(_, S, _) ->
    {ok, S}.

-spec terminate(term(), term()) ->
    ok.
terminate(_, _) ->
    ok.

