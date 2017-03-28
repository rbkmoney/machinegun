%%%
%%% Менеджер, управляющий воркерами, и, соответственно, нагрузкой
%%%
-module(mg_stress_testing_worker_manager).
-behaviour(gen_server).

%% API
-export([child_spec/1]).
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

-type child_opts() :: {string(), integer(), integer()}.

-export_type([options/0]).
-type options() :: #{
    name             => atom   (),
    ccw              => integer(),
    wps              => integer(),
    wps              => integer(),
    session_duration => integer(),
    total_duration   => integer()
}.

%%
%% API
%%
-spec child_spec(options()) ->
    supervisor:child_spec().
child_spec(Options) ->
    #{
        id       => maps:get(name, Options),
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options()) ->
    supervisor:startlink_ret().
start_link(Options) ->
    Name = maps:get(name, Options),
    io:format("~p\n", [Name]),
    gen_server:start_link(self_ref(Name), ?MODULE, Options, []).

%%
%% gen_server callbacks
%%
-type state() :: #{
    worker_sup => atom   (),
    ccw        => integer(),
    wps        => integer(),
    aps        => integer(),
    stop_date  => integer()
}.

-spec init(options()) ->
    {ok, state()}.
init(Options) ->
    S = init_state(Options),
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
    {noreply, state()}.
handle_info(init, S) ->
    {noreply, schedule_connect_timer(S)};
handle_info({timeout, TRef, start_client}, S=#{connect_tref:=ConnectTRef})
    when (TRef == ConnectTRef)
    ->
    case stop_date(S) < now() of
        true ->
            case current_ccw(S) < max_ccw(S) of
                true ->
                    S1 = start_new_client(S),
                    {noreply, schedule_connect_timer(S1)};
                false ->
                    {noreply, schedule_connect_timer(S)}
            end;
        false ->
            case is_finished(S) of
                true  -> {stop, normal, S};
                false -> {noreply, S}
            end
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
-spec start_new_client(state()) ->
    state().
start_new_client(S) ->
    {ok, _} = supervisor:start_child(self_ref(worker_sup), child_opts(S)),
    S.

-spec child_opts(state()) ->
    child_opts().
child_opts(S0) ->
    {Name, S} = get_next_name(S0),
    [Name, session_duration(S), request_delay(S)].

-spec get_next_name(state()) ->
    {pos_integer(), state()}.
get_next_name(S) ->
    Name = maps:get(last_name, S) + 1,
    {Name, S#{last_name => Name}}.

-spec current_ccw(state()) ->
    integer().
current_ccw(S) ->
    maps:get(ccw, S).

-spec max_ccw(state()) ->
    integer().
max_ccw(S) ->
    maps:get(max_ccw, S).

-spec session_duration(state()) ->
    integer().
session_duration(S) ->
    maps:get(session_duration, S).

-spec request_delay(state()) ->
    integer().
request_delay(S) ->
    maps:get(request_delay, S).

-spec stop_date(state()) ->
    date().
stop_date(S) ->
    maps:get(stop_date, S).

-spec init_state(options()) ->
    state().
init_state(Options) ->
    #{
        max_ccw          => maps:get(ccw, Options),
        ccw              => 0,
        wps              => 0,
        aps              => 0,
        stop_date        => 0,
        connect_tref     => undefined,
        session_duration => maps:get(session_duration, Options),
        connect_delay    => 1000,
        request_delay    => 1000,
        last_name        => 1
    }.

-spec is_finished(state()) ->
    boolean().
is_finished(S) ->
    (current_ccw(S) == 0) and (stop_date() < now()).

-spec schedule_connect_timer(state()) ->
    state().
schedule_connect_timer(S0) ->
    S = cancel_connect_timer(S0),
    TRef = erlang:start_timer(maps:get(connect_delay, S), self(), start_client),
    S#{connect_tref => TRef}.

-spec cancel_connect_timer(state()) ->
    state().
cancel_connect_timer(S = #{connect_tref:=undefined}) ->
    S;
cancel_connect_timer(S = #{connect_tref:=TRef}) ->
    erlang:cancel_timer(TRef),
    S#{connect_tref => undefined}.

-spec self_ref(atom()) ->
    mg_utils:gen_reg_name().
self_ref(Name) ->
    {via, gproc, {n, l, Name}}.
