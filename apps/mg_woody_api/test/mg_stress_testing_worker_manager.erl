%%%
%%% Менеджер, управляющий воркерами, и, соответственно, нагрузкой
%%%
-module(mg_stress_testing_worker_manager).
-behaviour(gen_server).

%% API
-export([child_spec/2]).
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

-type child_opts() :: {string(), integer(), integer()}.

-export_type([options/0]).
-type options() :: #{
    name             := atom(),
    ccw              := integer(),
    wps              := integer(),
    wps              := integer(),
    session_duration := integer(),
    total_duration   := integer()
}.

%%
%% API
%%
-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildId, Options) ->
    #{
        id       => ChildId,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(options()) ->
    supervisor:startlink_ret().
start_link(Options) ->
    Name = maps:get(name, Options),
    gen_server:start_link(self_reg_name(Name), ?MODULE, Options, []).

%%
%% gen_server callbacks
%%
-type state() :: #{
    worker_sup := atom   (),
    ccw        := integer(),
    wps        := integer(),
    aps        := integer(),
    stop_date  := integer()
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
    case now_ms() < stop_date(S) of
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
start_new_client(S0) ->
    {Id, S} = get_next_id(S0),
    {ok, _} = supervisor:start_child(self_reg_name(worker_sup), [Id, child_opts(S)]),
    increment_ccw(S).

-spec child_opts(state()) ->
    child_opts().
child_opts(S) ->
    #{
        session_duration => session_duration(S), 
        action_delay     => action_delay(S)
    }.

-spec increment_ccw(state()) ->
    state().
increment_ccw(S) ->
    S#{ccw => current_ccw(S) + 1}.

-spec get_next_id(state()) ->
    {integer(), state()}.
get_next_id(S) ->
    Name = maps:get(last_id, S) + 1,
    {Name, S#{last_id => Name}}.

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

-spec action_delay(state()) ->
    integer().
action_delay(S) ->
    maps:get(action_delay, S).

-spec stop_date(state()) ->
    integer().
stop_date(S) ->
    maps:get(stop_date, S).

-spec init_state(options()) ->
    state().
init_state(Options) ->
    #{
        max_ccw          => maps:get(ccw, Options),
        ccw              => 0,
        stop_date        => calculate_stop_date(maps:get(total_duration, Options)),
        connect_tref     => undefined,
        session_duration => maps:get(session_duration, Options),
        connect_delay    => calculate_delay(maps:get(wps, Options)), 
        action_delay     => calculate_delay(maps:get(aps, Options)),
        last_id          => 1
    }.

-spec calculate_delay(integer()) ->
    integer().
calculate_delay(T) ->
    1000 div T.

-spec calculate_stop_date(integer()) ->
    integer().
calculate_stop_date(T) ->
    now_ms() + T.

-spec now_ms() ->
    integer().
now_ms() ->
    mg_utils:now_ms().

-spec is_finished(state()) ->
    boolean().
is_finished(S) ->
    (current_ccw(S) == 0) and (stop_date(S) < mg_utils:now_ms()).

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

-spec self_reg_name(atom()) ->
    mg_utils:gen_reg_name().
self_reg_name(Name) ->
    {via, gproc, {n, l, Name}}.

