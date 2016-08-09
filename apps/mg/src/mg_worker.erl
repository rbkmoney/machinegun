-module(mg_worker).
-behaviour(gen_server).

%% API
-export([child_spec /2]).
-export([start_link /2]).
-export([call       /2]).
-export([cast       /2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-callback handle_load(_ID, _Args) ->
    _State.

-callback handle_unload(_State) ->
    ok.

-callback handle_call(_Call, _State) ->
    {_Replay, _State}.

-callback handle_cast(_Cast, _State) ->
    _State.

-type options() :: mg_utils:mod_opts().

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options(), _ID) ->
    mg_utils:gen_start_ret().
start_link(Options, ID) ->
    gen_server:start_link(self_reg_name(ID), ?MODULE, {ID, Options}, []).

-spec call(_ID, _Call) ->
    _Result.
call(ID, Call) ->
    gen_server:call(self_ref(ID), {call, Call}).

-spec cast(_ID, _Cast) ->
    ok.
cast(ID, Cast) ->
    gen_server:cast(self_ref(ID), {cast, Cast}).


%%
%% gen_server callbacks
%%
-type state() ::
    #{
        id                => _ID,
        mod               => module(),
        state             => loading | {working, _State},
        unload_tref       => reference() | undefined,
        hibernate_timeout => timeout(),
        unload_timeout    => timeout()
    }.

-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init({ID, Options}) ->
    ok = gen_server:cast(erlang:self(), load),
    {Mod, Args} = mg_utils:separate_mod_opts(Options),
    State =
        #{
            id                => ID,
            mod               => Mod,
            state             => {loading, Args},
            unload_tref       => undefined,
            % hibernate_timeout => 5000,
            % unload_timeout    => 5 * 60 * 1000
            hibernate_timeout => 1,
            unload_timeout    => 1000
        },
    {ok, State}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({call, Call}, _, State=#{mod:=Mod, state:={working, ModState}}) ->
    {Reply, NewModState} = Mod:handle_call(Call, ModState),
    NewState = State#{state:={working, NewModState}},
    {reply, Reply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(load, State=#{id:=ID, mod:=Mod, state:={loading, Args}}) ->
    NewState = State#{state:={working, Mod:handle_load(ID, Args)}},
    {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
handle_cast({cast, Cast}, State=#{mod:=Mod, state:={working, ModState}}) ->
    NewState = State#{state:={working, Mod:handle_cast(Cast, ModState)}},
    {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info({timeout, unload}, State=#{mod:=Mod, state:={working, ModState}}) ->
    _ = Mod:handle_unload(ModState),
    {stop, normal, State};
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected gen_server info ~p", [Info]),
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)}.

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
-spec hibernate_timeout(state()) ->
    timeout().
hibernate_timeout(#{hibernate_timeout:=Timeout}) ->
    Timeout.

-spec unload_timeout(state()) ->
    timeout().
unload_timeout(#{unload_timeout:=Timeout}) ->
    Timeout.

-spec schedule_unload_timer(state()) ->
    state().
schedule_unload_timer(State=#{unload_tref:=UnloadTRef}) ->
    _ = case UnloadTRef of
            undefined -> ok;
            TRef      -> erlang:cancel_timer(TRef)
        end,
    State#{unload_tref:=start_timer(State)}.

-spec start_timer(state()) ->
    reference().
start_timer(State) ->
    erlang:start_timer(unload_timeout(State), erlang:self(), unload).

-spec self_ref(_ID) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec self_reg_name(_ID) ->
    mg_utils:gen_reg_name().
self_reg_name(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec wrap_id(_ID) ->
    term().
wrap_id(ID) ->
    {?MODULE, ID}.
