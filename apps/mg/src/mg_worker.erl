-module(mg_worker).

%% API
-export_type([options     /0]).
-export_type([call_context/0]).

-export([child_spec /2]).
-export([start_link /3]).
-export([call       /4]).
-export([brutal_kill/2]).
-export([reply      /2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-callback handle_load(_ID, _Args) ->
    {ok, _State} | {error, _Error}.

-callback handle_unload(_State) ->
    ok.

-callback handle_call(_Call, call_context(), _State) ->
    {{reply, _Reply} | noreply, _State}.


-type options() :: #{
    worker            => mg_utils:mod_opts(),
    hibernate_timeout => pos_integer(),
    unload_timeout    => pos_integer()
}.
-type call_context() :: _. % в OTP он не описан :(

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options(), _NS, _ID) ->
    mg_utils:gen_start_ret().
start_link(Options, NS, ID) ->
    gen_server:start_link(self_reg_name({NS, ID}), ?MODULE, {ID, Options}, []).

-spec call(_NS, _ID, _Call, pos_integer()) ->
    _Result | {error, _}.
call(NS, ID, Call, MaxQueueLength) ->
    case mg_utils:get_msg_queue_len(self_reg_name({NS, ID})) < MaxQueueLength of
        true ->
            gen_server:call(self_ref({NS, ID}), {call, Call});
        false ->
            {error, {transient, overload}}
    end.

%% for testing
-spec brutal_kill(_NS, _ID) ->
    ok.
brutal_kill(NS, ID) ->
    true = erlang:exit(mg_utils:gen_where(self_ref({NS, ID})), kill),
    ok.

%% Internal API
-spec reply(call_context(), _Reply) ->
    _.
reply(CallCtx, Reply) ->
    _ = gen_server:reply(CallCtx, Reply),
    ok.


%%
%% gen_server callbacks
%%
-type state() ::
    #{
        id                => _ID,
        mod               => module(),
        status            => {loading, _Args} | {working, _State},
        unload_tref       => reference() | undefined,
        hibernate_timeout => timeout(),
        unload_timeout    => timeout()
    }.

-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init({ID, Options = #{worker := WorkerModOpts}}) ->
    HibernateTimeout = maps:get(hibernate_timeout, Options,      5 * 1000),
    UnloadTimeout    = maps:get(unload_timeout   , Options, 5 * 60 * 1000),
    {Mod, Args} = mg_utils:separate_mod_opts(WorkerModOpts),
    State =
        #{
            id                => ID,
            mod               => Mod,
            status            => {loading, Args},
            unload_tref       => undefined,
            hibernate_timeout => HibernateTimeout,
            unload_timeout    => UnloadTimeout
        },
    {ok, State}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).

% загрузка делается отдельно и лениво, чтобы не блокировать этим супервизор,
% т.к. у него легко может начать расти очередь
handle_call(Call={call, _}, From, State=#{id:=ID, mod:=Mod, status:={loading, Args}}) ->
    case Mod:handle_load(ID, Args) of
        {ok, ModState} ->
            handle_call(Call, From, State#{status:={working, ModState}});
        Error={error, _} ->
            {stop, normal, Error, State}
    end;
handle_call({call, Call}, From, State=#{mod:=Mod, status:={working, ModState}}) ->
    {ReplyAction, NewModState} = Mod:handle_call(Call, From, ModState),
    NewState = State#{status:={working, NewModState}},
    case ReplyAction of
        {reply, Reply} -> {reply, Reply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
        noreply        -> {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)}
    end;
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info({timeout, TRef, unload}, State=#{mod:=Mod, unload_tref:=TRef, status:=Status}) ->
    case Status of
        {working, ModState} ->
            _ = Mod:handle_unload(ModState);
        {loading, _} ->
            ok
    end,
    {stop, normal, State};
handle_info({timeout, _, unload}, State=#{}) ->
    % А кто-то опаздал!
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)};
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
