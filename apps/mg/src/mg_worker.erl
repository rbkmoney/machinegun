%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_worker).

-include_lib("mg/include/pulse.hrl").

%% API
-export_type([options     /0]).
-export_type([call_context/0]).

-export([child_spec    /2]).
-export([start_link    /4]).
-export([call          /7]).
-export([brutal_kill   /3]).
-export([reply         /2]).
-export([get_call_queue/3]).
-export([is_alive      /3]).
-export([list_all      /1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-callback handle_load(_ID, _Args, _ReqCtx) ->
    {ok, _State} | {error, _Error}.

-callback handle_unload(_State) ->
    ok.

-callback handle_call(_Call, call_context(), _ReqCtx, mg_deadline:deadline(), _State) ->
    {{reply, _Reply} | noreply, _State}.


-type options() :: #{
    worker            => mg_utils:mod_opts(),
    registry          => mg_procreg:options(),
    hibernate_timeout => pos_integer(),
    unload_timeout    => pos_integer()
}.
-type call_context() :: _. % в OTP он не описан, а нужно бы :(

%% Internal types

-type pulse() :: mg_pulse:handler().

-define(wrap_id(NS, ID), {?MODULE, {NS, ID}}).

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => brutal_kill
    }.

-spec start_link(options(), mg:ns(), mg:id(), _ReqCtx) ->
    mg_utils:gen_start_ret().
start_link(Options, NS, ID, ReqCtx) ->
    mg_procreg:start_link(procreg_options(Options), ?wrap_id(NS, ID), ?MODULE, {ID, Options, ReqCtx}, []).

-spec call(options(), mg:ns(), mg:id(), _Call, _ReqCtx, mg_deadline:deadline(), pulse()) ->
    _Result | {error, _}.
call(Options, NS, ID, Call, ReqCtx, Deadline, Pulse) ->
    ok = mg_pulse:handle_beat(Pulse, #mg_worker_call_attempt{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }),
    mg_procreg:call(
        procreg_options(Options),
        ?wrap_id(NS, ID),
        {call, Deadline, Call, ReqCtx},
        mg_deadline:to_timeout(Deadline)
    ).

%% for testing
-spec brutal_kill(options(), mg:ns(), mg:id()) ->
    ok.
brutal_kill(Options, NS, ID) ->
    case mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)) of
        undefined ->
            ok;
        Pid ->
            true = erlang:exit(Pid, kill),
            ok
    end.

%% Internal API
-spec reply(call_context(), _Reply) ->
    _.
reply(CallCtx, Reply) ->
    _ = gen_server:reply(CallCtx, Reply),
    ok.

-spec get_call_queue(options(), mg:ns(), mg:id()) ->
    [_Call].
get_call_queue(Options, NS, ID) ->
    Pid = mg_utils:exit_if_undefined(mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)), noproc),
    {messages, Messages} = erlang:process_info(Pid, messages),
    [Call || {'$gen_call', _, {call, _, Call, _}} <- Messages].

-spec is_alive(options(), mg:ns(), mg:id()) ->
    boolean().
is_alive(Options, NS, ID) ->
    Pid = mg_utils:gen_reg_name_to_pid(self_ref(Options, NS, ID)),
    Pid =/= undefined andalso erlang:is_process_alive(Pid).

-spec list_all(mg_procreg:options()) -> % TODO nonuniform interface
    [{mg:ns(), mg:id(), pid()}].
list_all(ProcregOptions) ->
    AllWorkers = mg_procreg:all(ProcregOptions),
    [{NS, ID, Pid} || {?wrap_id(NS, ID), Pid} <- AllWorkers].


%%
%% gen_server callbacks
%%
-type state() ::
    #{
        id                => _ID,
        mod               => module(),
        status            => {loading, _Args, _ReqCtx} | {working, _State},
        unload_tref       => reference() | undefined,
        hibernate_timeout => timeout(),
        unload_timeout    => timeout()
    }.

-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init({ID, Options = #{worker := WorkerModOpts}, ReqCtx}) ->
    HibernateTimeout = maps:get(hibernate_timeout, Options,  5 * 1000),
    UnloadTimeout    = maps:get(unload_timeout   , Options, 60 * 1000),
    {Mod, Args} = mg_utils:separate_mod_opts(WorkerModOpts),
    State =
        #{
            id                => ID,
            mod               => Mod,
            status            => {loading, Args, ReqCtx},
            unload_tref       => undefined,
            hibernate_timeout => HibernateTimeout,
            unload_timeout    => UnloadTimeout
        },
    {ok, schedule_unload_timer(State)}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).

% загрузка делается отдельно и лениво, чтобы не блокировать этим супервизор,
% т.к. у него легко может начать расти очередь
handle_call(Call={call, _, _, _}, From, State=#{id:=ID, mod:=Mod, status:={loading, Args, ReqCtx}}) ->
    case Mod:handle_load(ID, Args, ReqCtx) of
        {ok, ModState} ->
            handle_call(Call, From, State#{status:={working, ModState}});
        Error={error, _} ->
            {stop, normal, Error, State}
    end;
handle_call({call, Deadline, Call, ReqCtx}, From, State=#{mod:=Mod, status:={working, ModState}}) ->
    case mg_deadline:is_reached(Deadline) of
        false ->
            {ReplyAction, NewModState} = Mod:handle_call(Call, From, ReqCtx, Deadline, ModState),
            NewState = State#{status:={working, NewModState}},
            case ReplyAction of
                {reply, Reply} -> {reply, Reply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
                noreply        -> {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)}
            end;
        true ->
            ok = logger:warning(
                "rancid worker call received: ~p from: ~p deadline: ~s reqctx: ~p",
                [Call, From, mg_deadline:format(Deadline), ReqCtx]
            ),
            {noreply, schedule_unload_timer(State), hibernate_timeout(State)}
    end;
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info({timeout, TRef, unload}, State=#{mod:=Mod, unload_tref:=TRef, status:=Status}) ->
    case Status of
        {working, ModState} ->
            _ = Mod:handle_unload(ModState);
        {loading, _, _} ->
            ok
    end,
    {stop, normal, State};
handle_info({timeout, _, unload}, State=#{}) ->
    % А кто-то опаздал!
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info ~p", [Info]),
    {noreply, State, hibernate_timeout(State)}.

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

-spec self_ref(options(), mg:ns(), mg:id()) ->
    mg_procreg:ref().
self_ref(Options, NS, ID) ->
    mg_procreg:ref(procreg_options(Options), ?wrap_id(NS, ID)).

-spec procreg_options(options()) ->
    mg_procreg:options().
procreg_options(#{registry := ProcregOptions}) ->
    ProcregOptions.
