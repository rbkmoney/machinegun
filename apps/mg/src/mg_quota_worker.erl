%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_quota_worker).

-behaviour(gen_server).

-export([child_spec/2]).
-export([start_link/1]).

-export([reserve/4]).

%% gen_server callbacks
-export([init/1]).
-export([handle_info/2]).
-export([handle_cast/2]).
-export([handle_call/3]).
-export([code_change/3]).
-export([terminate/2]).

%% Types
-type options() :: #{
    name := name(),
    limit := limit_options(),
    update_interval => timeout()
}.
-type name() :: binary() | unlimited.
-type share() :: mg_quota:share().
-type resource() :: mg_quota:resource().
-type client_id() :: mg_quota:client_id().
-type limit_options() :: mg_quota:limit_options().

-export_type([name/0]).
-export_type([share/0]).
-export_type([options/0]).
-export_type([resource/0]).
-export_type([client_id/0]).
-export_type([limit_options/0]).

%% Internal types
-record(state, {
    quota :: quota(),
    options :: options(),
    clients :: #{client_id() => pid()},
    client_monitors :: #{monitor() => client_id()},
    timer :: reference(),
    interval :: timeout()
}).
-record(client, {
    client_id :: client_id(),
    monitor :: monitor(),
    pid :: pid()
}).
-type state() :: #state{}.
-type quota() :: mg_quota:state().
-type client() :: #client{}.
-type client_options() :: mg_quota:client_options().
-type monitor() :: reference().

-define(DEFAULT_UPDATE_INTERVAL, 5000).
-define(UPDATE_MESSAGE, recalculate_targets).

%%
%% API
%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(#{name := Name} = Options) ->
    gen_server:start_link(self_reg_name(Name), ?MODULE, Options, []).

-spec reserve(client_options(), Usage :: resource(), Expectation :: resource(), name()) ->
    NewTarget :: resource().
reserve(_ClientOptions, _Usage, Expectation, unlimited) ->
    Expectation;
reserve(ClientOptions, Usage, Expectation, Name) ->
    gen_server:call(self_ref(Name), {reserve, ClientOptions, Usage, Expectation}).

%% gen_server callbacks

-spec init(options()) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    #{limit := Limit} = Options,
    Interval = maps:get(update_interval, Options, ?DEFAULT_UPDATE_INTERVAL),
    {ok, #state{
        options = Options,
        quota = mg_quota:new(#{limit => Limit}),
        clients = #{},
        client_monitors = #{},
        interval = Interval,
        timer = erlang:send_after(Interval, self(), ?UPDATE_MESSAGE)
    }}.

-spec handle_call(Call :: any(), mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({reserve, ClientOptions, Usage, Expectation}, {Pid, _Tag}, State0) ->
    State1 = ensure_is_registered(ClientOptions, Pid, State0),
    {ok, NewReserved, NewQuota} = mg_quota:reserve(ClientOptions, Usage, Expectation, State1#state.quota),
    {reply, NewReserved, State1#state{quota = NewQuota}};
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(Cast :: any(), state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

-spec handle_info(Info :: any(), state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(?UPDATE_MESSAGE, State) ->
    {ok, NewQuota} = mg_quota:recalculate_targets(State#state.quota),
    {noreply, restart_timer(?UPDATE_MESSAGE, State#state{quota = NewQuota})};
handle_info({'DOWN', Monitor, process, _Object, _Info}, State) ->
    {noreply, forget_about_client(Monitor, State)};
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected gen_server info received: ~p", [Info]),
    {noreply, State}.

-spec code_change(OldVsn :: any(), state(), Extra :: any()) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec terminate(Reason :: any(), state()) ->
    ok.
terminate(_Reason, _State) ->
    ok.

%% Internals

% Client monitoring

-spec ensure_is_registered(client_options(), pid(), state()) ->
    state().
ensure_is_registered(#{client_id := ClientID}, Pid, #state{clients = Clients} = State) ->
    do_ensure_is_registered(maps:get(ClientID, Clients, undefined), ClientID, Pid, State).

-spec do_ensure_is_registered(client() | undefined, client_id(), pid(), state()) ->
    state().
do_ensure_is_registered(undefined, ID, Pid, State) ->
    #state{clients = Clients, client_monitors = Monitors} = State,
    Monitor = erlang:monitor(process, Pid),
    Client = #client{client_id = ID, pid = Pid, monitor = Monitor},
    State#state{
        clients = Clients#{ID => Client},
        client_monitors = Monitors#{Monitor => ID}
    };
do_ensure_is_registered(#client{pid = ClientPid}, _ID, Pid, State) when ClientPid =:= Pid ->
    State.

-spec forget_about_client(monitor(), state()) ->
    state().
forget_about_client(Monitor, State) ->
    #state{clients = AllClients, client_monitors = Monitors, quota = Quota} = State,
    case maps:find(Monitor, Monitors) of
        {ok, ClientID} ->
            State#state{
                clients = maps:remove(ClientID, AllClients),
                client_monitors = maps:remove(Monitor, Monitors),
                quota = mg_quota:remove_client(ClientID, Quota)
            };
        error ->
            State
    end.

% Worker registration

-spec self_ref(name()) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec self_reg_name(name()) ->
    mg_utils:gen_reg_name().
self_reg_name(ID) ->
    {via, gproc, {n, l, wrap_id(ID)}}.

-spec wrap_id(name()) ->
    term().
wrap_id(ID) ->
    {?MODULE, ID}.

% Timer

-spec restart_timer(any(), state()) -> state().
restart_timer(Message, #state{timer = TimerRef, interval = Interval} = State) ->
    _ = erlang:cancel_timer(TimerRef),
    State#state{timer = erlang:send_after(Interval, self(), Message)}.
