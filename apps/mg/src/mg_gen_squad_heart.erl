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

-module(mg_gen_squad_heart).

-export([start_link/2]).
-export([update_members/2]).

-export([broadcast/6]).

%%

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

%%

-type message() :: howdy.

-type payload() ::
    #{
        vsn     := 1,
        msg     := message(),
        from    := pid(),
        members := [pid()]
    }.

-type envelope() :: {'$squad', payload()}.

-type opts() :: #{
    heartbeat => mg_gen_squad:heartbeat_opts(),
    pulse     => mg_gen_squad_pulse:handler()
}.

-export_type([message/0]).
-export_type([payload/0]).
-export_type([envelope/0]).

-export_type([opts/0]).

%%

-spec start_link(_Feedback, opts()) ->
    {ok, pid()}.
start_link(Feedback, Opts) ->
    gen_server:start_link(?MODULE, mk_state(self(), Feedback, Opts), []).

-spec update_members([pid()], pid()) ->
    ok.
update_members(Members, HeartPid) ->
    gen_server:cast(HeartPid, {members, Members}).

%%

-spec broadcast(message(), pid(), [pid()], [pid()], _Ctx, opts()) ->
    ok.
broadcast(Message, Self, Members, Recepients, Ctx, Opts) ->
    Payload = mk_payload(Message, Self, Members),
    lists:foreach(
        fun (Pid) ->
            _ = gen_server:cast(Pid, {'$squad', Payload}),
            _ = beat({{broadcast, Payload}, {sent, Pid, Ctx}}, Opts)
        end,
        Recepients
    ).

-spec mk_payload(message(), _Self :: pid(), _Members :: [pid()]) ->
    payload().
mk_payload(Message, Self, Members) ->
    #{
        vsn     => 1,
        msg     => Message,
        from    => Self,
        members => Members
    }.

%%

-record(st, {
    self     :: pid(),
    feedback :: _,
    monitor  :: reference() | undefined,
    members  :: [pid()],
    opts     :: opts(),
    timer    :: reference() | undefined
}).

-type st() :: #st{}.

-spec mk_state(pid(), _Feedback, opts()) ->
    st().
mk_state(Self, Feedback, Opts) ->
    #st{
        self     = Self,
        feedback = Feedback,
        members  = [],
        opts     = Opts
    }.

-spec init(st()) ->
    {ok, st()}.
init(St) ->
    {ok, monitor_self(defer_heartbeat(St))}.

-type from() :: {pid(), _}.

-spec handle_call(_Call, from(), st()) ->
    {noreply, st()}.
handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St}.

-type cast() ::
    {members, [pid()]}.

-spec handle_cast(cast(), st()) ->
    {noreply, st()}.
handle_cast({members, Members}, St = #st{}) ->
    {noreply, St#st{members = Members}};
handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-type info() ::
    {timeout, reference(), heartbeat} |
    {'DOWN', reference(), process, pid(), _Reason}.

-spec handle_info(info(), st()) ->
    {noreply, st()}.
handle_info({timeout, TRef, heartbeat = Msg}, St) ->
    _ = beat({{timer, TRef}, {fired, Msg}}, St),
    {noreply, defer_heartbeat(handle_heartbeat(TRef, St))};
handle_info({'DOWN', MRef, process, Self, Reason}, St = #st{self = Self, monitor = MRef}) ->
    {stop, Reason, St#st{monitor = undefined}};
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St}.

%%

-spec defer_heartbeat(st()) ->
    st().
defer_heartbeat(St = #st{timer = undefined, opts = #{heartbeat := HeartbeatOpts}}) ->
    Msg = heartbeat,
    Timeout = maps:get(broadcast_interval, HeartbeatOpts),
    TRef = erlang:start_timer(Timeout, self(), Msg),
    _ = beat({{timer, TRef}, {started, Timeout, Msg}}, St),
    St#st{timer = TRef}.

-spec handle_heartbeat(reference(), st()) ->
    st().
handle_heartbeat(TRef, St = #st{timer = TRef, self = Self, members = Members}) ->
    ok = broadcast(howdy, Self, Members, Members, heartbeat, St#st.opts),
    ok = gen_server:cast(Self, St#st.feedback),
    St#st{timer = undefined}.

-spec monitor_self(st()) ->
    st().
monitor_self(St = #st{self = Self}) ->
    MRef = erlang:monitor(process, Self),
    St#st{monitor = MRef}.

%%

-spec beat(mg_gen_squad_pulse:beat(), st() | opts()) ->
    _.
beat(Beat, #st{opts = Opts}) ->
    beat(Beat, Opts);
beat(Beat, #{pulse := Handler}) ->
    mg_gen_squad_pulse:handle_beat(Handler, Beat);
beat(_Beat, _St) ->
    ok.
