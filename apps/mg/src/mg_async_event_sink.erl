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

-module(mg_async_event_sink).

-behaviour(gen_server).
-behaviour(mg_async_action).

-export([child_spec/2]).
-export([need_to_run/1]).
-export([update/2]).
-export([publish/2]).
-export([start_link/3]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

-type state() :: #{
    timer   := undefined | reference(),
    options => mg_events_machine:options(),
    id      => mg:id(),
    events  => [mg_events:event()]
}.

-type init_args() :: [].

%% API

-spec child_spec(mg_events_machine:options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ID) ->
    #{
        id       => ID,
        start    => {?MODULE, start_link, [Options]},
        restart  => transient,
        type     => worker
    }.

-spec need_to_run(mg_events_machine:state()) ->
    boolean().
need_to_run(#{last_published_event := undefined, events_range := undefined}) ->
    false;
need_to_run(#{last_published_event := undefined}) ->
    true;
need_to_run(#{last_published_event := LastPublishedEvent, events_range := EventsRange}) ->
    LastPublishedEvent < mg_events:get_last_event_id(EventsRange).

-spec update(mg_events_machine:last_event_id(), mg_events_machine:state()) ->
    mg_events_machine:state().
update(LastEventID, State) ->
    State#{
        last_published_event => LastEventID
    }.

-spec publish(mg_events_machine:options(), mg:id()) ->
    ok.
publish(Options, ID) ->
    _ = erlang:spawn(
        fun() ->
            try
                do_publish(Options, ID)
            catch
                exit:{Reason, _} when Reason == noproc; Reason == normal ->
                    ok = start(Options, ID),
                    do_publish(Options, ID)
            end
        end
    ),
    ok.

-spec start_link(mg_events_machine:options(), mg:id(), init_args()) ->
    mg_procreg:start_link_ret().
start_link(Options, ID, Args) ->
    mg_procreg:start_link(procreg_options(Options), name(Options, ID), ?MODULE, Args, []).

%% gen_server callbacks

-spec init(init_args()) ->
    mg_utils:gen_server_init_ret(state()).
init([]) ->
    {ok, #{timer => undefined}}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({publish, Options, ID}, From, State) ->
    gen_server:reply(From, ok),
    case mg_events_machine:get_unpublished_events(Options, ref(ID)) of
        [] ->
            {stop, normal, State};
        Events ->
            NewState = State#{
                options => Options,
                id      => ID,
                events  => Events
            },
            try_publish(NewState)
    end;
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    try_publish(State);
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info ~p", [Info]),
    {noreply, State}.

-spec code_change(_OldVsn, state(), _Extra) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

%% Internal functions

-spec do_publish(mg_events_machine:options(), mg:id()) ->
    ok.
do_publish(Options, ID) ->
    mg_procreg:call(procreg_options(Options), name(Options, ID), {publish, Options, ID}).

-spec name(mg_events_machine:options(), mg:id()) ->
    mg_procreg:name().
name(Options, ID) ->
    {namespace(Options), ID, ?MODULE}.

-spec namespace(mg_events_machine:options()) ->
    mg:ns().
namespace(#{namespace := Namespace}) ->
    Namespace.

-spec start(mg_events_machine:options(), mg:id()) ->
    ok.
start(Options, ID) ->
    case mg_async_action:start_child(Options, ?MODULE, [ID, []]) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok
    end.

-spec procreg_options(mg_events_machine:options()) ->
    mg_procreg:options().
procreg_options(_Options) ->
    mg_procreg_gproc.

-spec ref(mg:id()) ->
    mg_events_machine:ref().
ref(ID) ->
    {id, ID}.

-spec try_publish(state()) ->
    {stop, normal, state()} | {noreply, state()}.
try_publish(State) ->
    #{
        options := Options,
        id      := ID,
        events  := Events
    } = State,
    try
        ok = push_events_to_event_sinks(Options, ID, Events),
        LastEventID = get_last(Events),
        ok = mg_events_machine:update(Options, ref(ID), ?MODULE, LastEventID),
        {stop, normal, State}
    catch
        throw:{transient, Error} ->
            ok = logger:error("transient error while pushing to event sink: ~p", [Error]),
            NewState = schedule_retry(State),
            {noreply, NewState};
        Class:Error ->
            ok = logger:error("unexpected error while pushing to event sink: ~p:~p", [Class, Error]),
            ok = mg_events_machine:fail(Options, ref(ID), null, undefined),
            {stop, {shutdown, {Class, Error}}, State}
    end.

-spec get_last([mg_events:event()]) ->
    mg_events_machine:last_event_id().
get_last([#{id := ID} | Tail]) ->
    get_last(Tail, ID).

-spec get_last([mg_events:event()], mg_events_machine:last_event_id()) ->
    mg_events_machine:last_event_id().
get_last([], LastEventID) ->
    LastEventID;
get_last([#{id := ID} | Tail], LastEventID) ->
    get_last(Tail, erlang:max(LastEventID, ID)).

-spec push_events_to_event_sinks(mg_events_machine:options(), mg:id(), [mg_events:event()]) ->
    ok.
push_events_to_event_sinks(Options, ID, Events) ->
    Namespace  = maps:get(namespace, Options),
    EventSinks = maps:get(event_sinks, Options, []),
    ReqCtx     = null,
    Deadline   = undefined,
    lists:foreach(
        fun(EventSinkHandler) ->
            ok = mg_events_sink:add_events(
                EventSinkHandler,
                Namespace,
                ID,
                Events,
                ReqCtx,
                Deadline
            )
        end,
        EventSinks
    ).

-spec schedule_retry(state()) ->
    state().
schedule_retry(#{timer := TRef} = State) ->
    case TRef of
        undefined ->
            ok;
        _ ->
            ok = erlang:cancel_timer(TRef, [{async, false}, {info, false}])
    end,
    State#{
        timer => erlang:send_after(5000, self(), timeout) % TODO: configure and use retry strategy
    }.
