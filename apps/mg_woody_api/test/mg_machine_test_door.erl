-module(mg_machine_test_door).

%% API
-export([start_link/1]).

-export([start       /3]).
-export([do_action   /3]).
-export([repair      /3]).
-export([update_state/3]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

%% processor woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).


-type processor_options() :: {_Host, _Port, _Path}.
-type automaton_options() :: {_BaseURL, _NS}.

-define(aux_state, <<"aux_state">>).

%%
%% API
%%
-type event() ::
      {creating, _Tag}
    | repairing
    | opening
    | closing
    | {locking, _Passwd}
    | unlocking
.
-type events() :: [event()].

-type state() ::
      undefined
    | open
    | closed
    | {locked, _Passwd}
.

-type action() ::
       open
    |  close
    | {lock  , _Passwd}
    | {unlock, _Passwd}
    |  fail
    |  touch
.

-type client_state() :: #{
    last_event_id => mg:event_id(),
    state         => state()
}.


-spec start_link(processor_options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

-spec start(automaton_options(), mg:id(), mg:tag()) ->
    ok.
start(Options, ID, Tag) ->
    mg_automaton_client:start(Options, ID, Tag).

-spec do_action(automaton_options(), mg:ref(), action()) -> ok | {error, bad_state | bad_passwd}.
do_action(Options, Action, Ref) ->
    unpack(resp, mg_automaton_client:call(Options, Ref, pack(action, Action))).

-spec repair(automaton_options(), mg:ref(), ok | error) ->
    ok.
repair(Options, Ref, RepairResult) ->
    mg_automaton_client:repair(Options, Ref, pack(repair_result, RepairResult)).

-spec update_state(automaton_options(), mg:ref(), client_state()) ->
    client_state().
update_state(Options, Ref, ClientState=#{last_event_id:=LastEventID, state:=State}) ->
    #'Machine'{history=History} =
        mg_automaton_client:get_machine(
            Options, Ref, #'HistoryRange'{'after'=LastEventID, limit=1, direction=forward}
        ),
    case History of
        [] ->
            ClientState;
        [#'Event'{id=EventID, event_payload=Event}] ->
            NewState = apply_events([unpack(event, Event)], State),
            update_state(Options, Ref, ClientState#{last_event_id:=EventID, state:=NewState})
    end.

%%
%% Supervisor callbacks
%%
-spec init(processor_options()) ->
    mg_utils:supervisor_ret().
init({Host, Port, Path}) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        woody_server:child_spec(
            ?MODULE,
            #{
                ip            => Host,
                port          => Port,
                net_opts      => [],
                event_handler => mg_woody_api_event_handler,
                handlers      => [{Path, {{mg_proto_state_processing_thrift, 'Processor'}, ?MODULE, []}}]
            }
        )
    ]}}.

%%
%% processor woody handler
%%
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), _Options) ->
    {ok| _Resp, woody_client:context()} | no_return().

handle_function('ProcessSignal', {SignalArgs}, WoodyContext, Options) ->
    {process_signal(Options, SignalArgs), WoodyContext};

handle_function('ProcessCall', {CallArgs}, WoodyContext, Options) ->
    {process_call(Options, CallArgs), WoodyContext}.

%%
%% local
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, #'SignalArgs'{signal=Signal, machine=#'Machine'{history=History}}) ->
    State = collapse_history(History),
    Events = handle_signal_(Signal, State),
    #'SignalResult'{
        change =
            #'MachineStateChange'{
                aux_state = ?aux_state,
                events    = pack(events, Events)
            },
        action = actions_from_events(Events, State)
    }.

-spec process_call(_, mg:call_args()) ->
    mg:call_result().
process_call(_, #'CallArgs'{arg=Action, machine=#'Machine'{aux_state=?aux_state, history=History}}) ->
    State = collapse_history(History),
    {Resp, Events} = handle_action(unpack(action, Action), State),
    #'CallResult'{
        response = pack(resp, Resp),
        change =
            #'MachineStateChange'{
                aux_state = ?aux_state,
                events    = pack(events, Events)
            },
        action   = actions_from_events(Events, State)
    }.

-spec handle_signal_(mg:signal(), state()) ->
    event().
handle_signal_({init, #'InitSignal'{arg=Tag}}, undefined) ->
    [{creating, Tag}];
handle_signal_({timeout, #'TimeoutSignal'{}}, open) ->
    [closing];
handle_signal_({repair, #'RepairSignal'{arg=Args}}, _) ->
    case unpack(repair_result, Args) of
        ok ->
            [repairing];
        error ->
            exit(1)
    end.

-spec handle_action(_Call, state()) ->
    {_Resp, event()}.
handle_action(open, closed) ->
    {ok, [opening]};
handle_action(close, open) ->
    {ok, [closing]};
handle_action({lock, Passwd}, closed) ->
    {ok, [{locking, Passwd}]};
handle_action({unlock, Passwd0}, {locked, Passwd1}) ->
    case Passwd0 =:= Passwd1 of
        true  -> { ok                , [unlocking]};
        false -> {{error, bad_passwd}, [         ]}
    end;
handle_action(fail, _State) ->
    exit(action_fail);
handle_action(touch, _State) ->
    {ok, []};
handle_action(_, _State) ->
    {{error, bad_state}, []}.

-spec collapse_history(mg:history()) ->
    state().
collapse_history(History) ->
    apply_events([unpack(event, Event) || #'Event'{event_payload=Event} <- History], undefined).

-spec apply_events(events(), state()) ->
    state().
apply_events(Events, State) ->
    lists:foldl(fun apply_event/2, State, Events).

-spec
apply_event( event()         , state()    ) -> state().
apply_event({creating, _}    , undefined  ) -> open;
apply_event( repairing       , _          ) -> open;
apply_event( opening         , closed     ) -> open;
apply_event( closing         , open       ) -> closed;
apply_event({locking, Passwd}, closed     ) -> {locked, Passwd};
apply_event( unlocking       , {locked, _}) -> closed.

-spec actions_from_events(events(), state()) ->
    mg:actions().
actions_from_events([], _) ->
    #'ComplexAction'{};
actions_from_events(Events=[Event], OldState) ->
    NewState = apply_events(Events, OldState),
    #'ComplexAction'{
        set_timer = set_timer(NewState),
        tag       = tag(Event)
    }.

-spec set_timer(state()) ->
    mg:set_timer_action().
% set_timer(open) ->
    % #'SetTimerAction'{timer = {timeout, 2}};
set_timer(_) ->
    undefined.

-spec tag(events()) ->
    mg:tag_action().
tag({creating, Tag}) ->
    #'TagAction'{tag = Tag};
tag(_) ->
    undefined.

-spec pack(_Type, _Value) ->
    binary().
pack(events, Events) ->
    [pack(event, Event) || Event <- Events];
pack(_, V) ->
    term_to_binary(V).

-spec unpack(_Type, binary()) ->
    _Value.
unpack(events, Events) ->
    [unpack(event, Event) || Event <- Events];
unpack(_, V) ->
    binary_to_term(V).
