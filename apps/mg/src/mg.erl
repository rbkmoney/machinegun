-module(mg).

%% Types
%% base
-export_type([ns       /0]).
-export_type([id       /0]).
-export_type([tag      /0]).
-export_type([args     /0]).
-export_type([timeout_ /0]).
-export_type([timer    /0]).
-export_type([ref      /0]).
-export_type([direction/0]).

%% events and history
-export_type([aux_state    /0]).
-export_type([event_id     /0]).
-export_type([event_body   /0]).
-export_type([event        /0]).
-export_type([history_range/0]).
-export_type([history      /0]).
-export_type([machine      /0]).

%% actions
-export_type([tag_action      /0]).
-export_type([set_timer_action/0]).
-export_type([complex_action  /0]).

%% calls, signals, get_gistory
-export_type([state_change /0]).
-export_type([signal       /0]).
-export_type([call_response/0]).
-export_type([signal_args  /0]).
-export_type([call_args    /0]).
-export_type([signal_result/0]).
-export_type([call_result  /0]).

-export_type([machine_descriptor/0]).

%% event sink
-export_type([sink_event  /0]).
-export_type([sink_history/0]).


%% base
-type ns       () :: _.
-type id       () :: _.
-type tag      () :: _.
-type args     () :: _.
-type timeout_ () :: non_neg_integer().
-type timer    () :: {timeout, timeout_()} | {deadline, calendar:datetime()}.
-type ref      () :: {id, id()} | {tag, tag()}.
-type direction() :: forward | backward.

%% state, events and history
-type aux_state    () :: _ | undefined.
-type event_id     () :: pos_integer().
-type event_body   () :: _.
-type event        () :: #{
    id         => id(),
    created_at => calendar:datetime(),
    body       => event_body()
}.
-type history_range() :: {After::id() | undefined, Limit::non_neg_integer() | undefined, direction()}.
-type history      () :: [event()].
-type machine      () :: #{
    ns            => ns(),
    id            => id(),
    history       => history(),
    history_range => history_range(),
    aux_state     => aux_state()
}.

%% actions
-type tag_action         () :: tag().
-type set_timer_action   () :: timer().
-type complex_action  () :: #{
    timer => set_timer_action() | undefined,
    tag   => tag_action      () | undefined
}.

%% calls, signals, get_gistory
-type state_change () :: {aux_state(), [event_body()]}.
-type signal       () :: {init, args()} | timeout | {repair, args()}.
-type call_response() :: term().
-type signal_args  () :: {signal(), machine()}.
-type call_args    () :: {args(), machine()}.
-type signal_result() :: {state_change(), complex_action()}.
-type call_result  () :: {call_response(), state_change(), complex_action()}.

-type machine_descriptor() :: {ns(), ref(), history_range()}.

%% event sink
-type sink_event() :: #{
    source_ns => ns(),
    source_id => id(),
    event     => event()
}.
-type sink_history() :: [sink_event()].
