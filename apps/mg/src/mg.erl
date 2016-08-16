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
-export_type([event_id     /0]).
-export_type([event_body   /0]).
-export_type([event        /0]).
-export_type([history      /0]).

%% actions
-export_type([tag_action      /0]).
-export_type([set_timer_action/0]).
-export_type([complex_action  /0]).

%% calls, signals, get_gistory
-export_type([signal       /0]).
-export_type([call_response/0]).
-export_type([signal_args  /0]).
-export_type([call_args    /0]).
-export_type([signal_result/0]).
-export_type([call_result  /0]).
-export_type([history_range/0]).

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

%% events and history
-type event_id     () :: pos_integer().
-type event_body   () :: _.
-type event        () :: #{
    id         => id(),
    created_at => calendar:datetime(),
    body       => event_body()
}.
-type history      () :: [event_body()].

%% actions
-type tag_action      () :: tag().
-type set_timer_action() :: timer().
-type complex_action  () :: #{
    timer => set_timer_action() | undefined,
    tag   => tag_action      () | undefined
}.

%% calls, signals, get_gistory
-type signal       () :: {init, id(), args()} | timeout | {repair, args()}.
-type call_response() :: term().
-type signal_args  () :: {signal(), history()}.
-type call_args    () :: {args(), history()}.
-type signal_result() :: {[event_body()], complex_action()}.
-type call_result  () :: {call_response(), [event_body()], complex_action()}.
-type history_range() :: {After::id() | undefined, Limit::non_neg_integer() | undefined, direction()}.

%% event sink
-type sink_event() :: #{
    source_ns => ns(),
    source_id => id(),
    event     => event()
}.
-type sink_history() :: [sink_event()].
