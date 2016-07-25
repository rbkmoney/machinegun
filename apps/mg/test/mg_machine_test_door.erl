-module(mg_machine_test_door).
-behaviour(mg_machine).

%% API
-export([start_link/0]).

-export([start     /1]).
-export([open      /1]).
-export([close     /1]).
-export([lock      /2]).
-export([unlock    /2]).

%% mg_machine callbacks
-export([process_signal/2, process_call/2]).

-define(MACHINE_OPTIONS, {?MODULE, mg_machine_db_test}).


%%
%% API
%%
-spec start_link() ->
    mg_utils:gen_start_ret().
start_link() ->
    mg_machine:start_link(?MACHINE_OPTIONS).

-spec start(_Tag) ->
    _ID.
start(Tag) ->
    mg_machine:start(?MACHINE_OPTIONS, Tag, sync).

-spec open(_Tag) ->
    ok | {error, bad_state}.
open(Tag) ->
    mg_machine:call(?MACHINE_OPTIONS, {tag, Tag}, open).

-spec close(_Tag) ->
    ok | {error, bad_state}.
close(Tag) ->
    mg_machine:call(?MACHINE_OPTIONS, {tag, Tag}, close).

-spec lock(_Tag, _Passwd) ->
    ok | {error, bad_state}.
lock(Tag, Passwd) ->
    mg_machine:call(?MACHINE_OPTIONS, {tag, Tag}, {lock, Passwd}).

-spec unlock(_Tag, _Passwd) ->
    ok | {error, bad_state | bad_passwd}.
unlock(Tag, Passwd) ->
    mg_machine:call(?MACHINE_OPTIONS, {tag, Tag}, {unlock, Passwd}).


%%
%% mg_machine callbacks
%%
-type event() ::
      nothing
    | {creating, _Tag}
    | repairing
    | opening
    | closing
    | {locking, _Passwd}
    | unlocking
.

-type state() ::
      undefined
    | opened
    | closed
    | {locked, _Passwd}
.


-spec process_signal(mg_machine:signal(), mg_machine:history()) ->
    {event(), mg_machine:actions()}.
process_signal(Signal, History) ->
    OldState = collapse_history(History),
    Event    = handle_signal_(Signal, OldState),
    NewState = apply_event(Event, OldState),
    {Event, add_actions_from_new_state(NewState, add_actions_from_event(Event, #{}))}.

-spec process_call(_Call, mg_machine:history()) ->
    {_Resp, event(), mg_machine:actions()}.
process_call(Call, History) ->
    OldState      = collapse_history(History),
    {Resp, Event} = handle_call_(Call, OldState),
    NewState      = apply_event(Event, OldState),
    {Resp, Event, add_actions_from_new_state(NewState, add_actions_from_event(Event, #{}))}.

%%
%% local
%%
-spec handle_signal_(mg_machine:signal(), state()) ->
    event().
handle_signal_({init, Tag}, undefined) ->
    {creating, Tag};
handle_signal_(timeout, opened) ->
    closing;
handle_signal_({repair, _}, _) ->
    repairing.

-spec handle_call_(_Call, state()) ->
    {_Resp, event()}.
handle_call_(open, closed) ->
    {ok, opening};
handle_call_(close, opened) ->
    {ok, closing};
handle_call_({lock, Passwd}, closed) ->
    {ok, {locking, Passwd}};
handle_call_({unlock, Passwd0}, {locked, Passwd1}) ->
    case Passwd0 =:= Passwd1 of
        true  -> { ok                , unlocking};
        false -> {{error, bad_passwd}, nothing  }
    end;
handle_call_(_Call, _State) ->
    {{error, bad_state}, nothing}.

-spec collapse_history(mg_machine:history()) ->
    state().
collapse_history(History) ->
    maps:fold(
        fun(_Key, Event, StateAcc) ->
            apply_event(Event, StateAcc)
        end,
        undefined,
        History
    ).

-spec
apply_event( event()         , state()    ) -> state().
apply_event( nothing         , State      ) -> State;
apply_event({creating, _}    , undefined  ) -> opened;
apply_event( repairing       , _          ) -> opened;
apply_event( opening         , closed     ) -> opened;
apply_event( closing         , opened     ) -> closed;
apply_event({locking, Passwd}, closed     ) -> {locked, Passwd};
apply_event( unlocking       , {locked, _}) -> closed.

-spec add_actions_from_event(event(), mg_machine:actions()) ->
    mg_machine:actions().
add_actions_from_event({creating, Tag}, Actions) ->
    Actions#{tag => Tag};
add_actions_from_event(_, Actions) ->
    Actions.

-spec add_actions_from_new_state(state(), mg_machine:actions()) ->
    mg_machine:actions().
add_actions_from_new_state(opened, Actions) ->
    Actions#{timeout => {relative, 1}};
add_actions_from_new_state(_, Actions) ->
    Actions.
