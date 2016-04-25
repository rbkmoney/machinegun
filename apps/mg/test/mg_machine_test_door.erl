-module(mg_machine_test_door).
-behaviour(mg_machine).

%% API
-type id    () :: binary().
-type passwd() :: binary().

-export([doors_sup_start_link/0]).
-export([open   /1]).
-export([close  /1]).
-export([lock   /2]).
-export([unlock /2]).

%% mg_machine callbacks
-export([handle_load/2, handle_call/2, handle_cast/2, handle_unload/1, apply_event/2]).

%%
%% API
%%
-spec doors_sup_start_link() ->
    {ok, pid()} | {error, _}.
doors_sup_start_link() ->
    mg_machines:start_link(?MODULE, ?MODULE, undefined).

-spec open(id()) ->
    ok | {error, bad_call}.
open(ID) ->
    mg_machines:call(?MODULE, ID, open).

-spec close(id()) ->
    ok | {error, bad_call}.
close(ID) ->
    mg_machines:call(?MODULE, ID, close).

-spec lock(id(), passwd()) ->
    ok | {error, bad_call}.
lock(ID, Passwd) ->
    mg_machines:call(?MODULE, ID, {lock, Passwd}).

-spec unlock(id(), passwd()) ->
    ok | {error, bad_call | bad_passwd}.
unlock(ID, Passwd) ->
    mg_machines:call(?MODULE, ID, {unlock, Passwd}).


%%
%% mg_machine callbacks
%%
-type state() :: opened  | closed   | {locked, passwd()}.
-type event() :: nothing | creating | opening | closing | {locking, passwd()} | unlocking.


-spec handle_load(id(), undefined) ->
    event().
handle_load(_ID, undefined) ->
    creating.

-spec handle_call(_,  state()) ->
    {_, event()}.
handle_call(open, closed) ->
    {ok, opening};
handle_call(close, opened) ->
    {ok, closing};
handle_call({lock, Passwd}, closed) ->
    {ok, {locking, Passwd}};
handle_call({unlock, Passwd0}, {locked, Passwd1}) ->
    case Passwd0 =:= Passwd1 of
        true  -> {ok                 , unlocking};
        false -> {{error, bad_passwd}, nothing  }
    end;
handle_call(_Call, _State) ->
    {{error, bad_call}, nothing}.

-spec handle_cast(_, state()) ->
    event().
handle_cast(_, _) ->
    erlang:error(bad_cast).

-spec handle_unload(state()) ->
    ok.
handle_unload(_) ->
    ok.

-spec
apply_event( event()         , state()    ) -> state().
apply_event( nothing         , State      ) -> State;
apply_event( creating        , undefined  ) -> opened;
apply_event( opening         , closed     ) -> opened;
apply_event( closing         , opened     ) -> closed;
apply_event({locking, Passwd}, closed     ) -> {locked, Passwd};
apply_event( unlocking       , {locked, _}) -> closed.
