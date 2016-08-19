-module(mg_storage_test_server).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-export_type([options/0]).
-behaviour(mg_storage).
-export([child_spec/3, create/3, get_status/2, get_history/3, resolve_tag/2, update/5]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% internal API
%%
-spec start_link(options(), mg_storage:timer_handler()) ->
    mg_utils:gen_start_ret().
start_link(Options, TimerHandler) ->
    gen_server:start_link(self_reg_name(Options), ?MODULE, {Options, TimerHandler}, []).

%%
%% mg_storage callbacks
%%
-type options() :: _Name::atom().

-spec child_spec(options(), atom(), mg_storage:timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, TimerHandler) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, TimerHandler]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec create(options(), mg:id(), _Args) ->
    ok.
create(Options, ID, Args) ->
    gen_server:call(self_ref(Options), {create, ID, Args}).

-spec get_status(options(), mg:id()) ->
    mg_storage:status() | undefined.
get_status(Options, ID) ->
    gen_server:call(self_ref(Options), {get_status, ID}).

-spec get_history(options(), mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    gen_server:call(self_ref(Options), {get_history, ID, Range}).

-spec resolve_tag(options(), mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    gen_server:call(self_ref(Options), {resolve_tag, Tag}).

-spec update(options(), mg:id(), mg_storage:status(), [mg:event()], mg:tag()) ->
    ok.
update(Options, ID, Status, Events, Tag) ->
    gen_server:call(self_ref(Options), {update, ID, Status, Events, Tag}).

%%
%% gen_server callbacks
%%
-type state() :: #{
    machines => #{mg:id () =>  mg_storage:status() },
    events   => #{mg:id () => [mg        :event ()]},
    tags     => #{mg:tag() =>  mg        :id    () },
    options  => options()
}.

-spec init({options(), mg_storage:timer_handler()}) ->
    mg_utils:gen_server_init_ret(state()).
init({Options, TimerHandler}) ->
    {ok,
        #{
            machines      => #{},
            events        => #{},
            tags          => #{},
            timer_handler => TimerHandler,
            options       => Options
        }
    }.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({create, ID, Args}, _From, State) ->
    NewState = do_create(ID, Args, State),
    {reply, ok, NewState};
handle_call({get_status, ID}, _From, State) ->
    Resp = do_get_status(ID, State),
    {reply, Resp, State};
handle_call({update, ID, Status, Events, Tag}, _From, State) ->
    NewState = do_update(ID, Status, Events, Tag, State),
    {reply, ok, NewState};
handle_call({add_events, ID, Events}, _From, State) ->
    NewState = do_add_events(ID, Events, State),
    {reply, ok, NewState};
handle_call({get_history, ID, Range}, _From, State) ->
    Resp = do_get_history(ID, Range, State),
    {reply, Resp, State};
handle_call({add_tag, ID, Tag}, _From, State) ->
    NewState = do_add_tag(ID, Tag, State),
    {reply, ok, NewState};
handle_call({resolve_tag, Tag}, _From, State) ->
    Resp = do_resolve_tag(Tag, State),
    {reply, Resp, State};

handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected cast received: ~p", [Cast]),
    {noreply, State}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected info ~p", [Info]),
    {noreply, State}.

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
-spec self_ref(atom()) ->
    mg_utils:gen_ref().
self_ref(Name) ->
    wrap_name(Name).

-spec self_reg_name(options()) ->
    mg_utils:gen_reg_name().
self_reg_name(Name) ->
    {local, wrap_name(Name)}.

-spec wrap_name(atom()) ->
    atom().
wrap_name(Name) ->
    erlang:list_to_atom(?MODULE_STRING ++ "_" ++ erlang:atom_to_list(Name)).

-spec do_create(mg:id(), mg:args(), state()) ->
    state().
do_create(ID, Args, State) ->
    do_store_events(ID, [], do_store_machine(ID, {created, Args}, State)).

-spec do_get_status(mg:id(), state()) ->
    mg_storage:status() | undefined.
do_get_status(ID, #{machines:=Machines}) ->
    try
        maps:get(ID, Machines)
    catch error:{badkey, ID} ->
        undefined
    end.

-spec do_update(mg:id(), mg_storage:status(), [mg:event()], mg:tag(), state()) ->
    {ok, state()}.
do_update(ID, Status, Events, Tag, State) ->
    do_actions(
        [
            fun(S) -> do_add_events   (ID, Events, S) end,
            fun(S) -> do_add_tag      (ID, Tag   , S) end,
            fun(S) -> do_update_status(ID, Status, S) end
        ],
        State
    ).

-spec do_update_status(mg:id(), mg_storage:status(), state()) ->
    state().
do_update_status(ID, Status, State) ->
    ok = try_set_timer(ID, Status, State),
    do_store_machine(ID, Status, State).

-spec do_add_events(mg:id(), [mg:event()], state()) ->
    state().
do_add_events(ID, NewMachineEvents, State=#{events:=Events}) ->
    MachineEvents = maps:get(ID, Events, []),
    do_store_events(ID, NewMachineEvents ++ MachineEvents, State).

-spec do_get_history(mg:id(), mg:history_range(), state()) ->
    mg:history().
do_get_history(ID, Range, #{events:=Events}) ->
    MachineEvents = maps:get(ID, Events, []),
    filter_history(MachineEvents, Range).

-spec do_add_tag(mg:id(), mg:tag() | undefined, state()) ->
    state().
do_add_tag(_, undefined, State) ->
    State;
do_add_tag(ID, Tag, State=#{tags:=Tags}) ->
    State#{tags:=maps:put(Tag, ID, Tags)}.

-spec do_resolve_tag(mg:tag(), state()) ->
    mg:id() | undefined.
do_resolve_tag(Tag, #{tags:=Tags}) ->
    try
        maps:get(Tag, Tags)
    catch error:{badkey, Tag} ->
        undefined
    end.

-spec do_store_machine(mg:id(), mg_storage:status(), state()) ->
    state().
do_store_machine(ID, Status, State=#{machines:=Machines}) ->
    State#{machines:=maps:put(ID, Status, Machines)}.

-spec do_store_events(mg:id(), [mg:event()], state()) ->
    state().
do_store_events(ID, MachineEvents, State=#{events:=Events}) ->
    State#{events:=maps:put(ID, MachineEvents, Events)}.

-spec try_set_timer(mg:id(), mg_storage:status(), state()) ->
    ok.
try_set_timer(ID, {working, TimerDateTime}, #{options:=Options, timer_handler:=TimerHandler})
    when TimerDateTime =/= undefined ->
    mg_timers:set(Options, ID, TimerDateTime, TimerHandler);
try_set_timer(_, _, _) ->
    ok.


-spec do_actions([fun((state()) -> state())], state()) ->
    state().
do_actions([], State) ->
    State;
do_actions([Action|RemainActions], State) ->
    NewState = Action(State),
    do_actions(RemainActions, NewState).

%%
%% history filtering
%%
-spec filter_history(mg:history(), mg:history_range() | undefined) ->
    mg:history().
filter_history(History, undefined) ->
    History;
filter_history(History, {After, Limit, Direction}) ->
    lists:reverse(filter_history_iter(apply_direction(Direction, History), After, Limit, [])).

-spec apply_direction(mg:direction(), mg:history()) ->
    mg:history().
apply_direction(forward, History) ->
    lists:reverse(History);
apply_direction(backward, History) ->
    History.

-spec filter_history_iter(mg:history(), mg:event_id() | undefined, non_neg_integer(), mg:history()) ->
    mg:history().
filter_history_iter([], _, _, Result) ->
    Result;
filter_history_iter(_, _, 0, Result) ->
    Result;
filter_history_iter([Event|HistoryTail], undefined, Limit, Result) ->
    filter_history_iter(HistoryTail, undefined, decrease_limit(Limit), [Event|Result]);
filter_history_iter([#{id:=EventID}|HistoryTail], After, Limit, []) when EventID =:= After ->
    filter_history_iter(HistoryTail, undefined, Limit, []);
filter_history_iter([_|HistoryTail], After, Limit, []) ->
    filter_history_iter(HistoryTail, After, Limit, []).

-spec decrease_limit(undefined | pos_integer()) ->
    non_neg_integer().
decrease_limit(undefined) ->
    undefined;
decrease_limit(N) ->
    N - 1.
