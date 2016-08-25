-module(mg_storage_test_server).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/1]).

%% mg_storage like callbacks
-export_type([options/0]).
-export([child_spec/2, create_machine/3, get_machine/2, get_history/3, resolve_tag/2, update_machine/4]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% internal API
%%
-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    gen_server:start_link(self_reg_name(Options), ?MODULE, Options, []).

%%
%% mg_storage callbacks
%%
-type options() :: _Name::atom().

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec create_machine(_Options, mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, ID, Args) ->
    gen_server:call(self_ref(Options), {create_machine, ID, Args}).

-spec get_machine(options(), mg:id()) ->
    mg_storage:machine() | undefined.
get_machine(Options, ID) ->
    gen_server:call(self_ref(Options), {get_machine, ID}).

-spec get_history(options(), mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    gen_server:call(self_ref(Options), {get_history, ID, Range}).

-spec resolve_tag(options(), mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    gen_server:call(self_ref(Options), {resolve_tag, Tag}).

-spec update_machine(options(), mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
update_machine(Options, ID, Machine, Update) ->
    gen_server:call(self_ref(Options), {update_machine, ID, Machine, Update}).

%%
%% gen_server callbacks
%%
-type state() :: #{
    machines => #{mg:id () =>  mg_storage:machine() },
    events   => #{mg:id () => [mg        :event  ()]},
    tags     => #{mg:tag() =>  mg        :id     () },
    options  => options()
}.

-spec init({options(), mg_storage:timer_handler()}) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    {ok,
        #{
            machines      => #{},
            events        => #{},
            tags          => #{},
            options       => Options
        }
    }.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({create_machine, ID, Args}, _From, State) ->
    {Resp, NewState} = do_create_machine(ID, Args, State),
    {reply, Resp, NewState};
handle_call({get_machine, ID}, _From, State) ->
    Resp = do_get_machine(ID, State),
    {reply, Resp, State};
handle_call({update_machine, ID, Machine, Update}, _From, State) ->
    {Resp, NewState} = do_update_machine(ID, Machine, Update, State),
    {reply, Resp, NewState};
handle_call({get_history, ID, Range}, _From, State) ->
    Resp = do_get_history(ID, Range, State),
    {reply, Resp, State};
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

-spec do_get_machine(mg:id(), state()) ->
    mg_storage:machine() | undefined.
do_get_machine(ID, #{machines:=Machines}) ->
    try
        maps:get(ID, Machines)
    catch error:{badkey, ID} ->
        undefined
    end.

-spec do_create_machine(mg:id(), mg:args(), state()) ->
    {mg_storage:machine(), state()}.
do_create_machine(ID, Args, State) ->
    Machine =
        #{
            status        => {created, Args},
            last_event_id => undefined,
            db_state      => 1
        },
    NewState = do_store_machine(ID, Machine, State),
    {Machine, NewState}.

-spec do_update_machine(mg:id(), mg_storage:machine(), mg_storage:update(), state()) ->
    {mg_storage:machine(), state()}.
do_update_machine(ID, Machine, Update, State) ->
    % хотим убедится, что логика правильно работает с экземпляром machine
    ok = check_machine_version(ID, Machine, State),

    OldStatus = maps:get(status, Machine),
    NewStatus = maps:get(status, Update, OldStatus),
    ok = try_set_timer(ID, NewStatus, State),

    NewMachine =
        Machine#{
            status        := NewStatus,
            last_event_id := maps:get(last_event_id, Update, maps:get(last_event_id, Machine)),
            db_state      := maps:get(db_state, Machine) + 1
        },
    NewState =
        do_actions(
            [
                fun(S) -> do_add_events   (ID, maps:get(new_events, Update, []       ), S) end,
                fun(S) -> do_add_tag      (ID, maps:get(new_tag   , Update, undefined), S) end,
                fun(S) -> do_store_machine(ID, NewMachine, S) end
            ],
            State
        ),
    {NewMachine, NewState}.

-spec check_machine_version(mg:id(), mg_storage:machine(), state()) ->
    ok | no_return().
check_machine_version(ID, Machine, State) ->
    case do_get_machine(ID, State) of
        DBMachine when DBMachine =:= Machine ->
            ok;
        DBMachine ->
            exit({machine_version_mismatch, DBMachine, Machine})
    end.

-spec do_add_events(mg:id(), [mg:event()], state()) ->
    state().
do_add_events(ID, NewMachineEvents, State=#{events:=Events}) ->
    MachineEvents = maps:get(ID, Events, []),
    do_store_events(ID, MachineEvents ++ NewMachineEvents, State).

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
    State#{tags:=Tags#{Tag => ID}}.

-spec do_resolve_tag(mg:tag(), state()) ->
    mg:id() | undefined.
do_resolve_tag(Tag, #{tags:=Tags}) ->
    try
        maps:get(Tag, Tags)
    catch error:{badkey, Tag} ->
        undefined
    end.

-spec do_store_machine(mg:id(), mg_storage:machine(), state()) ->
    state().
do_store_machine(ID, Machine, State=#{machines:=Machines}) ->
    State#{machines:=maps:put(ID, Machine, Machines)}.

-spec do_store_events(mg:id(), [mg:event()], state()) ->
    state().
do_store_events(ID, MachineEvents, State=#{events:=Events}) ->
    State#{events:=maps:put(ID, MachineEvents, Events)}.

-spec try_set_timer(mg:id(), mg_storage:status(), state()) ->
    ok.
try_set_timer(ID, {working, TimerDateTime}, #{options:=Options})
    when TimerDateTime =/= undefined ->
    mg_timers:set(Options, ID, TimerDateTime);
try_set_timer(ID, _, #{options:=Options}) ->
    mg_timers:cancel(Options, ID).



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
    History;
apply_direction(backward, History) ->
    lists:reverse(History).

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
