-module(mg_storage_test).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-export_type([options/0]).
-export([child_spec/3, create_machine/4, get_machine/3, get_history/5, update_machine/5]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% internal API
%%
-spec start_link(options(), mg:ns()) ->
    mg_utils:gen_start_ret().
start_link(Options, Namespace) ->
    gen_server:start_link(self_reg_name(Namespace), ?MODULE, {Options, Namespace}, []).

%%
%% mg_storage callbacks
%%
-type options() :: _.

-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, Namespace]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(_Options, Namespace, ID, Args) ->
    gen_server:call(self_ref(Namespace), {create_machine, ID, Args}).

-spec get_machine(options(), mg:ns(), mg:id()) ->
    mg_storage:machine() | undefined.
get_machine(_Options, Namespace, ID) ->
    gen_server:call(self_ref(Namespace), {get_machine, ID}).

-spec get_history(options(), mg:ns(), mg:id(), mg_storage:machine(), mg:history_range()) ->
    mg:history().
get_history(_Options, Namespace, ID, Machine, Range) ->
    gen_server:call(self_ref(Namespace), {get_history, ID, Machine, Range}).

-spec update_machine(options(), mg:ns(), mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
update_machine(_Options, Namespace, ID, Machine, Update) ->
    gen_server:call(self_ref(Namespace), {update_machine, ID, Machine, Update}).

%%
%% gen_server callbacks
%%
-type state() :: #{
    namespace => mg:ns(),
    options   => options(),
    machines  => #{mg:id() => mg_storage:machine()},
    events    => #{{mg:id(), mg:event_id()} => mg:event()}
}.

-spec init({options(), mg:ns()}) ->
    mg_utils:gen_server_init_ret(state()).
init({Options, Namespace}) ->
    {ok,
        #{
            namespace => Namespace,
            options   => Options,
            machines  => #{},
            events    => #{}
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
handle_call({get_history, ID, Machine, Range}, _From, State) ->
    Resp = do_get_history(ID, Machine, Range, State),
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
-spec self_ref(mg:ns()) ->
    mg_utils:gen_ref().
self_ref(Namespace) ->
    {via, gproc, gproc_key(Namespace)}.

-spec self_reg_name(mg:ns()) ->
    mg_utils:gen_reg_name().
self_reg_name(Namespace) ->
    {via, gproc, gproc_key(Namespace)}.

-spec gproc_key(mg:ns()) ->
    gproc:key().
gproc_key(Namespace) ->
    {n, l, wrap(Namespace)}.

-spec wrap(_) ->
    term().
wrap(V) ->
    {?MODULE, V}.

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
            status       => {created, Args},
            events_range => undefined,
            db_state     => 1
        },
    NewState = do_store_machine(ID, Machine, State),
    {Machine, NewState}.

-spec do_update_machine(mg:id(), mg_storage:machine(), mg_storage:update(), state()) ->
    {mg_storage:machine(), state()}.
do_update_machine(ID, Machine, Update, State) ->
    ok = check_machine_version(ID, Machine, State),

    OldStatus = maps:get(status, Machine),
    NewStatus = maps:get(status, Update, OldStatus),

    NewMachineEvents = maps:get(new_events, Update, []),

    NewMachine =
        Machine#{
            status       := NewStatus,
            events_range := update_events_range(maps:get(events_range, Machine, undefined), NewMachineEvents),
            db_state     := maps:get(db_state  , Machine) + 1
        },
    NewState =
        do_actions(
            [
                fun(S) -> do_add_events   (ID, NewMachineEvents, S) end,
                fun(S) -> do_store_machine(ID, NewMachine      , S) end
            ],
            State
        ),
    {NewMachine, NewState}.


-spec update_events_range(mg_storage:events_range(), [mg:event()]) ->
    mg_storage:events_range().
update_events_range(OldEventsRange, []) ->
    OldEventsRange;
update_events_range(undefined, [FirstEvent|RemainEvents]) ->
    FirstEventID = get_event_id(FirstEvent),
    update_events_range({FirstEventID, FirstEventID}, RemainEvents);
update_events_range({First, _}, NewMachineEvents) ->
    {First, get_event_id(lists:last(NewMachineEvents))}.

-spec get_event_id(mg:event()) ->
    mg:event_id().
get_event_id(#{id:=ID}) ->
    ID.

-spec check_machine_version(mg:id(), mg_storage:machine(), state()) ->
    ok | no_return().
check_machine_version(ID, Machine, State) ->
    % хотим убедится, что логика правильно работает с экземпляром machine
    case do_get_machine(ID, State) of
        DBMachine when DBMachine =:= Machine ->
            ok;
        DBMachine ->
            exit({machine_version_mismatch, DBMachine, Machine})
    end.

-spec do_add_events(mg:id(), [mg:event()], state()) ->
    state().
do_add_events(ID, NewMachineEvents, State) ->
    lists:foldl(
        fun(MachineEvent, StateAcc) ->
            do_store_event(ID, MachineEvent, StateAcc)
        end,
        State,
        NewMachineEvents
    ).

-spec do_get_history(mg:id(), mg_storage:machine(), mg:history_range(), state()) ->
    mg:history().
do_get_history(ID, Machine, RequestedRange, State=#{events:=Events}) ->
    ok = check_machine_version(ID, Machine, State),
    maps:values(
        maps:with(
            mg_storage_utils:get_machine_events_ids(ID, Machine, RequestedRange),
            Events
        )
    ).

-spec do_store_machine(mg:id(), mg_storage:machine(), state()) ->
    state().
do_store_machine(ID, Machine, State=#{machines:=Machines}) ->
    State#{machines:=maps:put(ID, Machine, Machines)}.

-spec do_store_event(mg:id(), mg:event(), state()) ->
    state().
do_store_event(ID, MachineEvent=#{id:=MachineEventID}, State=#{events:=Events}) ->
    State#{events:=maps:put({ID, MachineEventID}, MachineEvent, Events)}.

-spec do_actions([fun((state()) -> state())], state()) ->
    state().
do_actions([], State) ->
    State;
do_actions([Action|RemainActions], State) ->
    NewState = Action(State),
    do_actions(RemainActions, NewState).
