-module(mg_storage_test_server).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-export_type([options/0]).
-behaviour(mg_storage).
-export([child_spec/3, create/3, get_status/2, update_status/3, add_events/3, get_history/3, add_tag/3, resolve_tag/2]).

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

-spec create(_Options, mg:id(), _Args) ->
    ok.
create(Options, ID, Args) ->
    throw_if_error(gen_server:call(self_ref(Options), {create, ID, Args})).

-spec get_status(_Options, mg:id()) ->
    mg_storage:status().
get_status(Options, ID) ->
    throw_if_error(gen_server:call(self_ref(Options), {get_status, ID})).

-spec update_status(_Options, mg:id(), mg_storage:status()) ->
    ok.
update_status(Options, ID, Status) ->
    throw_if_error(gen_server:call(self_ref(Options), {update_status, ID, Status})).

-spec add_events(_Options, mg:id(), [mg:event()]) ->
    ok.
add_events(Options, ID, Events) ->
    throw_if_error(gen_server:call(self_ref(Options), {add_events, ID, Events})).

-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    throw_if_error(gen_server:call(self_ref(Options), {get_history, ID, Range})).

-spec add_tag(_Options, mg:id(), mg:tag()) ->
    ok.
add_tag(Options, ID, Tag) ->
    throw_if_error(gen_server:call(self_ref(Options), {add_tag, ID, Tag})).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    throw_if_error(gen_server:call(self_ref(Options), {resolve_tag, Tag})).


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
    {Resp, NewState} = do_create(ID, Args, State),
    {reply, Resp, NewState};
handle_call({get_status, ID}, _From, State) ->
    Resp = do_get_status(ID, State),
    {reply, Resp, State};
handle_call({update_status, ID, Status}, _From, State) ->
    {Resp, NewState} = do_update_status(ID, Status, State),
    {reply, Resp, NewState};
handle_call({add_events, ID, Events}, _From, State) ->
    {Resp, NewState} = do_add_events(ID, Events, State),
    {reply, Resp, NewState};
handle_call({get_history, ID, Range}, _From, State) ->
    Resp = do_get_history(ID, Range, State),
    {reply, Resp, State};
handle_call({add_tag, ID, Tag}, _From, State) ->
    {Resp, NewState} = do_add_tag(ID, Tag, State),
    {reply, Resp, NewState};
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

-spec throw_if_error(ok | {ok, V} | {error, _}) ->
    ok | V | no_return().
throw_if_error(ok) ->
    ok;
throw_if_error({ok, V}) ->
    V;
throw_if_error({error, Error}) ->
    mg_storage:throw_error(Error).


-spec do_create(mg:id(), mg:args(), state()) ->
    {ok | {error, machine_already_exist}, state()}.
do_create(ID, Args, State=#{machines:=Machines}) ->
    case maps:is_key(ID, Machines) of
        false ->
            {ok, do_store_events(ID, [], do_store_machine(ID, {created, Args}, State))};
        true ->
            {{error, machine_already_exist}, State}
    end.

-spec do_get_status(mg:id(), state()) ->
    {ok, mg_storage:status()} | {error, machine_not_found}.
do_get_status(ID, #{machines:=Machines}) ->
    try
        {ok, maps:get(ID, Machines)}
    catch error:{badkey, ID} ->
        {error, machine_not_found}
    end.

-spec do_update_status(mg:id(), mg_storage:status(), state()) ->
    {ok | {error, machine_not_found}, state()}.
do_update_status(ID, Status, State=#{machines:=Machines}) ->
    case maps:is_key(ID, Machines) of
        true ->
            ok = try_set_timer(ID, Status, State),
            {ok, do_store_machine(ID, Status, State)};
        false ->
            {{error, machine_not_found}, State}
    end.

-spec do_add_events(mg:id(), [mg:event()], state()) ->
    {ok | {error, machine_not_found}, state()}.
do_add_events(ID, NewMachineEvents, State=#{events:=Events}) ->
    case maps:get(ID, Events, undefined) of
        undefined ->
            {{error, machine_not_found}, State};
        MachineEvents ->
            {ok, do_store_events(ID, NewMachineEvents ++ MachineEvents, State)}
    end.

-spec do_get_history(mg:id(), mg:history_range(), state()) ->
    {ok, mg:history()} | {error, machine_not_found}.
do_get_history(ID, Range, #{events:=Events}) ->
    case maps:get(ID, Events, undefined) of
        undefined ->
            {error, machine_not_found};
        MachineEvents ->
            {ok, filter_history(MachineEvents, Range)}
    end.

-spec do_add_tag(mg:id(), mg:tag(), state()) ->
    {ok | {error, tag_already_exist}, state()}.
do_add_tag(ID, Tag, State=#{tags:=Tags}) ->
    case maps:is_key(Tag, Tags) of
        false ->
            {ok, State#{tags:=maps:put(Tag, ID, Tags)}};
        true ->
            {{error, tag_already_exist}, State}
    end.

-spec do_resolve_tag(mg:tag(), state()) ->
    {ok, mg:id()} | {error, machine_not_found}.
do_resolve_tag(Tag, #{tags:=Tags}) ->
    try
        {ok, maps:get(Tag, Tags)}
    catch error:{badkey, Tag} ->
        {error, machine_not_found}
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
