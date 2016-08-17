-module(mg_storage_test_server).
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export_type([options/0]).
-export([child_spec/2, start_link/1, create_machine/3, get_machine/3, update_machine/4, resolve_tag/2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type options() :: _Name::atom().

-spec child_spec(atom(), options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    gen_server:start_link(self_reg_name(Options), ?MODULE, Options, []).

%%

-spec create_machine(options(), mg:id(), mg:args()) ->
    ok.
create_machine(Options, ID, Args) ->
    throw_if_error(gen_server:call(self_ref(Options), {create_machine, ID, Args})).

-spec get_machine(options(), mg:id(), mg:history_range() | undefined) ->
    mg_storage:machine().
get_machine(Options, ID, Range) ->
    throw_if_error(gen_server:call(self_ref(Options), {get_machine, ID, Range})).

-spec update_machine(options(), mg_storage:machine(), mg_storage:machine(), mg_storage:timer_handler()) ->
    ok.
update_machine(Options, _OldMachine, NewMachine, TimerHandler) ->
    throw_if_error(gen_server:call(self_ref(Options), {update_machine, NewMachine, TimerHandler})).

-spec resolve_tag(options(), mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    throw_if_error(gen_server:call(self_ref(Options), {resolve_tag, Tag})).

%%
%% gen_server callbacks
%%
-type state() :: #{
    machines => #{mg:id() => mg_storage:machine()}
}.

-spec init(options()) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    {ok,
        #{
            machines => #{},
            options  => Options
        }
    }.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({create_machine, ID, Args}, _From, State) ->
    {Resp, NewState} = do_create_machine(ID, Args, State),
    {reply, Resp, NewState};
handle_call({get_machine, ID, Range}, _From, State) ->
    Resp = do_get_machine(ID, Range, State),
    {reply, Resp, State};
handle_call({update_machine, NewMachine, TimerHandler}, _From, State) ->
    {Resp, NewState} = do_update_machine(NewMachine, TimerHandler, State),
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


-spec do_create_machine(mg:id(), mg:args(), state()) ->
    {ok | {error, machine_already_exist}, state()}.
do_create_machine(ID, Args, State=#{machines:=Machines}) ->
    case maps:is_key(ID, Machines) of
        false ->
            {ok, do_store_machine({ID, {created, Args}, [], []}, State)};
        true ->
            {{error, machine_already_exist}, State}
    end.

-spec do_get_machine(mg:id(), mg:history_range(), state()) ->
    {ok, mg_storage:machine()} | {error, machine_not_found}.
do_get_machine(ID, Range, #{machines:=Machines}) ->
    case maps:get(ID, Machines, machine_not_found) of
        Machine = {_, _, _, _} ->
            {ok, filter_machine_history(Machine, Range)};
        machine_not_found ->
            {error, machine_not_found}
    end.

-spec do_update_machine(mg_storage:machine(), mg_storage:timer_handler(), state()) ->
    {ok, mg:id()} | {error, machine_not_found}.
do_update_machine(NewMachine={ID, _, _, _}, TimerHandler, State=#{machines:=Machines, options:=Options}) ->
    case maps:is_key(ID, Machines) of
        true ->
            ok = try_set_timer(Options, NewMachine, TimerHandler),
            {ok, do_store_machine(NewMachine, State)};
        false ->
            {{error, machine_not_found}, State}
    end.

-spec do_resolve_tag(mg:tag(), state()) ->
    {ok, mg:id()} | {error, machine_not_found}.
do_resolve_tag(Tag, #{machines:=Machines}) ->
    find_tag(Tag, maps:to_list(Machines)).

-spec do_store_machine(mg_storage:machine(), state()) ->
    state().
do_store_machine(Machine={ID, _, _, _}, State=#{machines:=Machines}) ->
    State#{machines:=maps:put(ID, Machine, Machines)}.

-spec try_set_timer(options(), mg_storage:machine(), mg_storage:timer_handler()) ->
    ok.
try_set_timer(Options, {ID, {working, TimerDateTime}, _, _}, TimerHandler) when TimerDateTime =/= undefined ->
    mg_timers:set(Options, ID, TimerDateTime, TimerHandler);
try_set_timer(_Options, {_, _, _, _}, _) ->
    ok.

-spec find_tag(mg:tag(), list(mg_storage:machine())) ->
    {ok, mg:id()} | {error, machine_not_found}.
find_tag(_, []) ->
    {error, machine_not_found};
find_tag(Tag, [{ID, {ID, _, _, Tags}}|RemainMachines]) ->
    case lists:member(Tag, Tags) of
        true ->
            {ok, ID};
        false ->
            find_tag(Tag, RemainMachines)
    end.

%%
%% history filtering
%%
-spec filter_machine_history(mg_storage:machine(), mg:history_range() | undefined) ->
    mg_storage:machine().
filter_machine_history(Machine, undefined) ->
    Machine;
filter_machine_history({ID, Status, History, Tags}, {After, Limit, Direction}) ->
    {ID, Status, filter_history(apply_direction(Direction, History), After, Limit), Tags}.

-spec apply_direction(mg:direction(), mg:history()) ->
    mg:history().
apply_direction(forward, History) ->
    lists:reverse(History);
apply_direction(backward, History) ->
    History.

-spec filter_history(mg:history(), mg:event_id() | undefined, pos_integer()) ->
    mg:history().
filter_history(History, After, Limit) ->
    lists:reverse(filter_history_iter(lists:reverse(History), After, Limit, [])).

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
