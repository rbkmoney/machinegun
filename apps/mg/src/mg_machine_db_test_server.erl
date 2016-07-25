-module(mg_machine_db_test_server).

-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([child_spec/2, start_link/1, create_machine/2, get_machine/2, update_machine/4,
    resolve_tag/2, remove_machine/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% mg_persist_machine_db callbacks
%%
-spec child_spec(atom(), _Options) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(_Options) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    gen_server:start_link(self_reg_name(Options), ?MODULE, Options, []).

%%

-spec create_machine(_Options, _MachineArgs) ->
    % тут не должно быть рейсов
    _ID.
create_machine(Options, MachineArgs) ->
    ID = erlang:make_ref(),
    ok = write_machine(make_ets_name(Options), {ID, {created, MachineArgs}, #{}, []}),
    ID.

-spec get_machine(_Options, _ID) ->
    mg_machine_db:machine().
get_machine(Options, ID) ->
    read_machine(make_ets_name(Options), ID).

-spec update_machine(_Options, mg_machine_db:machine(), mg_machine_db:machine(), mg_machine_db:timer_handler()) ->
    ok.
update_machine(Options, _OldMachine, NewMachine, TimerHandler) ->
    ok = write_machine(make_ets_name(Options), NewMachine),
    try_set_timer(Options, NewMachine, TimerHandler).


-spec try_set_timer(_Options, mg_machine_db:machine(), mg_machine_db:timer_handler()) ->
    ok.
try_set_timer(_Options, {ID, {working, TimerDateTime}, _, _}, TimerHandler) when TimerDateTime =/= undefined ->
    mg_timers:set(timers, ID, TimerDateTime, TimerHandler);
try_set_timer(_Options, {_, _, _, _}, _) ->
    ok.


%% TODO not_found
-spec resolve_tag(_Options, _Tag) ->
    _ID.
resolve_tag(Options, Tag) ->
    ets:foldl(
        fun
            ({ID, _Status, _History, Tags}, undefined) ->
                case lists:member(Tag, Tags) of
                    true  -> ID;
                    false -> undefined
                end;
            (_, Result) ->
                Result
        end,
        undefined,
        make_ets_name(Options)
    ).

-spec remove_machine(_Options, _ID) ->
    ok.
remove_machine(Options, ID) ->
    true = ets:delete(make_ets_name(Options), ID),
    ok.

%%
%% gen_server callbacks
%%
-type state() :: #{}.

-spec init(_Options) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    % ген-сервер только держит ets'ку
    _ = ets:new(make_ets_name(Options), [set, public, named_table]),
    {ok, #{}}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
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
% -spec self_ref(_Options) ->
%     mg_utils:gen_ref().
% self_ref(_Options) ->
%     {local, ?MODULE}.

-spec self_reg_name(_Options) ->
    mg_utils:gen_reg_name().
self_reg_name(_Options) ->
    {local, ?MODULE}.

-spec make_ets_name(_Options) ->
    atom().
make_ets_name(_Options) ->
    ?MODULE.

-spec read_machine(atom(), _ID) ->
    mg_machine_db:machine().
read_machine(ETS, ID) ->
    [Machine] = ets:lookup(ETS, ID),
    Machine.

-spec write_machine(atom(), mg_machine_db:machine()) ->
    ok.
write_machine(ETS, Machine) ->
    true = ets:insert(ETS, [Machine]),
    ok.

%% TODO вариант с разными таблицами
% -spec make_ets_name(states | events | tags, _Options) ->
%     atom().
% make_ets_name(Type, _Options) ->
%     erlang:list_to_atom(erlang:atom_to_list(?MODULE) ++ "_" ++ erlang:atom_to_list(Type)).


% read_state(ID) ->
%     [State] = ets:lookup(make_ets_name(states), ID),
%     State.

% read_tags(ETS, ID, LastTagID) ->
%     Tags = ets:lookup(make_ets_name(tags), ID),
%     [Tag || Tag={ID, _} <- Tags, ID =< LastTagID].

% read_history(ETS, ID, LastEventID) ->
%     Events = ets:lookup(make_ets_name(events), ID),
%     [Event || Event={ID, _} <- Events, ID =< LastEventID].

% -spec get_machine(_Options, _ID) ->
%     mg_machine_db:machine().
% get_machine(Options, ID) ->
%     {Status, LastEventID, LastTagID} = read_state  (ID),
%     Tags                             = read_tags   (ID, LastTagID),
%     History                          = read_history(ID, LastEventID),
%     {Status, History, Tags}.

% -spec update_machine(_Options, mg_machine_db:machine(), mg_machine_db:machine()) ->
%     ok.
% update_machine(Options, OldMachine, NewMachine) ->
%     % TODO найти дельту
%     ok  = write_tags(),
%     ok  = write_history(),
%     ok  = write_state(NewMachine)
%     ok.

% _ = [ets:new(make_ets_name(Type, Options), [set, public, named_table]) || Type <- [states, events, tags]],

