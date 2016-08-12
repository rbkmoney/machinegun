-module(mg_db_test_server).

-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([options/0]).
-export([child_spec/2, start_link/1, create_machine/3, get_machine/3, update_machine/4, resolve_tag/2]).

%% gen_server callbacks
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
    % тут не должно быть рейсов
    ok.
create_machine(Options, ID, Args) ->
    insert_machine(make_ets_name(Options), {ID, {created, Args}, [], []}).

-spec get_machine(options(), mg:id(), mg:history_range() | undefined) ->
    mg_db:machine().
get_machine(Options, ID, Range) ->
    filter_machine_history(read_machine(make_ets_name(Options), ID), Range).

-spec update_machine(options(), mg_db:machine(), mg_db:machine(), mg_db:timer_handler()) ->
    ok.
update_machine(Options, _OldMachine, NewMachine, TimerHandler) ->
    write_machine(make_ets_name(Options), NewMachine),
    try_set_timer(Options, NewMachine, TimerHandler).


-spec try_set_timer(options(), mg_db:machine(), mg_db:timer_handler()) ->
    ok.
try_set_timer(Options, {ID, {working, TimerDateTime}, _, _}, TimerHandler) when TimerDateTime =/= undefined ->
    mg_timers:set(Options, ID, TimerDateTime, TimerHandler);
try_set_timer(_Options, {_, _, _, _}, _) ->
    ok.


-spec resolve_tag(options(), mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    ID = ets:foldl(
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
        ),
    case ID of
        undefined -> mg_db:throw_error(not_found);
        _         -> ID
    end.

%%
%% gen_server callbacks
%%
-type state() :: #{}.

-spec init(options()) ->
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
-spec self_reg_name(options()) ->
    mg_utils:gen_reg_name().
self_reg_name(Name) ->
    {local, wrap_name(Name)}.

-spec make_ets_name(options()) ->
    atom().
make_ets_name(Name) ->
    wrap_name(Name).

-spec wrap_name(atom()) ->
    atom().
wrap_name(Name) ->
    erlang:list_to_atom(?MODULE_STRING ++ "_" ++ erlang:atom_to_list(Name)).

-spec read_machine(atom(), mg:id()) ->
    mg_db:machine().
read_machine(ETS, ID) ->
    case ets:lookup(ETS, ID) of
        [Machine] ->
            Machine;
        [] ->
            mg_db:throw_error(not_found)
    end.

-spec insert_machine(atom(), mg_db:machine()) ->
    ok.
insert_machine(ETS, Machine) ->
    case ets:insert_new(ETS, [Machine]) of
        true  -> ok;
        false -> mg_db:throw_error(already_exist)
    end.

-spec write_machine(atom(), mg_db:machine()) ->
    ok.
write_machine(ETS, Machine={ID, _, _, _}) ->
    case ets:member(ETS, ID) of
        true ->
            true = ets:insert(ETS, [Machine]),
            ok;
        false ->
            mg_db:throw_error(not_found)
    end.

%%
%% history filtering
%%
-spec filter_machine_history(mg_db:machine(), mg:history_range() | undefined) ->
    mg_db:machine().
filter_machine_history(Machine, undefined) ->
    Machine;
filter_machine_history({ID, Status, History, Tags}, #'HistoryRange'{'after'=After, limit=Limit}) ->
    {ID, Status, filter_history(History, After, Limit), Tags}.

-spec filter_history(mg:history(), mg:event_id() | undefined, pos_integer()) ->
    mg:history().
filter_history(History, After, Limit) ->
    filter_history_iter(lists:reverse(History), After, Limit, []).

-spec filter_history_iter(mg:history(), mg:event_id() | undefined, non_neg_integer(), mg:history()) ->
    mg:history().
filter_history_iter([], _, _, Result) ->
    Result;
filter_history_iter(_, _, 0, Result) ->
    Result;
filter_history_iter([Event|HistoryTail], undefined, Limit, Result) ->
    filter_history_iter(HistoryTail, undefined, decrease_limit(Limit), [Event|Result]);
filter_history_iter([#'Event'{id=ID}|HistoryTail], After, Limit, []) when ID =:= After ->
    filter_history_iter(HistoryTail, undefined, Limit, []);
filter_history_iter([_|HistoryTail], After, Limit, []) ->
    filter_history_iter(HistoryTail, After, Limit, []).

-spec decrease_limit(undefined | pos_integer()) ->
    non_neg_integer().
decrease_limit(undefined) ->
    undefined;
decrease_limit(N) ->
    N - 1.
