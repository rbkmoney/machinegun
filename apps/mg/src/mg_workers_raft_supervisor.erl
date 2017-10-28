%%%
%%% Супервизор, который запускает процесс сразу на всех элементах кластера.
%%% Не будет нормально работать с simple_one_for_one, т.к. использует функцию stop,
%%% которая выполняет одновременно terminate_child и delete_child
%%%
%%% TODO:
%%%  - timeouts
%%%
-module(mg_workers_raft_supervisor).

%% API
-export([start_link   /3]).
-export([start_child  /2]).
-export([stop_child   /2]).
-export([get_childspec/2]).
-export([is_started   /2]).

%% raft
-behaviour(raft).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, handle_info/3, apply_delta/4]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft_utils:gen_reg_name(), raft:options()) ->
    raft_utils:gen_start_ret().
start_link(RaftRegName, SupRegName, RaftOptions) ->
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        [
            #{
                id       => supervisor,
                start    => {mg_utils_supervisor_wrapper, start_link, [SupRegName, #{strategy => one_for_one}, []]},
                restart  => permanent,
                type     => supervisor
            },
            #{
                id       => raft_server,
                start    => {raft, start_link, [RaftRegName, {?MODULE, gen_reg_name_to_ref(SupRegName)}, RaftOptions]},
                restart  => permanent,
                type     => worker
            }
        ]
    ).

-spec start_child(raft:options(), supervisor:child_spec()) ->
    ok | {error, already_started}.
start_child(#{rpc := RPC, cluster := Cluster}, ChildSpec) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {start_child, ChildSpec},
        genlib_retry:linear(10, 100)
    ).

-spec stop_child(raft:options(), _ID) ->
    ok | {error, not_found}.
stop_child(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {stop_child, ID},
        genlib_retry:linear(10, 100)
    ).

-spec get_childspec(raft:options(), _ID) ->
    {ok, supervisor:child_spec()} | {error, not_found}.
get_childspec(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {get_childspec, ID},
        genlib_retry:linear(10, 100)
    ).

-spec is_started(raft:options(), _ID) ->
    boolean().
is_started(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {is_started, ID},
        genlib_retry:linear(10, 100)
    ).


%%
%% raft
%%
-type async_command() :: {get_childspec, _ID} | {is_started, _ID}.
-type sync_command () :: {start_child, supervisor:child_spec()} | {stop_child, _ID}.
-type state() :: undefined.
-type delta() :: sync_command().

-spec init(_) ->
    state().
init(_) ->
    undefined.

-spec handle_election(_, state()) ->
    {undefined, state()}.
handle_election(_, State) ->
    {undefined, State}.

-spec handle_async_command(raft_utils:gen_ref(), raft_rpc:request_id(), async_command(), state()) ->
    {raft:reply_action(), state()}.
handle_async_command(SupRef, _, {get_childspec, ID}, State) ->
    {reply, supervisor:get_childspec(SupRef, ID), State};
handle_async_command(SupRef, _, {is_started, ID}, State) ->
    Reply =
        case supervisor:get_childspec(SupRef, ID) of
            {ok   , _        } -> true;
            {error, not_found} -> false
        end,
    {{reply, Reply}, State}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft:reply_action(), delta() | undefined, state()}.
handle_command(SupRef, _, {start_child, ChildSpec = #{id := ID}}, State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, {error, already_present}}, undefined, State};
        {error, not_found} ->
            {{reply, ok}, {start_child, ChildSpec}, State}
    end;
handle_command(SupRef, _, {stop_child, ID}, State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, ok}, {stop_child, ID}, State};
        Error = {error, not_found} ->
            {{reply, Error}, undefined, State}
    end.

-spec handle_info(_, _Info, state()) ->
    {undefined, state()}.
handle_info(_, Info, State) ->
    ok = error_logger:error_msg("unexpected info received: ~p", [Info]),
    {undefined, State}.

-spec apply_delta(raft_utils:gen_ref(), raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(SupRef, _, {start_child, ChildSpec}, State) ->
    {ok, _} = supervisor:start_child(SupRef, ChildSpec),
    State;
apply_delta(SupRef, _, {stop_child, ID}, State) ->
    ok = supervisor:terminate_child(SupRef, ID),
    ok = supervisor:delete_child   (SupRef, ID),
    State.

%%

-spec
gen_reg_name_to_ref(raft_utils:gen_reg_name()) -> raft_utils:gen_ref().
gen_reg_name_to_ref({local, Name}            ) -> Name;
gen_reg_name_to_ref(Global = {global, _}     ) -> Global;
gen_reg_name_to_ref(Via    = {via, _, _}     ) -> Via.
