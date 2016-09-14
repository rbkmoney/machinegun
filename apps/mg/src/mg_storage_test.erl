-module(mg_storage_test).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

%% internal API
-export([start_link/3]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export([child_spec/4, create_machine/4, get_machine/3, get_history/5, resolve_tag/3, update_machine/5]).

-type options() :: mg_storage_test_server:options().

%%
%% supervisor callbacks
%%
-spec init({options(), mg:ns(), mg_storage:timer_handler()}) ->
    mg_utils:supervisor_ret().
init({Options, Namespace, TimerHandler}) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_storage_test_server:child_spec(Options, Namespace, server),
        mg_timers             :child_spec(timers, Namespace, TimerHandler)
    ]}}.

%%
%% internal API
%%
-spec start_link(options(), mg:ns(), mg_storage:timer_handler()) ->
    mg_utils:gen_start_ret().
start_link(Options, Namespace, TimerHandler) ->
    supervisor:start_link(?MODULE, {Options, Namespace, TimerHandler}).

%%
%% mg_storage callbacks
%%
-spec child_spec(options(), mg:ns(), atom(), mg_storage:timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, ChildID, TimerHandler) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, Namespace, TimerHandler]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, Namespace, ID, Args) ->
    mg_storage_test_server:create_machine(Options, Namespace, ID, Args).

-spec get_machine(options(), mg:ns(), mg:id()) ->
    mg_storage:machine() | undefined.
get_machine(Options, Namespace, ID) ->
    mg_storage_test_server:get_machine(Options, Namespace, ID).

-spec get_history(options(), mg:ns(), mg:id(), mg_storage:machine(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, Namespace, ID, Machine, Range) ->
    mg_storage_test_server:get_history(Options, Namespace, ID, Machine, Range).

-spec resolve_tag(options(), mg:ns(), mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Namespace, Tag) ->
    mg_storage_test_server:resolve_tag(Options, Namespace, Tag).

-spec update_machine(options(), mg:ns(), mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
update_machine(Options, Namespace, ID, Machine, Update) ->
    mg_storage_test_server:update_machine(Options, Namespace, ID, Machine, Update).
