-module(mg_storage_test).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export([child_spec/3, create_machine/3, get_machine/2, get_history/3, resolve_tag/2, update_machine/4]).

%%
%% supervisor callbacks
%%
-spec init({_Options, mg_storage:timer_handler()}) ->
    mg_utils:supervisor_ret().
init({Options, TimerHandler}) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_storage_test_server:child_spec(Options, server, TimerHandler),
        mg_timers             :child_spec(timers, Options)
    ]}}.

%%
%% internal API
%%
-spec start_link(_Options, mg_storage:timer_handler()) ->
    mg_utils:gen_start_ret().
start_link(Options, TimerHandler) ->
    supervisor:start_link(?MODULE, {Options, TimerHandler}).

%%
%% mg_storage callbacks
%%
-spec child_spec(_Options, atom(), mg_storage:timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, TimerHandler) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, TimerHandler]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec create_machine(_Options, mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, ID, Args) ->
    mg_storage_test_server:create_machine(Options, ID, Args).

-spec get_machine(_Options, mg:id()) ->
    mg_storage:machine() | undefined.
get_machine(Options, ID) ->
    mg_storage_test_server:get_machine(Options, ID).

-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    mg_storage_test_server:get_history(Options, ID, Range).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    mg_storage_test_server:resolve_tag(Options, Tag).

-spec update_machine(_Options, mg:id(), mg_storage:machine(), mg_storage:update()) ->
    mg_storage:machine().
update_machine(Options, ID, Machine, Update) ->
    mg_storage_test_server:update_machine(Options, ID, Machine, Update).
