-module(mg_storage_test).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export([child_spec/3, get_status/2, get_history/3, resolve_tag/2, update/5]).

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

-spec get_status(_Options, mg:id()) ->
    mg_storage:status() | undefined.
get_status(Options, ID) ->
    mg_storage_test_server:get_status(Options, ID).

-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    mg_storage_test_server:get_history(Options, ID, Range).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    mg_storage_test_server:resolve_tag(Options, Tag).

-spec update(_Options, mg:id(), mg_storage:status(), [mg:event()], mg:tag()) ->
    ok.
update(Options, ID, Status, Events, Tag) ->
    mg_storage_test_server:update(Options, ID, Status, Events, Tag).
