-module(mg_db_test).
-behaviour(mg_db).
-behaviour(supervisor).

%% supervisor callbacks
-export([init/1]).

%% mg_persist_machine_db callbacks
-export([child_spec/2, start_link/1, create_machine/3, get_machine/2, update_machine/4,
    resolve_tag/2, remove_machine/2]).

%%
%% supervisor callbacks
%%
-spec init(_Options) ->
    mg_utils:supervisor_ret().
init(Options) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_db_test_server:child_spec(server, Options),
        mg_timers:child_spec(timers, timers) % TODO fix name
    ]}}.

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
    supervisor:start_link(?MODULE, Options).

-spec create_machine(_Options, mg:id(), mg:args()) ->
    ok.
create_machine(Options, ID, Args) ->
    mg_db_test_server:create_machine(Options, ID, Args).

-spec get_machine(_Options, mg:id()) ->
    mg_db:machine().
get_machine(Options, ID) ->
    mg_db_test_server:get_machine(Options, ID).

-spec update_machine(_Options, mg_db:machine(), mg_db:machine(), mg_db:timer_handler()) ->
    ok.
update_machine(Options, OldMachine, NewMachine, TimerHandler) ->
    mg_db_test_server:update_machine(Options, OldMachine, NewMachine, TimerHandler).

%% TODO not_found
-spec resolve_tag(_Options, mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    mg_db_test_server:resolve_tag(Options, Tag).

-spec remove_machine(_Options, mg:id()) ->
    ok.
remove_machine(Options, ID) ->
    mg_db_test_server:remove_machine(Options, ID).
