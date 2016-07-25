-module(mg_machine_db_test).
-behaviour(mg_machine_db).
-behaviour(supervisor).

%% supervisor callbacks
-export([init/1]).

%% mg_persist_machine_db callbacks
-export([child_spec/2, start_link/1, create_machine/2, get_machine/2, update_machine/4,
    resolve_tag/2, remove_machine/2]).

%%
%% supervisor callbacks
%%
-spec init(_Options) ->
    mg_utils:supervisor_ret().
init(Options) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_machine_db_test_server:child_spec(server, Options),
        mg_timers:child_spec(timers, timers) % TODO fix name
    ]}}.

%%
%% mg_persist_machine_db callbacks
%%
-spec child_spec(atom(), _Args) ->
    supervisor:child_spec().
child_spec(ChildID, Args) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Args]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(_Options) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

-spec create_machine(_Args, _MachineArgs) ->
    _ID.
create_machine(Args, MachineArgs) ->
    mg_machine_db_test_server:create_machine(Args, MachineArgs).

-spec get_machine(_Args, _ID) ->
    mg_machine_db:machine().
get_machine(Args, ID) ->
    mg_machine_db_test_server:get_machine(Args, ID).

-spec update_machine(_Args, mg_machine_db:machine(), mg_machine_db:machine(), mg_machine_db:timer_handler()) ->
    ok.
update_machine(Args, OldMachine, NewMachine, TimerHandler) ->
    mg_machine_db_test_server:update_machine(Args, OldMachine, NewMachine, TimerHandler).

%% TODO not_found
-spec resolve_tag(_Args, _Tag) ->
    _ID.
resolve_tag(Args, Tag) ->
    mg_machine_db_test_server:resolve_tag(Args, Tag).

-spec remove_machine(_Args, _ID) ->
    ok.
remove_machine(Args, ID) ->
    mg_machine_db_test_server:remove_machine(Args, ID).
