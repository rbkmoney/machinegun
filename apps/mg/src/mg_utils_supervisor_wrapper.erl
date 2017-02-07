-module(mg_utils_supervisor_wrapper).

%% API
-export([start_link/2]).
-export([start_link/3]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% API
-spec start_link(supervisor:sup_flags(), [supervisor:child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

-spec start_link(mg_utils:gen_reg_name(), supervisor:sup_flags(), [supervisor:child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(RegName, Flags, ChildsSpecs) ->
    supervisor:start_link(RegName, ?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({supervisor:sup_flags(), [supervisor:child_spec()]}) ->
    mg_utils:supervisor_ret().
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, ChildsSpecs}}.
