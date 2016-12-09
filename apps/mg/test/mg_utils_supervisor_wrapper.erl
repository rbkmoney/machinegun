-module(mg_utils_supervisor_wrapper).

%% API
-export([start_link/2]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% API
-spec start_link(supervisor:sup_flags(), [supervisor:child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({supervisor:sup_flags(), [supervisor:child_spec()]}) ->
    mg_utils:supervisor_ret().
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, ChildsSpecs}}.
