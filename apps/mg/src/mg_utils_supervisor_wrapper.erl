-module(mg_utils_supervisor_wrapper).

%% API
-export([start_link/2]).
-export([start_link/3]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

-export_type([supervisor_child_spec/0]).
-type supervisor_child_spec() ::
      supervisor:child_spec()
    | empty_child_spec
.

-type filter_func() :: fun((_Elem) -> boolean()).

%% API
-spec start_link(supervisor:sup_flags(), [supervisor_child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(Flags, ChildsSpecs) ->
    supervisor:start_link(?MODULE, {Flags, ChildsSpecs}).

-spec start_link(mg_utils:gen_reg_name(), supervisor:sup_flags(), [supervisor_child_spec()]) ->
    mg_utils:gen_start_ret().
start_link(RegName, Flags, ChildsSpecs) ->
    supervisor:start_link(RegName, ?MODULE, {Flags, ChildsSpecs}).

%%
%% supervisor callbacks
%%
-spec init({supervisor:sup_flags(), [supervisor_child_spec()]}) ->
    mg_utils:supervisor_ret().
init({Flags, ChildsSpecs}) ->
    {ok, {Flags, filter_child_specs(filter_func(), ChildsSpecs)}}.

-spec filter_child_specs(filter_func(), list(supervisor_child_spec())) ->
    list(supervisor_child_spec()).
filter_child_specs(FilterFunc, ChildSpecs) ->
    lists:filter(FilterFunc, ChildSpecs).

-spec filter_func() ->
    filter_func().
filter_func() ->
    fun
        (empty_child_spec) ->
            false;
        (_) ->
            true
    end.
