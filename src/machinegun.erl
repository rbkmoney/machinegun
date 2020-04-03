-module(machinegun).

%% API
-export([start/0]).
-export([stop /0]).

%% application callbacks
-behaviour(application).
-export([start/2]).
-export([stop /1]).

-spec start() ->
    {ok, _}.
start() ->
    application:ensure_all_started(?MODULE).

-spec stop() ->
    ok.
stop() ->
    application:stop(?MODULE).

-spec start(_, _) ->
    {ok, pid()}.
start(_StartType, _StartArgs) ->
    Config = maps:from_list(genlib_app:env(?MODULE)),
    ChildSpecs = machinegun_configurator:construct_child_specs(Config),
    mg_core_utils_supervisor_wrapper:start_link(
        {local, ?MODULE},
        #{strategy => rest_for_one},
        ChildSpecs
    ).

-spec stop(any()) ->
    ok.
stop(_State) ->
    ok.
