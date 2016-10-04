-module(mg_automaton_client).

%% API
-export_type([options/0]).
-export([start      /3]).
-export([repair     /3]).
-export([call       /3]).
-export([get_history/3]).

%%
%% API
%%
-type options() :: URL::string().

-spec start(options(), mg:id(), mg:args()) ->
    mg:id().
start({BaseURL, NS}, ID, Args) ->
    call_service(BaseURL, 'Start', [NS, ID, Args]).

-spec repair(options(), mg:ref(), mg:args()) ->
    ok.
repair({BaseURL, NS}, Ref, Args) ->
    call_service(BaseURL, 'Repair', [NS, Ref, Args]).

-spec call(options(), mg:ref(), mg:args()) ->
    mg:call_resp().
call({BaseURL, NS}, Ref, Args) ->
    call_service(BaseURL, 'Call', [NS, Ref, Args]).

-spec get_history(options(), mg:ref(), mg:history_range()) ->
    mg:history().
get_history({BaseURL, NS}, Ref, Range) ->
    call_service(BaseURL, 'GetHistory', [NS, Ref, Range]).

%%
%% local
%%
-spec call_service(_BaseURL, atom(), [_arg]) ->
    _.
call_service(BaseURL, Function, Args) ->
    try
        {R, _} =
            woody_client:call(
                woody_client:new_context(
                    woody_client:make_id(erlang:atom_to_binary(?MODULE, utf8)),
                    mg_woody_api_event_handler
                ),
                {{mg_proto_state_processing_thrift, 'Automaton'}, Function, Args},
                #{url => BaseURL ++ "/v1/automaton"}
            ),
        R
    catch throw:{{exception, Exception}, _} ->
        throw(Exception)
    end.
