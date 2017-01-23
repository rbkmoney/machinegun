-module(mg_automaton_client).

%% API
-export_type([options/0]).
-export([start          /3]).
-export([repair         /3]).
-export([call           /3]).
-export([call_with_retry/4]).
-export([get_machine    /3]).

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
    call_service(BaseURL, 'Repair', [machine_desc(NS, Ref), Args]).

-spec call(options(), mg:ref(), mg:args()) ->
    mg:call_resp().
call({BaseURL, NS}, Ref, Args) ->
    call_service(BaseURL, 'Call', [machine_desc(NS, Ref), Args]).

-spec call_with_retry(options(), mg:ref(), mg:args(), genlib_retry:strategy()) ->
    mg:call_resp().
call_with_retry({BaseURL, NS}, Ref, Args, Strategy) ->
    call_service(BaseURL, 'Call', [machine_desc(NS, Ref), Args], Strategy).

-spec get_machine(options(), mg:ref(), mg:history_range()) ->
    mg:machine().
get_machine({BaseURL, NS}, Ref, Range) ->
    call_service(BaseURL, 'GetMachine', [machine_desc(NS, Ref, Range)]).

%%
%% local
%%
-spec machine_desc(mg:ns(), mg:ref()) ->
    _.
machine_desc(NS, Ref) ->
    machine_desc(NS, Ref, {undefined, undefined, forward}).

-spec machine_desc(mg:ns(), mg:ref(), mg:history_range()) ->
    _.
machine_desc(NS, Ref, HRange) ->
    mg_woody_api_packer:pack(machine_descriptor, {NS, Ref, HRange}).

-spec woody_call(_BaseURL, atom(), [_arg]) ->
    _.
woody_call(BaseURL, Function, Args) ->
    woody_client:call(
            {{mg_proto_state_processing_thrift, 'Automaton'}, Function, Args},
            #{
                url           => BaseURL ++ "/v1/automaton",
                event_handler => {mg_woody_api_event_handler, undefined}
            },
            woody_context:new()
    ).

-spec call_service(_BaseURL, atom(), [_arg]) ->
    _.
call_service(BaseURL, Function, Args) ->
    WR = woody_call(BaseURL, Function, Args),

    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    end.

-spec call_service(_BaseURL, atom(), [_arg], genlib_retry:strategy()) ->
    _.
call_service(BaseURL, Function, Args, Strategy) ->
    WR = woody_call(BaseURL, Function, Args),

    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NewStrategy} ->
                    ok = timer:sleep(Timeout),
                    call_service(BaseURL, Function, Args, NewStrategy);
                finish ->
                    erlang:throw(Exception)
            end
    end.
