-module(mg_automaton_client).

%% API
-export_type([options/0]).
-export([start      /3]).
-export([repair     /3]).
-export([call       /3]).
-export([get_machine/3]).

%%
%% API
%%
-type options() :: #{
    url => URL::string(),
    ns  => mg:ns(),
    retry_strategy => genlib_retry:strategy() | undefined
}.

-spec start(options(), mg:id(), mg:args()) ->
    mg:id().
start(#{url := BaseURL, ns := NS, retry_strategy := Strategy}, ID, Args) ->
    call_service(BaseURL, 'Start', [NS, ID, Args], Strategy).

-spec repair(options(), mg:ref(), mg:args()) ->
    ok.
repair(#{url := BaseURL, ns := NS, retry_strategy := Strategy}, Ref, Args) ->
    call_service(BaseURL, 'Repair', [machine_desc(NS, Ref), Args], Strategy).

-spec call(options(), mg:ref(), mg:args()) ->
    mg:call_resp().
call(#{url := BaseURL, ns := NS, retry_strategy := Strategy}, Ref, Args) ->
    call_service(BaseURL, 'Call', [machine_desc(NS, Ref), Args], Strategy).

-spec get_machine(options(), mg:ref(), mg:history_range()) ->
    mg:machine().
get_machine(#{url := BaseURL, ns := NS, retry_strategy := Strategy}, Ref, Range) ->
    call_service(BaseURL, 'GetMachine', [machine_desc(NS, Ref, Range)], Strategy).

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

-spec call_service(_BaseURL, atom(), [_Arg], genlib_retry:strategy() | undefined) ->
    _.
call_service(BaseURL, Function, Args, undefined) ->
    WR = woody_call(BaseURL, Function, Args),

    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    end;
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

-spec woody_call(_BaseURL, atom(), [_Arg]) ->
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
