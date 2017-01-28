-module(mg_woody_api_automaton).

%% API
-export_type([options/0]).
-export([handler/1]).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: #{mg:ns() => mg_events_machine:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/automaton", {{mg_proto_state_processing_thrift, 'Automaton'}, {?MODULE, Options}}}.

%%
%% woody handler
%%


-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('Start', [NS, ID, Args], WoodyContext, Options) ->
    ok = mg_woody_api_utils:handle_safe(
            fun() ->
                mg_events_machine:start(
                    get_ns_options(NS, Options),
                    unpack(id, ID),
                    {unpack(args, Args), WoodyContext}
                )
            end
        ),
    {ok, ok};

handle_function('Repair', [MachineDesc, Args], WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    ok = mg_woody_api_utils:handle_safe(
            fun() ->
                mg_events_machine:repair(
                    get_ns_options(NS, Options),
                    Ref,
                    {unpack(args, Args), WoodyContext},
                    Range
                )
            end
        ),
    {ok, ok};

handle_function('Call', [MachineDesc, Args], WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Response =
        mg_woody_api_utils:handle_safe(
            % блин, как же в эрланге не хватает каррирования... :-\
            fun() ->
                mg_events_machine:call(
                    get_ns_options(NS, Options),
                    Ref,
                    {unpack(args, Args), WoodyContext},
                    Range
                )
            end
        ),
    {ok, pack(call_response, Response)};

handle_function('GetMachine', [MachineDesc], _WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    History =
        mg_woody_api_utils:handle_safe(
            fun() ->
                mg_events_machine:get_machine(get_ns_options(NS, Options), Ref, Range)
            end
        ),
    {ok, pack(machine, History)}.

%%
%% local
%%
-spec get_ns_options(mg:ns(), options()) ->
    mg_events_machine:options().
get_ns_options(Namespace, Options) ->
    try
        maps:get(Namespace, Options)
    catch
        error:{badkey, Namespace} ->
            throw(namespace_not_found)
    end.
