-module(mg_woody_api_utils).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export([handle_safe/1]).

%%
%% API
%%
-spec handle_safe(fun(() -> R)) ->
    R.
handle_safe(F) ->
    try
        F()
    catch throw:Error ->
        case map_error(Error) of
            {throw, NewError} ->
                erlang:throw(NewError);
            {woody_error, {WoodyErrorClass, Description}} ->
                BinaryDescription = erlang:list_to_binary(io_lib:format("~9999p", [Description])),
                woody_error:raise(system, {internal, WoodyErrorClass, BinaryDescription})
        end
    end.

-spec map_error(mg_machine:thrown_error() | namespace_not_found) ->
    {throw, _} | {woody_error, {woody_error:class(), _Details}}.
map_error(machine_not_found    ) -> {throw, #'MachineNotFound'     {}};
map_error(machine_already_exist) -> {throw, #'MachineAlreadyExists'{}};
map_error(machine_failed       ) -> {throw, #'MachineFailed'       {}};
map_error(namespace_not_found  ) -> {throw, #'NamespaceNotFound'   {}};
map_error(event_sink_not_found ) -> {throw, #'EventSinkNotFound'   {}};

% может Reason не прокидывать дальше?
map_error({transient, Reason}) -> {woody_error, {resource_unavailable, Reason}};
map_error({timeout  , Reason}) -> {woody_error, {result_unexpected   , Reason}};

map_error(UnknownError) -> erlang:error(badarg, [UnknownError]).
