%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_events_modernizer).

-export_type([options/0]).
-export_type([machine_event/0]).
-export_type([modernized_event_body/0]).

-export([modernize_machine/5]).

%%

-type options() :: #{
    current_format_version := mg_events:format_version(),
    handler                := mg_utils:mod_opts(handler_opts())
}.

-type handler_opts()    :: term(). % handler specific
-type request_context() :: term(). % handler specific

-type machine_event() :: #{
    ns    => mg:ns(),
    id    => mg:id(),
    event => mg_events:event()
}.

-type modernized_event_body() :: mg_events:body().

-callback modernize_event(handler_opts(), request_context(), machine_event()) ->
    modernized_event_body().

%%

-type ref()           :: mg_events_machine:ref().
-type history_range() :: mg_events:history_range().

-spec modernize_machine(options(), mg_events_machine:options(), request_context(), ref(), history_range()) ->
    ok.
modernize_machine(Options, EventsMachineOptions, ReqCtx, Ref, HRange) ->
    #{ns := NS, id := ID, history := History} =
        mg_events_machine:get_machine(EventsMachineOptions, Ref, HRange),
    OutdatedHistory = filter_outdated_history(Options, History),
    lists:foreach(
        fun (Event) ->
            ModernizedBody = call_handler(Options, ReqCtx, event_to_machine_event(NS, ID, Event)),
            case update_event(Event, ModernizedBody) of
                Event ->
                    ok;
                ModernizedEvent ->
                    store_event(EventsMachineOptions, ID, ModernizedEvent)
            end
        end,
        OutdatedHistory
    ).

-spec update_event(mg_events:event(), modernized_event_body()) ->
    mg_events:event().
update_event(Event = #{body := Body}, ModernizedBody) ->
    case Versions = {get_format_version(Body), get_format_version(ModernizedBody)} of
        {undefined, _} ->
            % _Любое_ обновлённое представление данных, не имевших версии, достойно лечь в базу.
            Event#{body := ModernizedBody};
        {VersionWas, Version} when is_integer(Version), Version > VersionWas ->
            % Обновлённое представление данных c более старшей версией достойно лечь в базу.
            Event#{body := ModernizedBody};
        {VersionWas, VersionWas} ->
            % Неизменное представление данных, проще пропустить. Отдельно обрабатываем подобный случай для
            % сценариев, когда модернизатор ещё не обновился и не знает, как обновить данные; в таком
            % случае ему пожалуй будет проще вернуть их в неизменном виде.
            Event;
        _ ->
            % Обновлённое представление проверсионированных данных c более младшей версией или даже без неё.
            % Это нарушение протокола, лучше вылететь с ошибкой?
            erlang:throw({logic, {invalid_modernized_version, Versions}})
    end.

-spec store_event(mg_events_machine:options(), mg:id(), mg_events:event()) ->
    ok.
store_event(Options, ID, Event) ->
    {Key, Value} = mg_events:add_machine_id(ID, mg_events:event_to_kv(Event)),
    mg_storage:put(events_storage_options(Options), Key, undefined, Value, []).

-spec filter_outdated_history(options(), [mg_events:event()]) ->
    [mg_events:event()].
filter_outdated_history(Options, History) ->
    lists:filter(fun (Event) -> is_outdated_event(Options, Event) end, History).

-spec is_outdated_event(options(), mg_events:event()) ->
    boolean().
is_outdated_event(#{current_format_version := Current}, #{body := Body}) ->
    case get_format_version(Body) of
        undefined ->
            % Данные, не содержащие хоть какой-то версии данных, в любом случае _устаревшие_.
            true;
        Version ->
            Current > Version
    end.

-spec get_format_version(mg_events:content()) ->
    mg_events:format_version() | undefined.
get_format_version({Metadata, _}) ->
    maps:get(format_version, Metadata, undefined).

-spec event_to_machine_event(mg:ns(), mg:id(), mg_events:event()) ->
    machine_event().
event_to_machine_event(NS, ID, Event) ->
    #{ns => NS, id => ID, event => Event}.

-spec call_handler(options(), request_context(), machine_event()) ->
    modernized_event_body().
call_handler(#{handler := Handler}, ReqCtx, MachineEvent) ->
    % TODO обработка ошибок?
    mg_utils:apply_mod_opts(Handler, modernize_event, [ReqCtx, MachineEvent]).

%%
%% options manipulation
%%
%% TODO
%% На самом деле это кусок имплементации mg_events_machine, такого быть не должно. Возможно стоит
%% пересмотреть граф зависимостей, например выделить mg_events_storage в виде отдельного модуля.

-spec events_storage_options(mg_events_machine:options()) ->
    mg_storage:options().
events_storage_options(#{namespace := NS, events_storage := StorageOptions}) ->
    {Mod, Options} = mg_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, mg_events_machine, events_storage}, namespace => NS}}.
