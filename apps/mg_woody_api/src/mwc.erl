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

%%%
%%% Модуль для работы с mg из консоли, чтобы не писать длинные команды.
%%% Сюда можно (нужно) добавлять всё, что понадобится в нелёгкой жизни девопса.
%%%
-module(mwc).

%% API
-export_type([scalar/0]).

-export([get_statuses_distrib  /1]).
-export([simple_repair         /2]).
-export([simple_repair         /3]).
-export([resume_interrupted_one/2]).
-export([kill                  /2]).
-export([get_failed_machines   /1]).
-export([get_machine           /2]).
-export([get_events_machine    /2]).
-export([get_events_machine    /3]).

-export([m_opts /1]).
-export([em_opts/1]).

%%
%% API
%%
-type scalar() :: string() | atom() | binary() | number().

% получение распределения по статусам
-spec get_statuses_distrib(scalar()) ->
    [{atom(), non_neg_integer()}].
get_statuses_distrib(Namespace) ->
    [
        {StatusQuery, status_count(Namespace, StatusQuery)}
        || StatusQuery <- mg_machine:all_statuses()
    ].

-spec status_count(scalar(), mg_machine:search_query()) ->
    non_neg_integer().
status_count(Namespace, StatusQuery) ->
    Result = mg_machine:search(m_opts(Namespace), StatusQuery),
    erlang:length(Result).

% восстановление машины
-spec simple_repair(scalar(), scalar()) ->
    woody_context:ctx() | no_return().
simple_repair(Namespace, ID) ->
    simple_repair(Namespace, ID, mg_deadline:default()).

-spec simple_repair(scalar(), scalar(), mg_deadline:deadline()) ->
    woody_context:ctx() | no_return().
simple_repair(Namespace, ID, Deadline) ->
    WoodyCtx = woody_context:new(),
    ok = mg_machine:simple_repair(
            m_opts(Namespace),
            id(ID),
            mg_woody_api_utils:woody_context_to_opaque(WoodyCtx),
            Deadline
        ),
    WoodyCtx.

-spec resume_interrupted_one(scalar(), scalar()) ->
    ok | no_return().
resume_interrupted_one(Namespace, ID) ->
    ok = mg_machine:resume_interrupted(
        m_opts(Namespace),
        id(ID),
        mg_deadline:from_timeout(5000)
    ).

% убийство машины
-spec kill(scalar(), scalar()) ->
    ok.
kill(Namespace, ID) ->
    ok = mg_workers_manager:brutal_kill(mg_machine:manager_options(m_opts(Namespace)), id(ID)).

-spec get_failed_machines(mg:ns()) ->
    [{mg:id(), Reason::term()}].
get_failed_machines(Namespace) ->
    Options = m_opts(Namespace),
    [
        {ID, Reason}
    ||
        {ID, {error, Reason, _}} <-
            [{ID, mg_machine:get_status(Options, ID)} || ID <- mg_machine:search(Options, failed)]
    ].

% посмотреть стейт машины
-spec get_machine(scalar(), scalar()) ->
    mg_machine:machine_state().
get_machine(Namespace, ID) ->
    mg_machine:get(m_opts(Namespace), id(ID)).

-spec get_events_machine(scalar(), mg_events_machine:ref()) ->
    mg_events_machine:machine().
get_events_machine(Namespace, Ref) ->
    get_events_machine(Namespace, Ref, {undefined, undefined, forward}).

-spec get_events_machine(scalar(), mg_events_machine:ref(), mg_events:history_range()) ->
    mg_events_machine:machine().
get_events_machine(Namespace, Ref, HRange) ->
    mg_events_machine:get_machine(em_opts(Namespace), Ref, HRange).

%%

-spec em_opts(scalar()) ->
    mg_events_machine:options().
em_opts(Namespace) ->
    mg_woody_api:events_machine_options(
        ns(Namespace),
        application:get_all_env(mg_woody_api)
    ).

-spec m_opts(scalar()) ->
    mg_machine:options().
m_opts(Namespace) ->
    mg_woody_api:machine_options(ns(Namespace), ns_config(Namespace)).

-spec ns_config(scalar()) ->
    _Config.
ns_config(Namespace) ->
    maps:get(ns(Namespace), genlib_app:env(mg_woody_api, namespaces)).

-spec ns(scalar()) ->
    mg:ns().
ns(Namespace) ->
    genlib:to_binary(Namespace).

-spec id(scalar()) ->
    mg:id().
id(ID) ->
    genlib:to_binary(ID).
