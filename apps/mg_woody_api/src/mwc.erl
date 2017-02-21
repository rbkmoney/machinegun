%%%
%%% Модуль для работы с mg из консоли, чтобы не писать длинные команды.
%%% Сюда можно (нужно) добавлять всё, что понадобится в нелёгкой жизни девопса.
%%%
-module(mwc).

%% API
-export_type([scalar/0]).

-export([get_statuses_distrib/1]).
-export([simple_repair       /2]).
-export([simple_repair       /3]).
-export([touch               /2]).
-export([touch               /3]).
-export([kill                /2]).
-export([get_machine         /2]).
-export([get_events_machine  /3]).
-export([get_db_state        /2]).

%%
%% API
%%
-type scalar() :: string() | atom() | binary() | number().

% получение распределения по статусам
-spec get_statuses_distrib(scalar()) ->
    [{atom(), non_neg_integer()}].
get_statuses_distrib(Namespace) ->
    [
        {StatusQuery, erlang:length(mg_machine:search(machine_options(Namespace), StatusQuery))}
        || StatusQuery <- mg_machine:all_statuses()
    ].

% восстановление машины
-spec simple_repair(scalar(), scalar()) ->
    ok | no_return().
simple_repair(Namespace, ID) ->
    simple_repair(Namespace, ID, mg_utils:default_deadline()).

-spec simple_repair(scalar(), scalar(), mg_utils:deadline()) ->
    ok | no_return().
simple_repair(Namespace, ID, Deadline) ->
    mg_machine:simple_repair(machine_options(Namespace), id(ID), Deadline).


-spec touch(scalar(), scalar()) ->
    ok | no_return().
touch(Namespace, ID) ->
    touch(Namespace, ID, mg_utils:default_deadline()).

-spec touch(scalar(), scalar(), mg_utils:deadline()) ->
    ok | no_return().
touch(Namespace, ID, Deadline) ->
    ok = mg_machine:touch(machine_options(Namespace), id(ID), Deadline).

% убийство машины
-spec kill(scalar(), scalar()) ->
    ok.
kill(Namespace, ID) ->
    ok = mg_workers_manager:brutal_kill(mg_machine:manager_options(machine_options(Namespace)), id(ID)).


% посмотреть стейт машины из процесса и из бд
-spec get_machine(scalar(), scalar()) ->
    mg_machine:machine_state().
get_machine(Namespace, ID) ->
    mg_machine:get(machine_options(Namespace), id(ID)).

-spec get_events_machine(scalar(), mg_events_machine:ref(), mg_events:history_range()) ->
    mg_events_machine:machine().
get_events_machine(Namespace, Ref, HRange) ->
    mg_events_machine:get_machine(machine_options(Namespace), Ref, HRange).

-spec get_db_state(scalar(), scalar()) ->
    mg_storage:value() | undefined.
get_db_state(Namespace, ID) ->
    mg_storage:get(storage_options(Namespace), id(ID)).

%%

-spec machine_options(scalar()) ->
    mg_machine:options().
machine_options(Namespace) ->
    #{
        namespace => ns(Namespace),
        storage   => storage()
    }.

-spec storage_options(mg:ns()) ->
    mg_storage:options().
storage_options(Namespace) ->
    #{
        namespace => Namespace,
        module    => storage()
    }.

-spec storage() ->
    mg_storage:storage().
storage() ->
    genlib_app:env(mg_woody_api, storage).

-spec ns(scalar()) ->
    mg:ns().
ns(Namespace) ->
    genlib:to_binary(Namespace).

-spec id(scalar()) ->
    mg:id().
id(ID) ->
    genlib:to_binary(ID).
