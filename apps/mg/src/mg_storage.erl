%%%
%%% Базовое поведение для хранилища данных машин.
%%% Каждая пишушая операция должна быть атомарной, это важно.
%%% Со своей стороны mg гарантирует, что не будет 2х одновременных пишуших запросов для одной машины.
%%%
%%% storage заведует таймерами.
%%%
%%% Про работу с ошибками написано в mg_machine.
%%%
-module(mg_storage).

%% API
-export_type([status       /0]).
-export_type([events_range /0]).
-export_type([machine      /0]).
-export_type([update       /0]).
-export_type([options      /0]).
-export_type([storage      /0]).

-export([child_spec    /3]).
-export([create_machine/4]).
-export([get_machine   /3]).
-export([get_history   /5]).
-export([update_machine/5]).

%%
%% API
%%
-type status() ::
      {created, mg:args()}
    |  working
    | {error  , _Reason}
.

%% не очень удобно, что получилось 2 формата range'а
%% надо подумать, как это исправить
-type events_range() :: {First::mg:event_id(), Last::mg:event_id()} | undefined.

-type machine() :: #{
    status       => status(),
    aux_state    => mg:aux_state(),
    events_range => events_range(),
    db_state     => _ % опционально
}.

%% все поля опциональны
-type update() :: #{
    status     => status(),
    aux_state  => mg:aux_state(),
    new_events => [mg:event()]
}.

-type options() :: term().
-type storage() :: mg_utils:mod_opts(options()).

%%

-callback child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().

-callback create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().

-callback get_machine(options(), mg:ns(), mg:id()) ->
    machine() | undefined.

-callback get_history(options(), mg:ns(), mg:id(), machine(), mg:history_range()) ->
    mg:history().

-callback update_machine(options(), mg:ns(), mg:id(), machine(), update()) ->
    machine().

%%

-spec child_spec(storage(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, ChildID) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Namespace, ChildID]).

-spec create_machine(storage(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, Namespace, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create_machine, [Namespace, ID, Args]).

%% Если машины нет, то возвращает undefined
-spec get_machine(storage(), mg:ns(), mg:id()) ->
    machine() | undefined.
get_machine(Options, Namespace, ID) ->
    mg_utils:apply_mod_opts(Options, get_machine, [Namespace, ID]).

%% Если машины нет, то возвращает пустой список
-spec get_history(storage(), mg:ns(), mg:id(), machine(), mg:history_range()) ->
    mg:history().
get_history(Options, Namespace, ID, Machine, Range) ->
    mg_utils:apply_mod_opts(Options, get_history, [Namespace, ID, Machine, Range]).

-spec update_machine(storage(), mg:ns(), mg:id(), machine(), update()) ->
    machine().
update_machine(Options, Namespace, ID, Machine, Update) ->
    mg_utils:apply_mod_opts(Options, update_machine, [Namespace, ID, Machine, Update]).
