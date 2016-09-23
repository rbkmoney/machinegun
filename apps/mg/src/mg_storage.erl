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
-export_type([timer_handler/0]).
-export_type([machine      /0]).
-export_type([update       /0]).
-export_type([options      /0]).
-export_type([storage      /0]).

-export([child_spec    /4]).
-export([create_machine/4]).
-export([get_machine   /3]).
-export([get_history   /5]).
-export([resolve_tag   /3]).
-export([update_machine/5]).

%%
%% API
%%
-type status() ::
      {created, mg:args()}
    | {working, calendar:datetime() | undefined}
    | {error  , _Reason}
.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-type machine() :: #{
    status     => status(),
    events_ids => [mg:event_id()],
    % events_ids => {First::mg:event_id(), Last::mg:event_id()},
    db_state   => _ % опционально
}.

%% все поля опциональны
-type update() :: #{
    status        => status(),
    new_events    => [mg:event()],
    new_tag       => mg:tag() | undefined
}.

-type options() :: term().
-type storage() :: mg_utils:mod_opts(options()).

%%

-callback child_spec(options(), mg:ns(), atom(), timer_handler()) ->
    supervisor:child_spec().

-callback create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().

-callback get_machine(options(), mg:ns(), mg:id()) ->
    machine() | undefined.

-callback get_history(options(), mg:ns(), mg:id(), machine(), mg:history_range() | undefined) ->
    mg:history().

-callback resolve_tag(options(), mg:ns(), mg:tag()) ->
    mg:id() | undefined.

-callback update_machine(options(), mg:ns(), mg:id(), machine(), update()) ->
    machine().

%%

-spec child_spec(options(), mg:ns(), atom(), timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, ChildID, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Namespace, ChildID, TimerHandler]).

-spec create_machine(options(), mg:ns(), mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, Namespace, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create_machine, [Namespace, ID, Args]).

%% Если машины нет, то возвращает undefined
-spec get_machine(options(), mg:ns(), mg:id()) ->
    machine() | undefined.
get_machine(Options, Namespace, ID) ->
    mg_utils:apply_mod_opts(Options, get_machine, [Namespace, ID]).

%% Если машины нет, то возвращает пустой список
-spec get_history(options(), mg:ns(), mg:id(), machine(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, Namespace, ID, Machine, Range) ->
    mg_utils:apply_mod_opts(Options, get_history, [Namespace, ID, Machine, Range]).

%% Если машины с таким тэгом нет, то возвращает undefined
-spec resolve_tag(options(), mg:ns(), mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Namespace, Tag) ->
    mg_utils:apply_mod_opts(Options, resolve_tag, [Namespace, Tag]).

-spec update_machine(options(), mg:ns(), mg:id(), machine(), update()) ->
    machine().
update_machine(Options, Namespace, ID, Machine, Update) ->
    mg_utils:apply_mod_opts(Options, update_machine, [Namespace, ID, Machine, Update]).
