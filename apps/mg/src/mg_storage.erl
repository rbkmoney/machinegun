%%%
%%% Базовое поведение для хранилища данных машин.
%%% Каждая пишушая операция должна быть атомарной, это важно.
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

-export([child_spec    /3]).
-export([create_machine/3]).
-export([get_machine   /2]).
-export([get_history   /3]).
-export([resolve_tag   /2]).
-export([update_machine/4]).

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
    status        => status(),
    last_event_id => mg:event_id(),
    db_state      => _ % опционально
}.

%% все поля опциональны
-type update() :: #{
    last_event_id => mg:event_id(),
    status        => status(),
    new_events    => [mg:event()],
    new_tag       => mg:tag() | undefined
}.

%%

-callback child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().

-callback create_machine(_Options, mg:args(), mg:id()) ->
    mg_storage:machine().

-callback get_machine(_Options, mg:id()) ->
    machine() | undefined.

-callback get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id() | undefined.

-callback update_machine(_Options, mg:id(), machine(), update()) ->
    machine().

%%

-spec child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, Name, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Name, TimerHandler]).

-spec create_machine(_Options, mg:id(), mg:args()) ->
    mg_storage:machine().
create_machine(Options, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create_machine, [ID, Args]).

%% Если машины нет, то возвращает undefined
-spec get_machine(_Options, mg:id()) ->
    machine() | undefined.
get_machine(Options, ID) ->
    mg_utils:apply_mod_opts(Options, get_machine, [ID]).

%% Если машины нет, то возвращает пустой список
-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    mg_utils:apply_mod_opts(Options, get_history, [ID, Range]).

%% Если машины с таким тэгом нет, то возвращает undefined
-spec resolve_tag(_Options, mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    mg_utils:apply_mod_opts(Options, resolve_tag, [Tag]).

-spec update_machine(_Options, mg:id(), machine(), update()) ->
    machine().
update_machine(Options, ID, Machine, Update) ->
    mg_utils:apply_mod_opts(Options, update_machine, [ID, Machine, Update]).
