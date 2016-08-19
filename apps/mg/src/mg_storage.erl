%%%
%%% Базовое поведение для хранилища данных машин.
%%% Каждая пишушая операция должна быть атомарной, это важно.
%%% storage заведует таймерами.
%%%
%%% TODO:
%%%  - нужно разделить на ожидаемые и неожидаемые ошибки
%%%
-module(mg_storage).

%% API
-export_type([status       /0]).
-export_type([timer_handler/0]).

-export([child_spec /3]).
-export([get_status /2]).
-export([get_history/3]).
-export([resolve_tag/2]).
-export([update     /5]).

%%
%% API
%%
-type status() ::
      {created, mg:args()}
    | {working, calendar:datetime() | undefined}
    | {error, _Reason}
.
-type timer_handler() :: {module(), atom(), [_Arg]}.

%%

-callback child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().

-callback get_status(_Options, mg:id()) ->
    status() | undefined.

-callback get_history(_Options, mg:id(), mg:history_range()) ->
    mg:history().

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id() | undefined.

-callback update(_Options, mg:id(), status(), [mg:event()], mg:tag()) ->
    ok.

%%

-spec child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, Name, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Name, TimerHandler]).

%% Если машины нет, то возвращает undefined
-spec get_status(_Options, mg:id()) ->
    status() | undefined.
get_status(Options, ID) ->
    mg_utils:apply_mod_opts(Options, get_status, [ID]).

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

-spec update(_Options, mg:id(), status(), [mg:event()], mg:tag()) ->
    ok.
update(Options, ID, Status, Events, Tag) ->
    mg_utils:apply_mod_opts(Options, update, [ID, Status, Events, Tag]).
