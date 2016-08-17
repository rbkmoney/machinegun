%%%
%%% Базовое поведение для хранилища данных машин.
%%% Каждая пишушая операция должна быть атомарной, это важно.
%%% DB заведует таймерами.
%%%
%%% TODO:
%%%  - переименовать в storage
%%%  - переделать на add_tag/resolve_tag
%%%  - переделать на add_event/read_history
%%%  - как-то странно тут выглядят таймеры, их бы сделать более явно и понятно или вынести
%%%  - нужно разделить на ожидаемые и неожидаемые ошибки
%%%
-module(mg_storage).

%% API
-export_type([status       /0]).
-export_type([machine      /0]).
-export_type([timer_handler/0]).

-export_type([error       /0]).
-export_type([thrown_error/0]).

-export([child_spec    /2]).
-export([create_machine/3]).
-export([get_machine   /3]).
-export([update_machine/4]).
-export([resolve_tag   /2]).

-export([throw_error   /1]).

%%
%% API
%%
-type status       () :: {created, mg:args()} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type machine      () :: {mg:id(), status(), mg:history(), [mg:tag()]}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-type error       () :: term().
-type thrown_error() :: {db, error()}.

%%

-callback child_spec(_Options, atom()) ->
    supervisor:child_spec().

-callback create_machine(_Options, mg:id(), _Args) ->
    ok.

-callback get_machine(_Options, mg:id(), mg:history_range() | undefined) ->
    machine().

-callback update_machine(_Options, Old::machine(), New::machine(), timer_handler()) ->
    ok.

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id().

%%

-spec child_spec(_Options, atom()) ->
    supervisor:child_spec().
child_spec(Options, Name) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Name]).

-spec create_machine(_Options, mg:id(), _Args) ->
    ok.
create_machine(Options, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create_machine, [ID, Args]).

-spec get_machine(_Options, mg:id(), mg:history_range() | undefined) ->
    machine().
get_machine(Options, ID, Range) ->
    mg_utils:apply_mod_opts(Options, get_machine, [ID, Range]).

-spec update_machine(_Options, Old::machine(), New::machine(), timer_handler()) ->
    ok.
update_machine(Options, OldMachine, NewMachine, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, update_machine, [OldMachine, NewMachine, TimerHandler]).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    mg_utils:apply_mod_opts(Options, resolve_tag, [Tag]).


%% все ошибки из модулей с поведением mg_storage должны кидаться через эту функцию
-spec throw_error(error()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({db, Error}).
