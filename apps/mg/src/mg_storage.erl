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

-export_type([error       /0]).
-export_type([thrown_error/0]).

-export([child_spec /3]).
-export([create     /3]).
-export([get_status /2]).
-export([get_history/3]).
-export([resolve_tag/2]).
-export([update     /5]).

-export([throw_error/1]).

%%
%% API
%%
-type status       () :: {created, mg:args()} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-type error       () :: term().
-type thrown_error() :: {storage, error()}.

%%

-callback child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().

-callback create(_Options, mg:id(), _Args) ->
    ok.

-callback get_status(_Options, mg:id()) ->
    status().

-callback get_history(_Options, mg:id(), mg:history_range()) ->
    mg:history().

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id().

-callback update(_Options, mg:id(), status(), [mg:event()], mg:tag()) ->
    ok.

%%

-spec child_spec(_Options, atom(), timer_handler()) ->
    supervisor:child_spec().
child_spec(Options, Name, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Name, TimerHandler]).

-spec create(_Options, mg:id(), _Args) ->
    ok.
create(Options, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create, [ID, Args]).

-spec get_status(_Options, mg:id()) ->
    status().
get_status(Options, ID) ->
    mg_utils:apply_mod_opts(Options, get_status, [ID]).

-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    mg_utils:apply_mod_opts(Options, get_history, [ID, Range]).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    mg_utils:apply_mod_opts(Options, resolve_tag, [Tag]).

-spec update(_Options, mg:id(), status(), [mg:event()], mg:tag()) ->
    ok.
update(Options, ID, Status, Events, Tag) ->
    mg_utils:apply_mod_opts(Options, update, [ID, Status, Events, Tag]).


%% все ошибки из модулей с поведением mg_storage должны кидаться через эту функцию
-spec throw_error(error()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({storage, Error}).
