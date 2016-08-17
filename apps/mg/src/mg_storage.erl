%%%
%%% Базовое поведение для хранилища данных машин.
%%% Каждая пишушая операция должна быть атомарной, это важно.
%%% storage заведует таймерами.
%%%
%%% TODO:
%%%  - переделать на add_event/read_history
%%%  - как-то странно тут выглядят таймеры, их бы сделать более явно и понятно или вынести
%%%  - нужно разделить на ожидаемые и неожидаемые ошибки
%%%
-module(mg_storage).

%% API
-export_type([status       /0]).
-export_type([timer_handler/0]).

-export_type([error       /0]).
-export_type([thrown_error/0]).

-export([child_spec   /2]).
-export([create       /3]).
-export([get_status   /2]).
-export([update_status/4]).
-export([add_events   /3]).
-export([get_history  /3]).
-export([add_tag      /3]).
-export([resolve_tag  /2]).

-export([throw_error  /1]).

%%
%% API
%%
-type status       () :: {created, mg:args()} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-type error       () :: term().
-type thrown_error() :: {storage, error()}.

%%

-callback child_spec(_Options, atom()) ->
    supervisor:child_spec().

-callback create(_Options, mg:id(), _Args) ->
    ok.

-callback get_status(_Options, mg:id()) ->
    status().

-callback update_status(_Options, mg:id(), status(), timer_handler()) ->
    ok.

-callback add_events(_Options, mg:id(), [mg:event()]) ->
    ok.

-callback get_history(_Options, mg:id(), mg:history_range()) ->
    mg:history().

-callback add_tag(_Options, mg:id(), mg:tag()) ->
    ok.

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id().

%%

-spec child_spec(_Options, atom()) ->
    supervisor:child_spec().
child_spec(Options, Name) ->
    mg_utils:apply_mod_opts(Options, child_spec, [Name]).

-spec create(_Options, mg:id(), _Args) ->
    ok.
create(Options, ID, Args) ->
    mg_utils:apply_mod_opts(Options, create, [ID, Args]).

-spec get_status(_Options, mg:id()) ->
    status().
get_status(Options, ID) ->
    mg_utils:apply_mod_opts(Options, get_status, [ID]).

-spec update_status(_Options, mg:id(), status(), timer_handler()) ->
    ok.
update_status(Options, ID, Status, TimerHandler) ->
    mg_utils:apply_mod_opts(Options, update_status, [ID, Status, TimerHandler]).

-spec add_events(_Options, mg:id(), [mg:event()]) ->
    ok.
add_events(Options, ID, Events) ->
    mg_utils:apply_mod_opts(Options, add_events, [ID, Events]).

-spec get_history(_Options, mg:id(), mg:history_range() | undefined) ->
    mg:history().
get_history(Options, ID, Range) ->
    mg_utils:apply_mod_opts(Options, get_history, [ID, Range]).

-spec add_tag(_Options, mg:id(), mg:tag()) ->
    ok.
add_tag(Options, ID, Tag) ->
    mg_utils:apply_mod_opts(Options, add_tag, [ID, Tag]).

-spec resolve_tag(_Options, mg:tag()) ->
    mg:id().
resolve_tag(Options, Tag) ->
    mg_utils:apply_mod_opts(Options, resolve_tag, [Tag]).


%% все ошибки из модулей с поведением mg_storage должны кидаться через эту функцию
-spec throw_error(error()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({storage, Error}).
