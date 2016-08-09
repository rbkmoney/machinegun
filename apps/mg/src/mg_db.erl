%%% TODO Пересмотреть ещё раз, продумать и записать требуемые гарантии когда дойдёт дело до работы с БД
%%% Тут должна гарантироваться атомарность! (?)
%%% DB заведует таймерами (?)
-module(mg_db).

%% API
-export_type([status       /0]).
-export_type([machine      /0]).
-export_type([timer_handler/0]).

-export([child_spec    /2]).
-export([create_machine/3]).
-export([get_machine   /3]).
-export([update_machine/4]).
-export([resolve_tag   /2]).

%%
%% API
%%
-type status       () :: {created, mg:args()} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type machine      () :: {mg:id(), mg:status(), mg:history(), [mg:tag()]}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

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
