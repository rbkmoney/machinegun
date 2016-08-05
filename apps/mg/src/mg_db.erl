-module(mg_db).

%% API
-export_type([status       /0]).
-export_type([machine      /0]).
-export_type([timer_handler/0]).

%%
%% API
%%
%% DB заведует таймерами (?)
-type status       () :: {created, mg:args()} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type machine      () :: {mg:id(), mg:status(), mg:history(), [mg:tag()]}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-callback child_spec(atom(), _Options) ->
    supervisor:child_spec().

-callback create_machine(_Options, mg:id(), _MachineArgs) ->
    % Тут должна гарантироваться атомарность! (?)
    ok.

-callback get_machine(_Options, mg:id()) ->
    machine().

-callback update_machine(_Options, Old::machine(), New::machine(), timer_handler()) ->
    ok.

-callback resolve_tag(_Options, mg:tag()) ->
    mg:id().
