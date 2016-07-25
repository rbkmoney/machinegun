-module(mg_machine_db).

%% API
-export_type([event_id     /0]).
-export_type([tags         /0]).
-export_type([history      /0]).
-export_type([status       /0]).
-export_type([machine      /0]).
-export_type([timer_handler/0]).

%%
%% API
%%
%% DB заведует таймерами (?)
-type event_id     () :: pos_integer().
-type tags         () :: [_Tag].
-type history      () :: #{event_id() => _Event}.
-type status       () :: {created, _MachineArgs} | {working, calendar:datetime() | undefined} | {error, _Reason}.
-type machine      () :: {_ID, status(), history(), tags()}.
-type timer_handler() :: {module(), atom(), [_Arg]}.

-callback child_spec(atom(), _Options) ->
    supervisor:child_spec().

-callback create_machine(_Options, _MachineArgs) ->
    % Тут должна гарантироваться атомарность!
    _ID.

-callback get_machine(_Options, _ID) ->
    machine().

-callback update_machine(_Options, Old::machine(), New::machine(), timer_handler()) ->
    ok.

-callback resolve_tag(_Options, _Tag) ->
    _ID.

% TODO
% -callback select_working(_Options, ) ->
%     [_ID].

% -callback destroy_machine(_ID) ->
%     ok.
