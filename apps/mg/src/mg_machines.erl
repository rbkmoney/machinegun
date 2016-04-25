%%%
%%% Запускаемые по требованию именнованные процессы.
%%% Распределяются по кластеру и реплицируют свой стейт через эвенты.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%%
-module(mg_machines).
-behaviour(supervisor).

%% API
-export([child_spec /4]).
-export([start_link /3]).
-export([call       /3]).
-export([cast       /3]).

%% Supervisor callbacks
-export([init/1]).


%%
%% API
%%
-spec child_spec(atom(), _TypeID, module(), _Args) ->
    supervisor:child_spec().
child_spec(ChildID, TypeID, Mod, Args) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [TypeID, Mod, Args]},
        restart  => permanent,
        type     => supervisor,
        shutdown => brutal_kill
    }.

-spec start_link(_TypeID, module(), _Args) ->
    mg_utils:gen_start_ret().
start_link(TypeID, Mod, Args) ->
    supervisor:start_link(self_reg_name(TypeID), ?MODULE, {Mod, Args}).

-define(START_IF_NEEDED(TypeID, ID, Expr),
    try
        Expr
    catch exit:{noproc,_} ->
        case start_child(TypeID, ID) of
            {ok, _} ->
                Expr;
            {error,{already_started,_}} ->
                Expr
        end
    end
).

% sync
-spec call(_TypeID, _ID, _Call) ->
    _Reply.
call(TypeID, ID, Call) ->
    ?START_IF_NEEDED(TypeID, ID, mg_machine:call(ID, Call)).

% async
-spec cast(_TypeID, _ID, _Cast) ->
    ok.
cast(TypeID, ID, Cast) ->
    ok = ?START_IF_NEEDED(TypeID, ID, mg_machine:cast(ID, Cast)).


%%
%% supervisor callbacks
%%
-spec init({module(), _Args}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({Mod, Args}) ->
    SupFlags = #{strategy => simple_one_for_one},
    {ok, {SupFlags, [mg_machine:child_spec(Mod, Args)]}}.

%%
%% local
%%
-spec start_child(_TypeID, _ID) ->
    {ok, pid()} | {error, term()}.
start_child(TypeID, ID) ->
    supervisor:start_child(self_ref(TypeID), [ID]).


-spec self_ref(_ID) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    {via, gproc, {n, l, {?MODULE, ID}}}.

-spec self_reg_name(_ID) ->
    mg_utils:gen_reg_name().
self_reg_name(ID) ->
    {via, gproc, {n, l, {?MODULE, ID}}}.
