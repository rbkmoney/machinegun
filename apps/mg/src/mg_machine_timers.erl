%%%
%%% TODO:
%%%  - переделать на шардированную схему
%%%  - переделать схему хранения таймеров в памяти
%%%
-module(mg_machine_timers).

%% API
-export_type([options/0]).
-export([child_spec    /2]).
-export([set_timer     /3]).
-export([cancel_timer  /2]).
-export([handle_timeout/1]).
-export([handle_timeout/2]).

%% internal API
-export([start_link/1]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/2]).

%%
%% API
%%
-type timer_handler() :: {module(), atom(), [_Arg]}.
-type options() :: #{
    namespace     => mg:ns(),
    storage       => mg_storage:storage(),
    timer_handler => timer_handler()
}.

-type id() :: mg:id().

%% пока это синглтон, но везде фигурирует TimersID, чтобы потом было проще
-define(TIMERS_ID, <<"main">>).
-define(all_history, {undefined, undefined, forward}).

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec set_timer(options(), mg:id(), calendar:datetime()) ->
    ok.
set_timer(Options, MachineID, DateTime) ->
    % TODO подумать об ошибках тут
    ok = mg_machine:call_with_lazy_start(
            machine_options(Options), ?TIMERS_ID, {set_timer, MachineID, DateTime}, ?all_history, undefined
        ).

-spec cancel_timer(options(), mg:id()) ->
    ok.
cancel_timer(Options, MachineID) ->
    ok = mg_machine:call_with_lazy_start(
            machine_options(Options), ?TIMERS_ID, {cancel_timer, MachineID}, ?all_history, undefined
        ).

-spec handle_timeout(options()) ->
    ok.
handle_timeout(Options) ->
    ok = handle_timeout(Options, ?TIMERS_ID).

-spec handle_timeout(options(), id()) ->
    ok.
handle_timeout(Options, TimersID) ->
    ok = mg_machine:call_with_lazy_start(Options, TimersID, handle_timeout, ?all_history, undefined).

%%
%% internal API
%%
-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

%%
%% supervisor callbacks
%%
-spec init(options()) ->
    mg_utils:supervisor_ret().
init(Options=#{namespace:=Namespace}) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_machine:child_spec(machine_options(Options), machines),
        mg_timers :child_spec(timers, Namespace, {?MODULE, handle_timeout, [Options]})
    ]}}.

%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    {{undefined, []}, #{}}.

-spec process_call(mg:ns(), mg:call_args()) ->
    mg:call_result().
process_call(Options, {Call, #{id:=TimersID, history:=History}}) ->
    State = fold_history(History),
    CallEvents = process_call_(Options, Call, State),
    ok = refresh_global_timer(Options, TimersID, get_next_timer(apply_events(CallEvents, State))),
    {ok, {undefined, CallEvents}, #{}}.

-spec process_call_(options(), _, state()) ->
    [event()].
process_call_(Options, handle_timeout, State) ->
    handle_ready_timers(Options, State);
process_call_(_, {set_timer, MachineID, DateTime}, _) ->
    [{set, MachineID, DateTime}];
process_call_(_, {cancel_timer, MachineID}, _) ->
    [{cancel, MachineID}].


%%
%% local
%%
-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options) ->
    Options#{
        processor => {?MODULE, Options}
    }.

%%
%% functions with state
%%
-type event() ::
      {set     , mg:id(), calendar:datetime()}
    | {cancel  , mg:id()}
    | {complete, mg:id()}
.
-type state() :: #{mg:id() => calendar:datetime()}.

-spec handle_ready_timers(options(), state()) ->
    [event()].
handle_ready_timers(Options, State) ->
    lists:foldl(
        fun(Timer, Acc) ->
            Acc ++ handle_ready_timer(Options, Timer)
        end,
        [],
        fetch_ready_timers(State)
    ).

-spec fetch_ready_timers(state()) ->
    [{mg:id(), calendar:datetime()}].
fetch_ready_timers(State) ->
    Now = calendar:universal_time(),
    % TODO уменьшить сложность
    maps:fold(
        fun
            (MachineID, DateTime, Acc) when DateTime =< Now ->
                [{MachineID, DateTime}|Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        State
    ).

-spec handle_ready_timer(options(), {mg:id(), calendar:datetime()}) ->
    [event()].
handle_ready_timer(Options, {ID, _}) ->
    _ = apply_hanlder(Options, ID),
    [{complete, ID}].

-spec refresh_global_timer(options(), id(), {mg:id(), calendar:datetime()} | undefined) ->
    ok.
refresh_global_timer(#{namespace:=Namespace}, TimersID, undefined) ->
    mg_timers:cancel(Namespace, TimersID);
refresh_global_timer(#{namespace:=Namespace}, TimersID, {_, DateTime}) ->
    mg_timers:set(Namespace, TimersID, DateTime).

-spec get_next_timer(state()) ->
    {mg:id(), calendar:datetime()} | undefined.
get_next_timer(State) ->
    % TODO
    % на данный момент всё плохо, и это действие имеет линейную сложность
    % нужно будет переделать на схему аналогичную mg_timers
    maps:fold(
        fun
            (MachineID, DateTime, undefined) ->
                {MachineID, DateTime};
            (MachineID, DateTime, {NextDateTime, _}) when DateTime < NextDateTime ->
                {MachineID, DateTime};
            (_, _, Acc) ->
                Acc
        end,
        undefined,
        State
    ).

-spec fold_history([event()]) ->
    state().
fold_history(History) ->
    apply_events([Event || #{body:=Event} <- History], #{}).

-spec apply_events([event()], state()) ->
    state().
apply_events(Events, State) ->
    lists:foldl(fun apply_event/2, State, Events).

-spec apply_event(event(), state()) ->
    state().
apply_event({set, MachineID, DateTime}, State) ->
    State#{MachineID => DateTime};
apply_event({cancel, MachineID}, State) ->
    maps:remove(MachineID, State);
apply_event({complete, MachineID}, State) ->
    maps:remove(MachineID, State).

-spec apply_hanlder(options(), mg:id()) ->
    _.
apply_hanlder(#{timer_handler:={M, F, A}=MFA}, ID) ->
    % TODO тут косяк, если хэндлер упадёт, его никто не вызовет снова
    % при транзиентных ошибках будет аналогично, нужно чинить
    erlang:spawn(
        fun() ->
            try
                erlang:apply(M, F, A ++ [ID])
            catch Class:Reason ->
                Exception = {Class, Reason, erlang:get_stacktrace()},
                ok = error_logger:error_msg("unexpected error while applying handler:~n~p~n~p", [MFA, Exception])
            end
        end
    ).
