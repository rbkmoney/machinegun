-module(mg_machine_tags).

%% API
-export_type([options/0]).
-export([child_spec  /2]).
-export([add_tag     /3]).
-export([resolve_tag /2]).

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/2]).

-define(all_history, {undefined, undefined, forward}).


-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_storage:storage()
}.

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_machine:child_spec(machine_options(Options), ChildID).

-spec add_tag(options(), mg:tag(), mg:id()) ->
    ok | {already_exists, mg:id()}.
add_tag(Options, Tag, MachineID) ->
    % TODO подумать об ошибках тут
    mg_machine:call_with_lazy_start(machine_options(Options), Tag, {add_tag, MachineID}, ?all_history, undefined).

-spec resolve_tag(options(), mg:tag()) ->
    mg:id() | undefined.
resolve_tag(Options, Tag) ->
    #{history:=History} =
        mg_machine:get_machine_with_lazy_start(machine_options(Options), Tag, ?all_history, undefined),
    do_resolve_tag(fold_history(History)).

%%
%% mg_processor handler
%%
-spec process_signal(_, mg:signal_args()) ->
    mg:signal_result().
process_signal(_, _) ->
    {{null, []}, #{}}.

-spec process_call(_, mg:call_args()) ->
    mg:call_result().
process_call(_, {{add_tag, MachineID}, #{id:=SelfID, history:=History}}) ->
    case do_resolve_tag(fold_history(History)) of
        undefined ->
            {ok, {null, [pack_event(generate_add_tag_event(MachineID))]}, #{}};
        SelfID ->
            {ok, {null, []}, #{}};
        OtherMachineID ->
            {{already_exists, OtherMachineID}, {null, []}, #{}}
    end.

%%
%% local
%%
-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{namespace:=Namespace, storage:=Storage}) ->
    #{
        namespace => Namespace,
        processor => ?MODULE,
        storage   => Storage
    }.

%%
%% functions with state
%%
-type state() :: mg:id() | undefined.
-type event() :: {add, mg:id()}.

-spec fold_history(mg:history()) ->
    state().
fold_history(History) ->
    apply_events(unpack_events([EventBody || #{body := EventBody} <- History]), undefined).

-spec apply_events([event()], state()) ->
    state().
apply_events(History, State) ->
    lists:foldl(fun apply_event/2, State, History).

-spec apply_event(event(), state()) ->
    state().
apply_event({add, MachineID}, undefined) ->
    MachineID.

-spec do_resolve_tag(state()) ->
    mg:id() | undefined.
do_resolve_tag(MachineID) ->
    MachineID.

-spec generate_add_tag_event(mg:id()) ->
    event().
generate_add_tag_event(ID) ->
    {add, ID}.

%%
%% packer to opaque
%%
-spec pack_event(event()) ->
    mg:event_body().
pack_event({add, MachineID}) ->
    [<<"add">>, MachineID].

-spec unpack_event(mg:event_body()) ->
    event().
unpack_event([<<"add">>, MachineID]) ->
    {add, MachineID}.

-spec unpack_events([mg:event_body()]) ->
    [event()].
unpack_events(History) ->
    lists:map(fun unpack_event/1, History).
