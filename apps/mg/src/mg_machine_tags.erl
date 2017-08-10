-module(mg_machine_tags).

%% API
-export_type([options/0]).
-export_type([tag    /0]).
-export([child_spec  /2]).
-export([add_tag     /5]).
-export([resolve_tag /2]).

%% mg_machine handler
-behaviour(mg_machine).
-export([process_machine/6]).

-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_storage:options(),
    logger    => mg_machine_logger:handler(),
    retries   => mg_machine:retrying_opt()
}.
-type tag() :: binary().

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_machine:child_spec(machine_options(Options), ChildID).

-spec add_tag(options(), tag(), mg:id(), mg:request_context(), mg_utils:deadline()) ->
    ok | {already_exists, mg:id()} | no_return().
add_tag(Options, Tag, ID, ReqCtx, Deadline) ->
    mg_machine:call_with_lazy_start(machine_options(Options), Tag, {add_tag, ID}, ReqCtx, Deadline, undefined).

-spec resolve_tag(options(), tag()) ->
    mg:id() | undefined | no_return().
resolve_tag(Options, Tag) ->
    try
        opaque_to_state(mg_machine:get(machine_options(Options), Tag))
    catch throw:machine_not_found ->
        undefined
    end.

%%
%% mg_machine handler
%%
-type state() :: mg:id() | undefined.

-spec process_machine(_, mg:id(), mg_machine:processor_impact(), _, _, mg_machine:machine_state()) ->
    mg_machine:processor_result().
process_machine(_, _, {init, undefined}, _, _, _) ->
    {{reply, ok}, sleep, state_to_opaque(undefined)};
process_machine(_, _, {repair, undefined}, _, _, State) ->
    {{reply, ok}, sleep, State};
process_machine(_, _, {call, {add_tag, ID}}, _, _, PackedState) ->
    case opaque_to_state(PackedState) of
        undefined ->
            {{reply, ok}, sleep, state_to_opaque(ID)};
        ID ->
            {{reply, ok}, sleep, PackedState};
        OtherID ->
            {{reply, {already_exists, OtherID}}, sleep, PackedState}
    end.

%%
%% local
%%
-spec machine_options(options()) ->
    mg_machine:options().
machine_options(#{namespace:=Namespace, storage:=Storage, logger := Logger, retries := Retries}) ->
    #{
        namespace => Namespace,
        processor => ?MODULE,
        storage   => Storage,
        logger    => Logger,
        retries   => Retries
    }.

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_storage:opaque().
state_to_opaque(undefined) ->
    [1, null];
state_to_opaque(ID) ->
    [1, ID].

-spec opaque_to_state(mg_storage:opaque()) ->
    state().
opaque_to_state([1, null]) ->
    undefined;
opaque_to_state([1, ID]) ->
    ID.
