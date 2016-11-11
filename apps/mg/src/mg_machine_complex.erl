%%%
%%% "Сложная" машина.
%%% Связывает всё вместе и реализует понятие тэгов и таймеров.
%%%
%%% TODO придумать имя получше
-module(mg_machine_complex).

%% API
-export_type([options/0]).

-export([child_spec /2]).

-export([start      /3]).
-export([repair     /4]).
-export([call       /4]).
-export([get_machine/3]).

%% internal API
-export([start_link    /1]).
-export([handle_timeout/2]).

%% supervisor
-behaviour(supervisor).
-export([init/1]).

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/3, process_call/3]).

%%
%% API
%%
-type options() :: #{
    namespace => mg:ns(),
    storage   => mg_storage:storage(),
    processor => mg_utils:mod_opts(),
    observer  => mg_utils:mod_opts()
}.

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start(options(), mg:id(), mg:args()) ->
    ok.
start(Options, ID, Args) ->
    mg_machine:start(Options, ID, Args).

-spec repair(options(), mg:ref(), mg:args(), mg:history_range()) ->
    ok.
repair(Options, Ref, Args, HRange) ->
    mg_machine:repair(Options, ref2id(Options, Ref), Args, HRange).

-spec call(options(), mg:ref(), mg:args(), mg:history_range()) ->
    _Resp.
call(Options, Ref, Call, HRange) ->
    mg_machine:call(Options, ref2id(Options, Ref), {call, Call}, HRange).

-spec get_machine(options(), mg:ref(), mg:history_range()) ->
    mg:machine().
get_machine(Options, Ref, Range) ->
    mg_machine:get_machine(Options, ref2id(Options, Ref), Range).

%%
%% internal API
%%
-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    case supervisor:start_link(?MODULE, Options) of
        {ok, Pid} ->
            ok = mg_machine_timers:handle_timeout(timers_machine_options(Options)),
            {ok, Pid};
        Error={error, _} ->
            Error
    end.

-spec handle_timeout(options(), mg:id()) ->
    ok.
handle_timeout(Options, MachineID) ->
    ok = mg_machine:call(Options, MachineID, handle_timeout, undefined).

%%
%% supervisor callbacks
%%
-spec init(options()) ->
    mg_utils:supervisor_ret().
init(Options) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
        mg_machine_tags  :child_spec(tags_machine_options  (Options), tags_machines  ),
        mg_machine_timers:child_spec(timers_machine_options(Options), timers_machines),
        mg_machine       :child_spec(machine_options       (Options), machines       )
    ]}}.

%%
%% mg_processor handler
%%
-spec process_signal(options(), mg:id(), mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, ID, SignalArgs) ->
    {StateChange, ComplexAction} =
        mg_processor:process_signal(get_option(processor, Options), ID, SignalArgs),
    ok = handle_processor_result(Options, ID, ComplexAction),
    {StateChange, #{}}.

-spec process_call(options(), mg:id(), mg:call_args()) ->
    mg:call_result().
process_call(Options, ID, {handle_timeout, Machine}) ->
    {StateChange, ComplexAction} = process_signal(Options, ID, {timeout, Machine}),
    {ok, StateChange, ComplexAction};
process_call(Options, ID, {{call, Call}, Machine}) ->
    {Response, StateChange, ComplexAction} =
        mg_processor:process_call(get_option(processor, Options), ID, {Call, Machine}),
    ok = handle_processor_result(Options, ID, ComplexAction),
    {Response, StateChange, #{}}.

-spec handle_processor_result(options(), mg:id(), mg:complex_action()) ->
    ok.
handle_processor_result(Options, ID, ComplexAction) ->
    ok = add_tag  (Options, ID, maps:get(tag  , ComplexAction, undefined)),
    ok = set_timer(Options, ID, maps:get(timer, ComplexAction, undefined)).


%%
%% local
%%
-spec machine_options(options()) ->
    mg_machine:options().
machine_options(Options) ->
    Options#{
        processor := {?MODULE, Options}
    }.

-spec timers_machine_options(options()) ->
    mg_machine_timers:options().
timers_machine_options(Options = #{namespace:=Namespace, storage:=Storage}) ->
    #{
        namespace     => {Namespace, timers},
        storage       => Storage,
        timer_handler => {?MODULE, handle_timeout, [Options]}
    }.


-spec tags_machine_options(options()) ->
    mg_machine_tags:options().
tags_machine_options(#{namespace:=Namespace, storage:=Storage}) ->
    #{
        namespace => {Namespace, tags},
        storage   => Storage
    }.

-spec ref2id(options(), mg:ref()) ->
    mg:id().
ref2id(_, {id, ID}) ->
    ID;
ref2id(Options, {tag, Tag}) ->
    case mg_machine_tags:resolve_tag(tags_machine_options(Options), Tag) of
        undefined ->
            throw(machine_not_found);
        ID ->
            ID
    end.

-spec add_tag(options(), mg:id(), undefined | mg:tag()) ->
    ok.
add_tag(_, _, undefined) ->
    ok;
add_tag(Options, ID, Tag) ->
    case mg_machine_tags:add_tag(tags_machine_options(Options), Tag, ID) of
        ok ->
            ok;
        {already_exists, OtherMachineID} ->
            throw({double_tagging, OtherMachineID})
    end.

-spec set_timer(options(), mg:id(), undefined | mg:timer()) ->
    ok.
set_timer(Options, ID, undefined) ->
    ok = mg_machine_timers:cancel_timer(timers_machine_options(Options), ID);
set_timer(Options, ID, Timer) ->
    Datetime = get_timeout_datetime(Timer),
    ok = mg_machine_timers:set_timer(timers_machine_options(Options), ID, Datetime).

-spec get_timeout_datetime(mg:timer()) ->
    calendar:datetime().
get_timeout_datetime({deadline, Datetime}) ->
    Datetime;
get_timeout_datetime({timeout, Timeout}) ->
    calendar:gregorian_seconds_to_datetime(
        calendar:datetime_to_gregorian_seconds(calendar:universal_time()) + Timeout
    ).

-spec get_option(namespace | processor | storage, options()) ->
    _.
get_option(Subj, Options) ->
    maps:get(Subj, Options).
