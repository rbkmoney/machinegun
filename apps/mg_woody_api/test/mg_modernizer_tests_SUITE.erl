%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_modernizer_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

-export([start_machine/1]).
-export([no_modernize_avail/1]).
-export([modernize_machine_part/1]).
-export([modernize_machine/1]).
-export([count_elements/1]).
-export([store_random_element/1]).
-export([store_fixed_element/1]).
-export([lookup_fixed_element/1]).

%%

-define(NS, <<"NS">>).
-define(ID, <<"😠💢"/utf8>>).

-define(FIXED_ELEMENT  , #{<<"BLAZE">> => [<<"IT">>, 420, 6969]}).
-define(MODERN_FMT_VSN , 42).

-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        {group, legacy_activities},
        {group, modern_activities}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name() | {group, group_name()}}].
groups() ->
    [

        {activities, [sequence], [
            count_elements,
            store_random_element,
            store_random_element,
            store_fixed_element,
            store_random_element,
            lookup_fixed_element,
            count_elements
        ]},

        {legacy_activities, [], [
            start_machine,
            no_modernize_avail,
            {group, activities}
        ]},

        {modern_activities, [], [
            modernize_machine_part,
            modernize_machine,
            modernize_machine,
            {group, activities}
        ]}

    ].

%%

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(),
    % dbg:p(all, c),
    % dbg:tpl({mg_woody_api, '_', '_'}, x),
    Apps = mg_ct_helper:start_applications([gproc]),
    % Запускаем memory storage, который сможет "пережить" рестарты mg
    {ok, StoragePid} = mg_storage_memory:start_link(#{name => ?MODULE}),
    true = erlang:unlink(StoragePid),
    [{suite_apps, Apps}, {storage_name, ?MODULE} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(suite_apps, C)).

%%

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(Name = legacy_activities, C0) ->
    C1 = start_mg_woody_api(Name, C0),
    {ok, ProcessorPid} = mg_test_processor:start(
        {0, 0, 0, 0}, 8023,
        genlib_map:compact(#{
            processor  => {"/processor", {default_func, fun legacy_call_handler/1}}
        })
    ),
    [{processor_pid, ProcessorPid} | C1];
init_per_group(Name = modern_activities, C0) ->
    C1 = start_mg_woody_api(Name, C0),
    {ok, ProcessorPid} = mg_test_processor:start(
        {0, 0, 0, 0}, 8023,
        genlib_map:compact(#{
            processor  => {"/processor", {default_func, fun modern_call_handler/1}},
            modernizer => {"/modernizer", fun modernize_handler/1}
        })
    ),
    [{processor_pid, ProcessorPid} | C1];
init_per_group(activities, C) ->
    C.

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(Name, C) when
    Name == legacy_activities;
    Name == modern_activities
->
    ok = proc_lib:stop(?config(processor_pid, C)),
    mg_ct_helper:stop_applications(?config(group_apps, C));
end_per_group(_, _C) ->
    ok.

-spec start_mg_woody_api(group_name(), config()) ->
    config().
start_mg_woody_api(Name, C) ->
    Scheduler = #{
        scan_interval => #{continue => 100, completed => 15000}
    },
    Config = [
        {woody_server, #{
            ip       => {0,0,0,0,0,0,0,0},
            port     => 8022,
            net_opts => [],
            limits   => #{}
        }},
        {namespaces, #{
            ?NS => maps:merge(
                #{
                    storage    => {mg_storage_memory, #{
                        existing_storage_name => ?config(storage_name, C)}
                    },
                    processor  => #{
                        url            => <<"http://localhost:8023/processor">>,
                        transport_opts => #{pool => ns, max_connections => 100}
                    },
                    default_processing_timeout => 5000,
                    schedulers => #{
                        timers         => Scheduler,
                        timers_retries => Scheduler,
                        overseer       => Scheduler
                    },
                    retries => #{}
                },
                case Name of
                    legacy_activities ->
                        #{};
                    modern_activities ->
                        #{
                            modernizer => #{
                                current_format_version => ?MODERN_FMT_VSN,
                                handler                => #{url => <<"http://localhost:8023/modernizer">>}
                            }
                        }
                end
            )
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
        }}
    ],
    Apps = mg_ct_helper:start_applications([{mg_woody_api, Config}]),
    [
        {group_name        , Name},
        {group_apps        , Apps},
        {automaton_options , #{
            url            => "http://localhost:8022",
            ns             => ?NS
        }} | C
    ].

%%

-type legacy_st() :: sets:set(mg_storage:opaque()).
-type modern_st() :: #{integer() => [mg_storage:opaque()]}.
-type any_st()    :: {legacy, legacy_st()} | {modern, modern_st()}.

-spec legacy_call_handler(mg:call_args()) ->
    mg:call_result().
legacy_call_handler({[<<"store">>, Element], Machine}) ->
    St = collapse_legacy(Machine),
    case lookup(Element, {legacy, St}) of
        true ->
            % resp, {auxst, events}, actions
            {true, {null(), []}, #{}};
        false ->
            % resp, {auxst, events}, actions
            {true, {null(), [{#{}, Element}]}, #{}}
    end.

-spec modern_call_handler(mg:call_args()) ->
    mg:call_result().
modern_call_handler({[<<"store">>, Element], Machine}) ->
    St = collapse_modern(Machine),
    Hash = erlang:phash2(Element),
    case lookup_by_hash(Hash, Element, St) of
        true ->
            % resp, {auxst, events}, actions
            {true, {null(), []}, #{}};
        false ->
            % resp, {auxst, events}, actions
            {true, {null(), [{#{format_version => ?MODERN_FMT_VSN}, [Hash, Element]}]}, #{}}
    end.

-spec modernize_handler(mg_events_modernizer:machine_event()) ->
    mg_events_modernizer:modernized_event_body().
modernize_handler(#{event := #{body := {Metadata, Element}}}) ->
    [] = maps:keys(Metadata),
    {
        #{format_version => ?MODERN_FMT_VSN},
        [erlang:phash2(Element), Element]
    }.

-spec null() -> mg_events:content().
null() ->
    {#{}, null}.

-spec collapse_legacy(mg_events_machine:machine()) -> legacy_st().
collapse_legacy(#{history := History}) ->
    lists:foldl(
        fun (#{body := {_Metadata, Element}}, Set) ->
            sets:add_element(Element, Set)
        end,
        sets:new(),
        History
    ).

-spec collapse_modern(mg_events_machine:machine()) -> modern_st().
collapse_modern(#{history := History}) ->
    lists:foldl(
        fun (#{body := {#{format_version := ?MODERN_FMT_VSN}, [Hash, Element]}}, St) ->
            maps:update_with(Hash, fun (Es) -> [Element | Es] end, [Element], St)
        end,
        #{},
        History
    ).

%%

-spec collapse(mg_events_machine:machine(), config()) -> any_st().
collapse(Machine, C) ->
    case ?config(group_name, C) of
        legacy_activities ->
            {legacy, collapse_legacy(Machine)};
        modern_activities ->
            {modern, collapse_modern(Machine)}
    end.

-spec count(any_st()) -> non_neg_integer().
count({legacy, St}) ->
    sets:size(St);
count({modern, St}) ->
    maps:fold(fun (_, Es, Sum) -> Sum + length(Es) end, 0, St).

-spec lookup(mg_storage:opaque(), any_st()) -> boolean().
lookup(Element, {legacy, St}) ->
    sets:is_element(Element, St);
lookup(Element, {modern, St}) ->
    Hash = erlang:phash2(Element),
    lookup_by_hash(Hash, Element, St).

-spec lookup_by_hash(integer(), mg_storage:opaque(), modern_st()) -> boolean().
lookup_by_hash(Hash, Element, St) ->
    case St of
        #{Hash := Es} ->
            lists:member(Element, Es);
        #{} ->
            false
    end.

%%

-spec start_machine(config()) ->
    _.
start_machine(C) ->
    Options = ?config(automaton_options, C),
    ok = mg_automaton_client:start(Options, ?ID, ?ID).

-spec no_modernize_avail(config()) ->
    _.
no_modernize_avail(C) ->
    Options = ?config(automaton_options, C),
    % TODO
    #mg_stateproc_NamespaceNotFound{} =
        (catch mg_automaton_client:modernize(Options, {id, ?ID}, {undefined, undefined, forward})).

-spec modernize_machine_part(config()) ->
    _.
modernize_machine_part(C) ->
    Options = ?config(automaton_options, C),
    ok = mg_automaton_client:modernize(Options, {id, ?ID}, {undefined, 1, forward}).

-spec modernize_machine(config()) ->
    _.
modernize_machine(C) ->
    Options = ?config(automaton_options, C),
    ok = mg_automaton_client:modernize(Options, {id, ?ID}, {undefined, undefined, forward}).

-spec count_elements(config()) ->
    _.
count_elements(C) ->
    Options = ?config(automaton_options, C),
    Machine = mg_automaton_client:get_machine(Options, {id, ?ID}, {undefined, undefined, forward}),
    Count = count(collapse(Machine, C)),
    true = is_integer(Count) and (Count >= 0).

-spec store_fixed_element(config()) ->
    _.
store_fixed_element(C) ->
    Options = ?config(automaton_options, C),
    true = mg_automaton_client:call(Options, {id, ?ID}, [<<"store">>, ?FIXED_ELEMENT]).

-spec store_random_element(config()) ->
    _.
store_random_element(C) ->
    Options = ?config(automaton_options, C),
    true = mg_automaton_client:call(Options, {id, ?ID}, [<<"store">>, [<<"BLARG">>, rand:uniform(1000000)]]).

-spec lookup_fixed_element(config()) ->
    _.
lookup_fixed_element(C) ->
    Options = ?config(automaton_options, C),
    Machine = mg_automaton_client:get_machine(Options, {id, ?ID}, {undefined, undefined, forward}),
    true = lookup(?FIXED_ELEMENT, collapse(Machine, C)).
