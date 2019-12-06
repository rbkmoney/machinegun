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

-module(mg_stress_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all             /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).

-export([stress_test/1]).

-define(NS, <<"NS">>).
-define(ES_ID, <<"test_event_sink">>).


-type test_name () :: atom().
-type config    () :: [{atom(), _}].


-spec all() ->
    [test_name()].
all() ->
    [
        stress_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([
        {hackney      , [{use_default_pool, false}]},
        {mg_woody_api , mg_woody_api_config(C)}
    ]),

    CallFunc =
        fun({Args, _Machine}) ->
            case Args of
                <<"event">> -> {Args, {{#{}, <<>>}, [{#{}, <<"event_body">>}]}, #{timer => undefined, tag => undefined}};
                _           -> {Args, {{#{}, <<>>}, []}, #{timer => undefined, tag => undefined}}
            end
        end
    ,
    SignalFunc =
        fun({Args, _Machine}) ->
            case Args of
                _ -> mg_test_processor:default_result(signal, Args)
            end
        end
    ,
    {ok, ProcessorPid} = mg_test_processor:start(
        {0, 0, 0, 0},
        8023,
        #{processor => {"/processor", {SignalFunc, CallFunc}}}
    ),

    [
        {apps              , Apps                             },
        {automaton_options , #{
            url => "http://localhost:8022",
            ns => ?NS,
            retry_strategy => mg_retry:new_strategy({exponential, 5, 2, 1000})
        }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    ok = proc_lib:stop(?config(processor_pid, C)),
    mg_ct_helper:stop_applications(?config(apps, C)).

-spec mg_woody_api_config(config()) ->
    list().
mg_woody_api_config(_C) ->
    Scheduler = #{
        scan_interval => #{continue => 500, completed => 15000}
    },
    [
        {woody_server, #{
            ip     => {0,0,0,0,0,0,0,0},
            port   => 8022,
            limits => #{},
            transport_opts => #{num_acceptors => 100}
        }},
        {namespaces, #{
            ?NS => #{
                storage    => mg_storage_memory,
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
                retries => #{},
                event_sinks => [{mg_events_sink_machine, #{name => default, machine_id => ?ES_ID}}]
            }
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
        }}
    ].


-spec stress_test(config()) -> _.
stress_test(C) ->
    TestTimeout = 5 * 1000,
    N = 10,

    Processes = [stress_test_start_processes(C, integer_to_binary(ID)) || ID <- lists:seq(1, N)],

    ok = timer:sleep(TestTimeout),
    ok = mg_ct_helper:stop_wait_all(Processes, shutdown, 2000).

-spec stress_test_start_processes(term(), mg:id()) ->
    _.
stress_test_start_processes(C, ID) ->
    Pid =
        erlang:spawn_link(
            fun() ->
                start_machine(C, ID),
                create(C, ID)
            end
        ),
    timer:sleep(1000),
    Pid.

%%
%% utils
%%
-spec start_machine(config(), mg:id()) ->
    _.
start_machine(C, ID) ->
    mg_automaton_client:start(automaton_options(C), ID, ID).

-spec create(config(), mg:id()) ->
    _.
create(C, ID) ->
    create_event(<<"event">>, C, ID),
    timer:sleep(1000),
    create(C, ID).

-spec create_event(binary(), config(), mg:id()) ->
    _.
create_event(Event, C, ID) ->
    Event = mg_automaton_client:call(automaton_options(C), {id, ID}, Event).

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).
