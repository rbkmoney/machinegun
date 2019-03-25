%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_ct_helper).

-export([start_application/1]).
-export([start_applications/1]).

-export([stop_applications/1]).

-type appname() :: atom().

-spec start_application(appname() | {appname(), [{atom(), _Value}]}) ->
    _Deps :: [appname()].

start_application(lager) ->
    genlib_app:start_application_with(lager, [
        {error_logger_hwm, 1000},
        {handlers, [
            {lager_common_test_backend, [
                info,
                {lager_default_formatter, [time, " ", severity, " ", metadata, " ", message]}
            ]}
        ]},
        {async_threshold, undefined}
    ]);
start_application(brod) ->
    genlib_app:start_application_with(brod, [
        {clients, [
            {mg_kafka_client, [
                {endpoints, [{"kafka1", 9092}, {"kafka2", 9092}, {"kafka3", 9092}]},
                {auto_start_producers, true}
            ]}
        ]}
    ]);
start_application({AppName, Env}) ->
    genlib_app:start_application_with(AppName, Env);
start_application(AppName) ->
    genlib_app:start_application(AppName).

-spec start_applications([appname()]) ->
    _Deps :: appname().

start_applications(Apps) ->
    lists:foldl(fun (App, Deps) -> Deps ++ start_application(App) end, [], Apps).

-spec stop_applications([appname()]) ->
    ok.

stop_applications(AppNames) ->
    lists:foreach(fun application:stop/1, lists:reverse(AppNames)).
