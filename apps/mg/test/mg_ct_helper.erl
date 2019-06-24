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
-export([assert_wait_expected/3]).

-type appname() :: atom().

-spec start_application(appname() | {appname(), [{atom(), _Value}]}) ->
    _Deps :: [appname()].

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

-spec(assert_wait_expected(any(), function(), mg_retry:strategy()) -> ok).
assert_wait_expected(Expected, Fun, Strategy) when is_function(Fun, 0) ->
    case Fun() of
        Expected ->
            ok;
        Other ->
            case genlib_retry:next_step(Strategy) of
                {wait, Timeout, NextStrategy} ->
                    timer:sleep(Timeout),
                    assert_wait_expected(Expected, Fun, NextStrategy);
                finish ->
                    error({assertion_failed, Expected, Other})
            end
    end.