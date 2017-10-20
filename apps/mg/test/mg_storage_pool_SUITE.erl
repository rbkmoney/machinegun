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

-module(mg_storage_pool_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).

%% base group tests
-export([worker_fail_test/1]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export([start_link/2]).
-export([child_spec/2, do_request/3]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        worker_fail_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({riakc_pb_socket, 'get_index_eq', '_'}, x),
    % dbg:tpl({riakc_pb_socket, 'get_index_range', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

%%
%% base group tests
%%
-define(REPEAT_N(N, Expr), lists:foreach(fun(_) -> Expr end, lists:seq(1, N))).

-spec worker_fail_test(config()) ->
    ok.
worker_fail_test(_C) ->
    Key = <<"pool_test_key">>,
    N = 100,
    Options =
        {
            mg_storage_pool,
            #{
                worker          => ?MODULE,
                size            => N,
                queue_len_limit => 100,
                retry_attempts  => 10
            }
        },
    {ok, _} =
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Options, storage, {local, storage})]
        ),

    ?REPEAT_N(100, begin undefined              =        mg_storage:get(Options, storage, Key           ) end),
    ?REPEAT_N(100, begin {'EXIT', {suicide, _}} = (catch mg_storage:get(Options, storage, <<"suicide">>)) end),
    ?REPEAT_N(100, begin undefined              =        mg_storage:get(Options, storage, Key           ) end),
    ok.

%%
%% mg_storage callbacks
%%
-spec child_spec(_, atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec start_link(_, mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(Options, RegName) ->
    gen_server:start_link(RegName, ?MODULE, Options, []).

-spec do_request(_, _, mg_storage:request()) ->
    mg_storage:response().
do_request(_, SelfRef, Req) ->
    gen_server:call(SelfRef, Req).

-type state() :: _.
-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init(_) ->
    {ok, undefined}.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()) | no_return().
handle_call({get, <<"suicide">>}, _From, State) ->
    exit(suicide),
    {noreply, State};
handle_call({get, _}, _From, State) ->
    {reply, undefined, State};
handle_call(Call, From, State) ->
    _ = exit({'unexpected call received', Call, From}),
    {noreply, State}.

-spec handle_cast(_Cast, state()) ->
    no_return().
handle_cast(Cast, State) ->
    _ = erlang:exit({'unexpected cast received', Cast}),
    {noreply, State}.

-spec handle_info(_Info, state()) ->
    no_return().
handle_info(Info, State) ->
    _ = erlang:exit({'unexpected info received', Info}),
    {noreply, State}.

-spec code_change(_, state(), _) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.
