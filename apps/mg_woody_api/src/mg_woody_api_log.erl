%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_woody_api_log).

%% logging API
-export([log/1]).

%% logging types
-type log_msg() :: {level(), msg(), meta()}.
-type msg() :: expanded_msg() | string().
-type meta() :: [{atom(), binary() | number() | meta()}]. % there is no such exported type in lager
-type level() :: lager:log_level().

-export_type([log_msg/0]).
-export_type([msg    /0]).
-export_type([meta   /0]).
-export_type([level  /0]).

%% internal types
-type expanded_msg() :: {Format::string(), Args::list()}.

%%
%% logging API
%%
-spec log(undefined | log_msg()) ->
    ok.
log(undefined) ->
    ok;
log({Level, Msg, Meta}) ->
    {MsgFormat, MsgArgs} = expand_msg(Msg),
    ok = lager:log(Level, [{pid, erlang:self()} | Meta], MsgFormat, MsgArgs).

-spec expand_msg(msg()) ->
    expanded_msg().
expand_msg(Msg={_, _}) ->
    Msg;
expand_msg(Str) ->
    {Str, []}.
