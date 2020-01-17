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

-module(mg_procreg_gproc).

%%

-behaviour(mg_procreg).

-export([ref/2]).
-export([reg_name/2]).
-export([select/2]).

-export([start_link/5]).
-export([call/4]).

-type options() :: undefined.

%%

-spec ref(options(), mg_procreg:name()) ->
    mg_procreg:ref().
ref(_Options, Name) ->
    {via, gproc, {n, l, Name}}.

-spec reg_name(options(), mg_procreg:name()) ->
    mg_procreg:reg_name().
reg_name(Options, Name) ->
    ref(Options, Name).

-spec select(options(), mg_procreg:name_pattern()) ->
    [{mg_procreg:name(), pid()}].
select(_Options, NamePattern) ->
    MatchSpec = [{{{n, l, NamePattern}, '_', '_'}, [], ['$$']}],
    [{Name, Pid} || [{n, l, Name}, Pid, _] <- gproc:select(MatchSpec)].

-spec start_link(options(), mg_procreg:reg_name(), module(), _Args, list()) ->
    mg_procreg:start_link_ret().
start_link(_Options, RegName, Module, Args, Opts) ->
    gen_server:start_link(RegName, Module, Args, Opts).

-spec call(options(), mg_procreg:ref(), _Call, timeout()) ->
    _Reply.
call(_Options, Ref, Call, Timeout) ->
    gen_server:call(Ref, Call, Timeout).
