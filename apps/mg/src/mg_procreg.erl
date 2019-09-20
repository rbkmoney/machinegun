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

-module(mg_procreg).

% Any term sans ephemeral ones, like `reference()`s / `pid()`s / `fun()`s.
-type name() :: term().
-type name_pattern() :: ets:match_pattern().

-type ref() :: mg_utils:gen_ref().
-type reg_name() :: mg_utils:gen_reg_name().

-type procreg_options() :: term().
-type options() :: mg_utils:mod_opts(procreg_options()).

-export_type([name/0]).
-export_type([name_pattern/0]).
-export_type([ref/0]).
-export_type([reg_name/0]).
-export_type([options/0]).

-export_type([start_link_ret/0]).
-export_type([start_supervisor_ret/0]).

-export([ref/2]).
-export([reg_name/2]).
-export([select/2]).

-export([start_link/5]).
-export([call/3]).
-export([call/4]).

%%

-export([start_supervisor/4]).

%% Names and references

-callback ref(procreg_options(), name()) ->
    ref().

-callback reg_name(procreg_options(), name()) ->
    reg_name().

-callback select(procreg_options(), name_pattern()) ->
    [{name(), pid()}].

-callback call(procreg_options(), ref(), _Call, timeout()) ->
    _Reply.

-type start_link_ret() ::
    {ok, pid()} | {error, term()}.

-callback start_link(procreg_options(), reg_name(), module(), _Args, list()) ->
    start_link_ret().

%% Leader supervisor

-type start_supervisor_ret() ::
    {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.

-callback start_supervisor(
    procreg_options(),
    name(),
    supervisor:sup_flags(),
    [supervisor:child_spec()]
) ->
    start_supervisor_ret().

%%

-spec ref(options(), name()) ->
    ref().
ref(Options, Name) ->
    mg_utils:apply_mod_opts(Options, ref, [Name]).

-spec reg_name(options(), name()) ->
    reg_name().
reg_name(Options, Name) ->
    mg_utils:apply_mod_opts(Options, reg_name, [Name]).

-spec select(options(), name_pattern()) ->
    [{name(), pid()}].
select(Options, NamePattern) ->
    mg_utils:apply_mod_opts(Options, select, [NamePattern]).

-spec call(options(), name(), _Call) ->
    _Reply.
call(Options, Name, Call) ->
    call(Options, Name, Call, 5000).

-spec call(options(), name(), _Call, timeout()) ->
    _Reply.
call(Options, Name, Call, Timeout) ->
    mg_utils:apply_mod_opts(Options, call, [ref(Options, Name), Call, Timeout]).

-spec start_link(options(), name(), module(), _Args, list()) ->
    start_link_ret().
start_link(Options, Name, Module, Args, Opts) ->
    mg_utils:apply_mod_opts(Options, start_link, [reg_name(Options, Name), Module, Args, Opts]).

%%

-spec start_supervisor(
    options(),
    name(),
    supervisor:sup_flags(),
    [supervisor:child_spec()]
) ->
    start_supervisor_ret().
start_supervisor(Options, Name, SupFlags, ChildSpecs) ->
    mg_utils:apply_mod_opts(Options, start_supervisor, [Name, SupFlags, ChildSpecs]).
