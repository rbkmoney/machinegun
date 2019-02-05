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

-module(mg_quota_manager).

-export([child_spec    /2]).
-export([start_link    /1]).

%% Types
-type options() :: #{quota_name() => quota_options()}.

-export_type([options/0]).

%% Internal types
-type quota_name() :: mg_quota_worker:name().
-type quota_options() :: mg_quota_worker:options().

%%
%% API
%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_one},
        [
            mg_quota_worker:child_spec(QuotaOptions, Name)
            || #{name := Name} = QuotaOptions <- maps:values(Options)
        ]
    ).