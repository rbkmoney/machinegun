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

%%%
%%% Интерфейс работы с хранилищем данных.
%%% Он с виду выглядит абстрактным и не привязанным к конкретной базе,
%%% но по факту он копирует интерфейс риака.
%%% (Хотя положить на него можно и другие базы.)
%%%
%%% TODO:
%%%  - сделать работу с пачками через функтор и контекст
%%%
-module(mg_storage).

%% API
-export_type([opaque      /0]).
-export_type([key         /0]).
-export_type([value       /0]).
-export_type([kv          /0]).
-export_type([context     /0]).
-export_type([continuation/0]).

-export_type([index_name       /0]).
-export_type([index_value      /0]).
-export_type([index_update     /0]).
-export_type([index_query_value/0]).
-export_type([index_limit      /0]).
-export_type([index_query      /0]).
-export_type([search_result    /0]).

-export_type([storage_options/0]).
-export_type([options/0]).

-export_type([request /0]).
-export_type([response/0]).

-export([child_spec/2]).
-export([child_spec/3]).
-export([put       /6]).
-export([get       /3]).
-export([search    /3]).
-export([delete    /4]).

-export([do_request/3]).

%% Internal API
-export([opaque_to_binary/1]).
-export([binary_to_opaque/1]).

%%
%% API
%%
-type opaque      () :: null | true | false | number() | binary() | [opaque()] | #{opaque() => opaque()}.
-type key         () :: binary().
-type value       () :: opaque().
-type kv          () :: {key(), value()}.
-type context     () :: term().
-type continuation() :: term(). % undefined означает, что данные кончились

%% типизация получилась отвратная, но лучше не вышло :-\
-type index_name       () :: {binary | integer, binary()}.
-type index_value      () :: binary() | integer().
-type index_update     () :: {index_name(), index_value()}.
-type index_query_value() :: index_value() | {index_value(), index_value()}.
-type index_limit      () :: non_neg_integer() | inf.
-type index_query      () :: {index_name(), index_query_value()}
                           | {index_name(), index_query_value(), index_limit()}
                           | {index_name(), index_query_value(), index_limit(), continuation()}.

-type search_result() :: {[{index_value(), key()}], continuation()} | {[key()], continuation()}.


-type storage_options() :: term().
-type options() :: mg_utils:mod_opts(storage_options()).

%%

-type request() ::
      {put, key(), context() | undefined, value(), [index_update()]}
    | {get, key()}
    | {search, index_query()}
    | {delete, key(), context()}
.

-type response() ::
      context()
    | {context(), value()} | undefined
    | search_result()
    | ok.

-callback child_spec(storage_options(), atom()) ->
    supervisor:child_spec() | undefined.

-callback child_spec(storage_options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec() | undefined.

-callback do_request(storage_options(), mg_utils:gen_ref(), request()) ->
    response().

-optional_callbacks([child_spec/2, child_spec/3]).

%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_utils:apply_mod_opts(Options, child_spec, [ChildID]).

-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec() | undefined.
child_spec(Options, ChildID, RegName) ->
    mg_utils:apply_mod_opts(Options, child_spec, [ChildID, RegName]).

-spec put(options(), mg_utils:gen_ref(), key(), context() | undefined, value(), [index_update()]) ->
    context().
put(_Options, _SelfRef, <<"">>, _Context, _Value, _Indexes) ->
    throw({logic, zero_length_key});
put(Options, SelfRef, Key, Context, Value, Indexes) ->
    do_request(Options, SelfRef, {put, Key, Context, Value, Indexes}).

-spec get(options(), mg_utils:gen_ref(), key()) ->
    {context(), value()} | undefined.
get(_Options, _SelfRef, <<"">>) -> %fix for riak timeouts
    undefined;
get(Options, SelfRef, Key) ->
    do_request(Options, SelfRef, {get, Key}).

-spec search(options(), mg_utils:gen_ref(), index_query()) ->
    [key()] | {[key()], continuation()}.
search(Options, SelfRef, Query) ->
    do_request(Options, SelfRef, {search, Query}).

-spec delete(options(), mg_utils:gen_ref(), key(), context()) ->
    ok.
delete(Options, SelfRef, Key, Context) ->
    do_request(Options, SelfRef, {delete, Key, Context}).

-spec do_request(options(), mg_utils:gen_ref(), request()) ->
    response().
do_request(Options, SelfRef, Request) ->
    mg_utils:apply_mod_opts(Options, do_request, [SelfRef, Request]).

%%
%% Internal API
%%
-define(msgpack_options, [
    {spec           , new             },
    {allow_atom     , none            },
    {unpack_str     , as_tagged_list  },
    {validate_string, false           },
    {pack_str       , from_tagged_list},
    {map_format     , map             }
]).

-spec opaque_to_binary(opaque()) ->
    binary().
opaque_to_binary(Opaque) ->
    case msgpack:pack(Opaque, ?msgpack_options) of
        Data when is_binary(Data) ->
            Data;
        {error, Reason} ->
            erlang:error(msgpack_pack_error, [Opaque, Reason])
    end.

-spec binary_to_opaque(binary()) ->
    opaque().
binary_to_opaque(Binary) ->
    case msgpack:unpack(Binary, ?msgpack_options) of
        {ok, Data} ->
            Data;
        {error, Reason} ->
            erlang:error(msgpack_unpack_error, [Binary, Reason])
    end.
