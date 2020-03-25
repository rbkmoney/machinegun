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
-include_lib("mg/include/pulse.hrl").

%% API
-export_type([name        /0]).

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
-export([put       /5]).
-export([get       /2]).
-export([search    /2]).
-export([delete    /3]).

-export([do_request/2]).

%% Internal API
-export([start_link/1]).

-export([opaque_to_binary/1]).
-export([binary_to_opaque/1]).

-define(KEY_SIZE_LOWER_BOUND, 1).
-define(KEY_SIZE_UPPER_BOUND, 1024).

%%
%% API
%%
-type name        () :: term().

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

-type search_result() ::
    {[{index_value(), key()}], continuation()} |
    {[key()], continuation()} |
    [{index_value(), key()}] |
    [key()].


-type storage_options() :: #{
    name := name(),
    pulse := mg_pulse:handler(),
    sidecar => mg_utils:mod_opts(),
    atom() => any()
}.
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

%% Timestamp and duration are in native units
-type duration() :: non_neg_integer().
-type timestamp() :: pos_integer().

-callback child_spec(storage_options(), atom()) ->
    supervisor:child_spec() | undefined.

-callback do_request(storage_options(), request()) ->
    response().

-optional_callbacks([child_spec/2]).

%%

-spec start_link(options()) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    mg_utils_supervisor_wrapper:start_link(
        #{strategy => rest_for_one},
        mg_utils:lists_compact([
            mg_utils:apply_mod_opts_if_defined(Options, child_spec, undefined, [storage]),
            sidecar_child_spec(Options, sidecar)
        ])
    ).

-spec child_spec(options(), term()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec put(options(), key(), context() | undefined, value(), [index_update()]) ->
    context().
put(_Options, Key, _Context, _Value, _Indexes) when byte_size(Key) < ?KEY_SIZE_LOWER_BOUND ->
    throw({logic, {invalid_key, {too_small, Key}}});
put(_Options, Key, _Context, _Value, _Indexes) when byte_size(Key) > ?KEY_SIZE_UPPER_BOUND ->
    throw({logic, {invalid_key, {too_big, Key}}});
put(Options, Key, Context, Value, Indexes) ->
    do_request(Options, {put, Key, Context, Value, Indexes}).

-spec get(options(), key()) ->
    {context(), value()} | undefined.
get(_Options, Key) when byte_size(Key) < ?KEY_SIZE_LOWER_BOUND ->
    throw({logic, {invalid_key, {too_small, Key}}});
get(_Options, Key) when byte_size(Key) > ?KEY_SIZE_UPPER_BOUND ->
    throw({logic, {invalid_key, {too_big, Key}}});
get(Options, Key) ->
    do_request(Options, {get, Key}).

-spec search(options(), index_query()) ->
    search_result().
search(Options, Query) ->
    do_request(Options, {search, Query}).

-spec delete(options(), key(), context()) ->
    ok.
delete(Options, Key, Context) ->
    do_request(Options, {delete, Key, Context}).

-spec do_request(options(), request()) ->
    response().
do_request(Options, Request) ->
    {_Handler, StorageOptions} = mg_utils:separate_mod_opts(Options, #{}),
    StartTimestamp = erlang:system_time(),
    ok = emit_beat_start(Request, StorageOptions, StartTimestamp),
    Result = mg_utils:apply_mod_opts(Options, do_request, [Request]),
    FinishTimestamp = erlang:system_time(),
    Duration = FinishTimestamp - StartTimestamp,
    ok = emit_beat_finish(Request, StorageOptions, FinishTimestamp, Duration),
    Result.

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

%% Internals

-spec sidecar_child_spec(options(), term()) ->
    supervisor:child_spec() | undefined.
sidecar_child_spec(Options, ChildID) ->
    {_Handler, StorageOptions} = mg_utils:separate_mod_opts(Options, #{}),
    case maps:find(sidecar, StorageOptions) of
        {ok, Sidecar} ->
            mg_utils:apply_mod_opts(Sidecar, child_spec, [Options, ChildID]);
        error ->
            undefined
    end.

%%
%% logging
%%

-spec emit_beat_start(mg_storage:request(), storage_options(), timestamp()) -> ok.
emit_beat_start({get, _}, #{pulse := Handler, name := Name}, StartTimestamp) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_get_start{
        name = Name,
        timestamp = StartTimestamp
    });
emit_beat_start({put, _, _, _, _}, #{pulse := Handler, name := Name}, StartTimestamp) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_put_start{
        name = Name,
        timestamp = StartTimestamp
    });
emit_beat_start({search, _}, #{pulse := Handler, name := Name}, StartTimestamp) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_search_start{
        name = Name,
        timestamp = StartTimestamp
    });
emit_beat_start({delete, _, _}, #{pulse := Handler, name := Name}, StartTimestamp) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_delete_start{
        name = Name,
        timestamp = StartTimestamp
    }).

-spec emit_beat_finish(mg_storage:request(), storage_options(), timestamp(), duration()) -> ok.
emit_beat_finish({get, _}, #{pulse := Handler, name := Name}, FinishTimestamp, Duration) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_get_finish{
        name = Name,
        timestamp = FinishTimestamp,
        duration  = Duration
    });
emit_beat_finish({put, _, _, _, _}, #{pulse := Handler, name := Name}, FinishTimestamp, Duration) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_put_finish{
        name = Name,
        timestamp = FinishTimestamp,
        duration  = Duration
    });
emit_beat_finish({search, _}, #{pulse := Handler, name := Name}, FinishTimestamp, Duration) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_search_finish{
        name = Name,
        timestamp = FinishTimestamp,
        duration  = Duration
    });
emit_beat_finish({delete, _, _}, #{pulse := Handler, name := Name}, FinishTimestamp, Duration) ->
    ok = mg_pulse:handle_beat(Handler, #mg_storage_delete_finish{
        name = Name,
        timestamp = FinishTimestamp,
        duration  = Duration
    }).
