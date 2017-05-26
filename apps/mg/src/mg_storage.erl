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

-export_type([storage/0]).
-export_type([options/0]).

-export([child_spec/2]).
-export([put       /5]).
-export([get       /2]).
-export([search    /2]).
-export([delete    /3]).

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
-type continuation() :: term().

%% типизация получилась отвратная, но лучше не вышло :-\
-type index_name       () :: {binary | integer, binary()}.
-type index_value      () :: binary() | integer().
-type index_update     () :: {index_name(), index_value()}.
-type index_query_value() :: index_value() | {index_value(), index_value()}.
-type index_limit      () :: non_neg_integer() | inf.
-type index_query      () :: {index_name(), index_query_value()}
                           | {index_name(), index_query_value(), index_limit()}
                           | {index_name(), index_query_value(), index_limit(), continuation()}.

-type storage() :: mg_utils:mod_opts().
-type options() :: #{
    namespace => mg:ns(),
    module    => storage(),
    search_limit => index_limit()
}.

%%

-callback child_spec(_Options, mg:ns(), atom()) ->
    supervisor:child_spec().

-callback put(_Options, mg:ns(), key(), context() | undefined, value(), [index_update()]) ->
    context().

-callback get(_Options, mg:ns(), key()) ->
    {context(), value()} | undefined.

-callback search(_Options, mg:ns(), index_query()) ->
    {[{index_value(), key()}], continuation()} | {[key()], continuation()}.

-callback delete(_Options, mg:ns(), key(), context()) ->
    ok.

%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    apply_mod_opts(Options, child_spec, [ChildID]).

-spec put(options(), key(), context() | undefined, value(), [index_update()]) ->
    context().
put(Options, Key, Context, Value, Indexes) ->
    apply_mod_opts(Options, put, [Key, Context, Value, Indexes]).

-spec get(options(), key()) ->
    {context(), value()} | undefined.
get(Options, Key) ->
    apply_mod_opts(Options, get, [Key]).

-spec search(options(), index_query()) ->
    [key()] | {[key()], continuation()}.
search(Options, Query) ->
    apply_mod_opts(Options, search, [Query]).

-spec delete(options(), key(), context()) ->
    ok.
delete(Options, Key, Context) ->
    apply_mod_opts(Options, delete, [Key, Context]).

%%

-spec apply_mod_opts(options(), Function::atom(), list()) ->
    _.
apply_mod_opts(#{module := Module, namespace := Namespace}, Function, Args) ->
    mg_utils:apply_mod_opts(Module, Function, [Namespace|Args]).

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
