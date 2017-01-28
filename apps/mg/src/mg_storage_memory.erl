-module(mg_storage_memory).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, put/5, get/3, delete/4]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% internal API
%%
-spec start_link(options(), mg:ns()) ->
    mg_utils:gen_start_ret().
start_link(Options, Namespace) ->
    gen_server:start_link(self_reg_name(Namespace), ?MODULE, {Options, Namespace}, []).

%%
%% mg_storage callbacks
%%
-type options() :: _.

-spec child_spec(options(), mg:ns(), atom()) ->
    supervisor:child_spec().
child_spec(Options, Namespace, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, Namespace]},
        restart  => permanent,
        shutdown => 5000
    }.

%% тут происходит упаковка и проверка, чтобы пораньше увидеть потенциальные проблемы
-spec put(_Options, mg:ns(), mg_storage:key(), mg_storage:context(), mg_storage:value()) ->
    mg_storage:context().
put(_Options, Namespace, Key, Context, Value) when is_binary(Namespace) andalso is_binary(Key) ->
    gen_server:call(self_ref(Namespace), {put, Key, Context, mg_storage:opaque_to_binary(Value)}).

-spec get(_Options, mg:ns(), mg_storage:key()) ->
    {mg_storage:context(), mg_storage:value()} | undefined.
get(_Options, Namespace, Key) when is_binary(Namespace) andalso is_binary(Key) ->
    case gen_server:call(self_ref(Namespace), {get, Key}) of
        undefined ->
            undefined;
        {Context, Value} ->
            {Context, mg_storage:binary_to_opaque(Value)}
    end.

-spec delete(_Options, mg:ns(), mg_storage:key(), mg_storage:context()) ->
    ok.
delete(_Options, Namespace, Key, Context) when is_binary(Namespace) andalso is_binary(Key) ->
    gen_server:call(self_ref(Namespace), {delete, Key, Context}).


%%
%% gen_server callbacks
%%
-type state() :: #{
    namespace => mg:ns(),
    options   => options(),
    values    => #{{mg:id(), mg_storage:key()} => {mg_storage:context(), mg_storage:value()}}
}.

-spec init({options(), mg:ns()}) ->
    mg_utils:gen_server_init_ret(state()).
init({Options, Namespace}) ->
    {ok,
        #{
            namespace => Namespace,
            options   => Options,
            values    => #{}
        }
    }.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({put, Key, Context, Value}, _From, State) ->
    {Resp, NewState} = do_put(Key, Context, Value, State),
    {reply, Resp, NewState};
handle_call({get, Key}, _From, State) ->
    Resp = do_get(Key, State),
    {reply, Resp, State};
handle_call({delete, Key, Context}, _From, State) ->
    NewState = do_delete(Key, Context, State),
    {reply, ok, NewState};

handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(_Cast, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = error_logger:error_msg("unexpected cast received: ~p", [Cast]),
    {noreply, State}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(Info, State) ->
    ok = error_logger:error_msg("unexpected info ~p", [Info]),
    {noreply, State}.

-spec code_change(_, state(), _) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.


%%
%% local
%%
-spec self_ref(mg:ns()) ->
    mg_utils:gen_ref().
self_ref(Namespace) ->
    {via, gproc, gproc_key(Namespace)}.

-spec self_reg_name(mg:ns()) ->
    mg_utils:gen_reg_name().
self_reg_name(Namespace) ->
    {via, gproc, gproc_key(Namespace)}.

-spec gproc_key(mg:ns()) ->
    gproc:key().
gproc_key(Namespace) ->
    {n, l, wrap(Namespace)}.

-spec wrap(_) ->
    term().
wrap(V) ->
    {?MODULE, V}.

%%

-spec do_get(mg_storage:key(), state()) ->
    {mg_storage:context(), mg_storage:value()} | undefined.
do_get(Key, #{values := Values}) ->
    maps:get(Key, Values, undefined).

-spec do_put(mg_storage:key(), mg_storage:context(), mg_storage:value(), state()) ->
    {mg_storage:context(), state()}.
do_put(Key, Context, Value, State = #{values := Values}) ->
    case {do_get(Key, State), Context} of
        {undefined, undefined} ->
            NewContext = 0,
            {NewContext, State#{values := maps:put(Key, {NewContext, Value}, Values)}};
        {undefined, _} ->
            exit({not_found, Key});
        {{Context, _}, Context} ->
            NextContext = next_context(Context),
            {NextContext, State#{values := maps:put(Key, {NextContext, Value}, Values)}};
        {{OtherContext, _}, Context} ->
            % по текущей схеме (пишет всегда только один процесс) конфликтов никогда не должно быть
            exit({conflict, Context, OtherContext})
    end.

-spec do_delete(mg_storage:key(), mg_storage:context(), state()) ->
    state().
do_delete(Key, Context, State = #{values := Values}) ->
    case do_get(Key, State) of
        {Context, _} ->
            State#{values := maps:remove(Key, Values)};
        undefined ->
            State;
        {OtherContext, _} ->
            % по текущей схеме (пишет всегда только один процесс) конфликтов никогда не должно быть
            exit({conflict, Context, OtherContext})
    end.

%% это аналог vclock'а для тестов
-spec next_context(mg_storage:context()) ->
    mg_storage:context().
next_context(Context) ->
    Context + 1.
