-module(mg_storage_memory).
-include_lib("stdlib/include/ms_transform.hrl").

%% internal API
-export([start_link/2]).

%% mg_storage callbacks
-behaviour(mg_storage).
-export_type([options/0]).
-export([child_spec/3, do_request/3]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% internal API
%%
-spec start_link(options(), mg_utils:gen_reg_name()) ->
    mg_utils:gen_start_ret().
start_link(Options, RegName) ->
    gen_server:start_link(RegName, ?MODULE, Options, []).

%%
%% mg_storage callbacks
%%
-type context      () :: non_neg_integer() | undefined.
-type options      () :: undefined.
-type continuation () :: binary() | undefined.
-type self_ref     () :: mg_utils:gen_ref().


-spec child_spec(options(), atom(), mg_utils:gen_reg_name()) ->
    supervisor:child_spec().
child_spec(Options, ChildID, RegName) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options, RegName]},
        restart  => permanent,
        shutdown => 5000
    }.

-spec do_request(options(), self_ref(), mg_storage:request()) ->
    mg_storage:response().
do_request(_, SelfRef, Req) ->
    gen_server:call(SelfRef, Req).

%%
%% gen_server callbacks
%%
-type state() :: #{
    options     => options(),
    values      => #{mg_storage:key() => {context(), mg_storage:value()}},
    indexes     => #{mg_storage:index_name() => index()}
}.
-type index() :: [{mg_storage:index_value(), mg_storage:key()}].
-type search_result() :: [{mg_storage:index_value(), mg_storage:key()}] | [mg_storage:key()].

-spec init(options()) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    {ok,
        #{
            options   => Options,
            values    => #{},
            indexes   => #{}
        }
    }.

-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()) | no_return().
handle_call({put, Key, Context, Value, IndexesUpdates}, _From, State) ->
    {Resp, NewState} = do_put(Key, Context, Value, IndexesUpdates, State),
    {reply, Resp, NewState};
handle_call({get, Key}, _From, State) ->
    Resp = do_get(Key, State),
    {reply, Resp, State};
handle_call({search, Query}, _From, State) ->
    {Resp, NewState} = do_search(Query, State),
    {reply, Resp, NewState};
handle_call({delete, Key, Context}, _From, State) ->
    NewState = do_delete(Key, Context, State),
    {reply, ok, NewState};

%% этот сторадж создан больше для тестирования,
%% поэто если в этот сторадж пришли странные запросы,
%% то лучше сразу падать
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


%%
%% local
%%
-spec do_get(mg_storage:key(), state()) ->
    {context(), mg_storage:value()} | undefined.
do_get(Key, #{values := Values}) ->
    maps:get(Key, Values, undefined).

-spec do_search(mg_storage:index_query(), state()) ->
    {{search_result(), continuation()}, state()}.
do_search({IndexName, QueryValue}, State) ->
    do_search({IndexName, QueryValue, inf}, State);
do_search({IndexName, QueryValue, Limit}, State) ->
    do_search({IndexName, QueryValue, Limit, undefined}, State);
do_search({IndexName, QueryValue, inf, _}, State = #{indexes := Indexes}) ->
    Res = do_search_index(maps:get(IndexName, Indexes, []), QueryValue),
    {Res, State};
do_search({IndexName, QueryValue, IndexLimit, undefined}, State = #{indexes := Indexes}) ->
    Res = do_search_index(maps:get(IndexName, Indexes, []), QueryValue),
    {generate_search_response(split_search_result(Res, IndexLimit)), State};
do_search({IndexName, QueryValue, IndexLimit, Cont}, State = #{indexes := Indexes}) ->
    Res = find_continuation(do_search_index(maps:get(IndexName, Indexes, []), QueryValue), Cont),
    {generate_search_response(split_search_result(Res, IndexLimit)), State}.

-spec find_continuation(search_result(), continuation()) ->
    search_result().
find_continuation(Result, undefined) ->
    Result;
find_continuation(Result, Cont) ->
    Key = binary_to_term(Cont),
    start_from_elem(Key, Result).

-spec split_search_result(search_result(), mg_storage:index_limit()) ->
    {search_result(), search_result()}.
split_search_result(SearchResult, IndexLimit) ->
    case IndexLimit >= erlang:length(SearchResult) of
        true ->
            {SearchResult, []};
        false ->
            lists:split(IndexLimit, SearchResult)
    end.

-spec generate_search_response({search_result(), search_result()}) ->
    {search_result(), continuation()}.
generate_search_response({[], _Remains}) ->
    {[], undefined};
generate_search_response({SearchResult, _Remains}) ->
    {SearchResult, generate_continuation(SearchResult)}.

-spec generate_continuation(search_result()) ->
    continuation().
generate_continuation(Result) ->
    term_to_binary(lists:last(Result)).

-spec do_put(mg_storage:key(), context(), mg_storage:value(), [mg_storage:index_update()], state()) ->
    {context(), state()}.
do_put(Key, Context, Value, IndexesUpdates, State0 = #{values := Values}) ->
    % по текущей схеме (пишет всегда только один процесс) конфликтов никогда не должно быть
    NextContext =
        case {do_get(Key, State0), Context} of
            {undefined        , undefined} -> 0;
            {{Context, _}     , Context  } -> next_context(Context);
            {undefined        , _        } -> exit({not_found, Key});
            {{OtherContext, _}, Context  } -> exit({conflict, Context, OtherContext})
        end,
    State1 = State0#{values := maps:put(Key, {NextContext, Value}, Values)},
    {NextContext, do_update_indexes(IndexesUpdates, Key, do_cleanup_indexes(Key, State1))}.

-spec do_delete(mg_storage:key(), context(), state()) ->
    state().
do_delete(Key, Context, State = #{values := Values}) ->
    case do_get(Key, State) of
        {Context, _} ->
            NewState = State#{values := maps:remove(Key, Values)},
            do_cleanup_indexes(Key, NewState);
        undefined ->
            State;
        {OtherContext, _} ->
            % по текущей схеме (пишет всегда только один процесс) конфликтов никогда не должно быть
            exit({conflict, Context, OtherContext})
    end.

%% это аналог vclock'а для тестов
-spec next_context(context()) ->
    context().
next_context(Context) ->
    Context + 1.

%%
%% index
%%
-spec do_search_index(index(), mg_storage:index_query_value()) ->
    search_result().
do_search_index(Index, QueryValue) ->
    lists:foldr(
        fun({IndexValue, Key}, ResultAcc) ->
            case does_value_satisfy_query(QueryValue, IndexValue) of
                true  -> [index_search_result(IndexValue, Key, QueryValue) | ResultAcc];
                false -> ResultAcc
            end
        end,
        [],
        Index
    ).

-spec index_search_result(mg_storage:index_value(), mg_storage:key(), mg_storage:index_query_value()) ->
    {mg_storage:index_value(), mg_storage:key()} | mg_storage:key().
index_search_result(IndexValue, Key, {_, _}) ->
    {IndexValue, Key};
index_search_result(_, Key, _) ->
    Key.

%% Очень тупое название, но ничего лучше в голову не пришло.
-spec does_value_satisfy_query(mg_storage:index_query_value(), mg_storage:index_value()) ->
    boolean().
does_value_satisfy_query({From, To}, Value) ->
    From =< Value andalso Value =< To;
does_value_satisfy_query(Equal, Value) ->
    Equal =:= Value.


-spec do_update_indexes([mg_storage:index_update()], mg_storage:key(), state()) ->
    state().
do_update_indexes(IndexesUpdates, Key, State) ->
    lists:foldl(
        fun(IndexUpdate, StateAcc) ->
            do_update_index(IndexUpdate, Key, StateAcc)
        end,
        State,
        IndexesUpdates
    ).

-spec do_update_index(mg_storage:index_update(), mg_storage:key(), state()) ->
    state().
do_update_index(IndexUpdate={IndexName, IndexValue}, Key, State = #{indexes := Indexes}) ->
    ok = check_index_update(IndexUpdate),
    Index    = maps:get(IndexName, Indexes, []),
    NewIndex = lists:sort([{IndexValue, Key} | Index]),
    State#{indexes := maps:put(IndexName, NewIndex, Indexes)}.

-spec check_index_update(mg_storage:index_update()) ->
    ok | no_return().
check_index_update({{binary , Name}, Value}) when is_binary(Name) andalso is_binary (Value) -> ok;
check_index_update({{integer, Name}, Value}) when is_binary(Name) andalso is_integer(Value) -> ok.

-spec do_cleanup_indexes(mg_storage:key(), state()) ->
    state().
do_cleanup_indexes(Key, State=#{indexes := Indexes}) ->
    NewIndexes =
        maps:map(
            fun(_, Index) ->
                do_cleanup_index(Key, Index)
            end,
            Indexes
        ),
    State#{indexes := NewIndexes}.

-spec do_cleanup_index(mg_storage:key(), index()) ->
    index().
do_cleanup_index(Key, Index) ->
    lists:foldr(
        fun
            ({_IndexValue, Key_}, AccIndex) when Key_ =:= Key ->
                AccIndex;
            ({IndexValue, OtherKey}, AccIndex) ->
                [{IndexValue, OtherKey} | AccIndex]
        end,
        [],
        Index
    ).

%% utils
-spec start_from_elem(any(), list()) ->
    list().
start_from_elem(_, [])  ->
    [];
start_from_elem(Item, [Item|Tail]) ->
    Tail;
start_from_elem(Item, [_|Tail]) ->
    start_from_elem(Item, Tail).
