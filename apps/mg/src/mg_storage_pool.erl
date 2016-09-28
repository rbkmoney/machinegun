-module(mg_storage_pool).

%% API
-export_type([options/0]).

-export([child_spec/1]).
-export([do        /2]).

%%
%% API
%%
%% в pooler'е нет типов :(
-type pooler_time_interval() :: {non_neg_integer(), min | sec | ms}.
-type pooler_pool_config  () :: [{atom(), term()}].

-type options() :: #{
    name                 => term(),
    start_mfa            => {atom(), atom(), list()},
    max_count            => non_neg_integer     (),
    init_count           => non_neg_integer     (),
    cull_interval        => pooler_time_interval(),
    max_age              => pooler_time_interval(),
    member_start_timeout => pooler_time_interval()
}.

-spec child_spec(options()) ->
    supervisor:child_spec().
child_spec(Options) ->
    % ChildID pooler генерит сам добавляя префикс _pooler_
    pooler:pool_child_spec(pooler_config(Options)).

-spec do(atom(), fun((pid()) -> R)) ->
    R.
do(PoolName, Fun) ->
    % TODO сделать нормально
    Timeout = {30, 'sec'},
    Pid =
        case pooler:take_member(PoolName, Timeout) of
            error_no_members ->
                % TODO log
                throw({temporary, storage_unavailable});
            Pid_ ->
                Pid_
        end,
    try
        R = Fun(Pid),
        ok = pooler:return_member(PoolName, Pid, ok),
        R
    catch Class:Reason ->
        ok = pooler:return_member(PoolName, Pid, fail),
        % TODO log
        erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

%%
%% local
%%
-spec pooler_config(options()) ->
    pooler_pool_config().
pooler_config(Options) ->
    maps:to_list(Options).
