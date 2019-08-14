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
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%% При невозможности загрузить падает с exit:{loading, Error}
%%%
%%% TODO:
%%%  - сделать выгрузку не по таймеру, а по занимаемой памяти и времени последней активности
%%%  -
%%%
-module(mg_workers_manager).

-include_lib("mg/include/pulse.hrl").

%% API
-export_type([options/0]).
-export_type([queue_limit/0]).

-export([child_spec    /2]).
-export([start_link    /1]).
-export([call          /5]).
-export([get_call_queue/2]).
-export([brutal_kill   /2]).
-export([is_alive      /2]).

%% Types
-type options() :: #{
    name                    => name(),
    message_queue_len_limit => queue_limit(),
    worker_options          => mg_worker:options(),
    pulse                   => mg_pulse:handler()
}.
-type queue_limit() :: non_neg_integer().

%% Internal types
-type id() :: mg:id().
-type name() :: mg:ns().
-type req_ctx() :: mg:request_context().
-type gen_ref() :: mg_utils:gen_ref().

%% Constants
-define(default_message_queue_len_limit, 50).

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
        self_reg_name(Options),
        #{strategy => simple_one_for_one},
        [
            mg_worker:child_spec(worker, maps:get(worker_options, Options))
        ]
    ).

% sync
-spec call(options(), id(), _Call, req_ctx(), mg_deadline:deadline()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline) ->
    case mg_deadline:is_reached(Deadline) of
        false ->
            call(Options, ID, Call, ReqCtx, Deadline, true);
        true ->
            {error, {transient, worker_call_deadline_reached}}
    end.

-spec call(options(), id(), _Call, req_ctx(), mg_deadline:deadline(), boolean()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline, CanRetry) ->
    #{name := Name, pulse := Pulse} = Options,
    try mg_worker:call(Name, ID, Call, ReqCtx, Deadline, Pulse) catch
        exit:Reason ->
            handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason, CanRetry)
    end.

-spec handle_worker_exit(options(), id(), _Call, req_ctx(), mg_deadline:deadline(), _Reason, boolean()) ->
    _Reply | {error, _}.
handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason, CanRetry) ->
    MaybeRetry = case CanRetry of
        true ->
            fun (_Details) -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline) end;
        false ->
            fun (Details) -> {error, {transient, Details}} end
    end,
    case Reason of
        % We have to take into account that `gen_server:call/2` wraps exception details in a
        % tuple with original call MFA attached.
        % > https://github.com/erlang/otp/blob/OTP-21.3/lib/stdlib/src/gen_server.erl#L215
        noproc             -> MaybeRetry(noproc);
        {noproc    , _MFA} -> MaybeRetry(noproc);
        {normal    , _MFA} -> MaybeRetry(normal);
        {shutdown  , _MFA} -> MaybeRetry(shutdown);
        {timeout   , _MFA} -> {error, Reason};
        {killed    , _MFA} -> {error, {transient, unavailable}};
        {{consuela , Details}, _MFA} ->
            {error, {transient, {registry_unavailable, Details}}};
        Unknown ->
            {error, {unexpected_exit, Unknown}}
    end.

-spec start_and_retry_call(options(), id(), _Call, req_ctx(), mg_deadline:deadline()) ->
    _Reply | {error, _}.
start_and_retry_call(Options, ID, Call, ReqCtx, Deadline) ->
    %
    % NOTE возможно тут будут проблемы и это место надо очень хорошо отсмотреть
    %  чтобы потом не ловить неожиданных проблем
    %
    % идея в том, что если нет процесса, то мы его запускаем
    %
    case start_child(Options, ID, ReqCtx) of
        {ok, _} ->
            call(Options, ID, Call, ReqCtx, Deadline, false);
        {error, {already_started, _}} ->
            call(Options, ID, Call, ReqCtx, Deadline, false);
        {error, Reason} ->
            handle_start_error(Reason)
    end.

-spec handle_start_error(_Reason) ->
    {error, _}.
handle_start_error({'EXIT', Reason}) ->
    % When server process startup exits in the context of `start_link/4` function
    handle_start_error(Reason);
handle_start_error({consuela, Reason}) ->
    {error, {transient, {registry_unavailable, Reason}}};
handle_start_error(Reason) ->
    {error, Reason}.

-spec get_call_queue(options(), id()) ->
    [_Call].
get_call_queue(Options, ID) ->
    try
        mg_worker:get_call_queue(maps:get(name, Options), ID)
    catch exit:noproc ->
        []
    end.

-spec brutal_kill(options(), id()) ->
    ok.
brutal_kill(Options, ID) ->
    try
        mg_worker:brutal_kill(maps:get(name, Options), ID)
    catch exit:noproc ->
        ok
    end.

-spec is_alive(options(), id()) ->
    boolean().
is_alive(Options, ID) ->
    mg_worker:is_alive(maps:get(name, Options), ID).


%%
%% local
%%
-spec start_child(options(), id(), req_ctx()) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID, ReqCtx) ->
    SelfRef = self_ref(Options),
    #{name := Name, pulse := Pulse} = Options,
    MsgQueueLimit = message_queue_len_limit(Options),
    MsgQueueLen = mg_utils:msg_queue_len(SelfRef),
    ok = mg_pulse:handle_beat(Pulse, #mg_worker_start_attempt{
        namespace = Name,
        machine_id = ID,
        request_context = ReqCtx,
        msg_queue_len = MsgQueueLen,
        msg_queue_limit = MsgQueueLimit
    }),
    case MsgQueueLen < MsgQueueLimit of
        true ->
            do_start_child(SelfRef, Name, ID, ReqCtx);
        false ->
            {error, {transient, overload}}
    end.

-spec do_start_child(gen_ref(), name(), id(), req_ctx()) ->
    {ok, pid()} | {error, term()}.
do_start_child(SelfRef, Name, ID, ReqCtx) ->
    try
        supervisor:start_child(SelfRef, [Name, ID, ReqCtx])
    catch
        exit:{timeout, Reason} ->
            {error, {timeout, Reason}}
    end.

-spec message_queue_len_limit(options()) ->
    queue_limit().
message_queue_len_limit(Options) ->
    maps:get(message_queue_len_limit, Options, ?default_message_queue_len_limit).

-spec self_ref(options()) ->
    gen_ref().
self_ref(Options) ->
    {via, gproc, gproc_key(Options)}.

-spec self_reg_name(options()) ->
    mg_utils:gen_reg_name().
self_reg_name(Options) ->
    {via, gproc, gproc_key(Options)}.

-spec gproc_key(options()) ->
    gproc:key().
gproc_key(Options) ->
    {n, l, wrap(maps:get(name, Options))}.

-spec wrap(_) ->
    term().
wrap(V) ->
    {?MODULE, V}.
