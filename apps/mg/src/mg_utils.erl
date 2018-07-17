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
%%% То, чего не хватает в OTP.
%%% TODO перенести в genlib
%%%
-module(mg_utils).

%% API
%% OTP
-export_type([reason                    /0]).
-export_type([gen_timeout               /0]).
-export_type([gen_start_ret             /0]).
-export_type([gen_ref                   /0]).
-export_type([gen_reg_name              /0]).
-export_type([gen_server_from           /0]).
-export_type([gen_server_init_ret       /1]).
-export_type([gen_server_handle_call_ret/1]).
-export_type([gen_server_handle_cast_ret/1]).
-export_type([gen_server_handle_info_ret/1]).
-export_type([gen_server_code_change_ret/1]).
-export_type([supervisor_ret            /0]).
-export([gen_reg_name_to_ref/1]).
-export([gen_reg_name_to_pid/1]).
-export([gen_ref_to_pid     /1]).
-export([msg_queue_len      /1]).
-export([check_overload     /2]).

-export_type([supervisor_old_spec      /0]).
-export_type([supervisor_old_flags     /0]).
-export_type([supervisor_old_child_spec/0]).
-export([supervisor_old_spec      /1]).
-export([supervisor_old_flags     /1]).
-export([supervisor_old_child_spec/1]).

%% deadlines
-export_type([deadline/0]).
-export([timeout_to_deadline/1]).
-export([deadline_to_timeout/1]).
-export([unixtime_ms_to_deadline/1]).
-export([deadline_to_unixtime_ms/1]).
-export([is_deadline_reached/1]).
-export([default_deadline   /0]).

%% Woody
-export_type([woody_handlers/0]).
-export_type([woody_handler /0]).

%% Other
-export_type([mod_opts/0]).
-export_type([mod_opts/1]).
-export([apply_mod_opts            /2]).
-export([apply_mod_opts            /3]).
-export([apply_mod_opts_if_defined /3]).
-export([apply_mod_opts_if_defined /4]).
-export([separate_mod_opts         /1]).

-export([throw_if_error    /1]).
-export([throw_if_error    /2]).
-export([throw_if_undefined/2]).
-export([exit_if_undefined /2]).

-export_type([exception   /0]).
-export([raise            /1]).
-export([format_exception /1]).

-export([join/2]).

-export([lists_compact/1]).
-export([stop_wait_all/3]).

-export([concatenate_namespaces/2]).

%%
%% API
%% OTP
%%
-type reason() ::
      normal
    | shutdown
    | {shutdown, _}
    | _
.
-type gen_timeout() ::
      'hibernate'
    | timeout()
.

-type gen_start_ret() ::
      {ok, pid()}
    | ignore
    | {error, _}
.

-type gen_ref() ::
      atom()
    | {atom(), atom()}
    | {global, atom()}
    | {via, atom(), term()}
    | pid()
.
-type gen_reg_name() ::
      {local , atom()}
    | {global, term()}
    | {via, module(), term()}
.

-type gen_server_from() :: {pid(), _}.

-type gen_server_init_ret(State) ::
       ignore
    | {ok  , State   }
    | {stop, reason()}
    | {ok  , State   , gen_timeout()}
.

-type gen_server_handle_call_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {reply  , _Reply  , State        }
    | {stop   , reason(), State        }
    | {reply  , _Reply  , State        , gen_timeout()}
    | {stop   , reason(), _Reply       , State        }
.

-type gen_server_handle_cast_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {stop   , reason(), State        }
.

-type gen_server_handle_info_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {stop   , reason(), State        }
.

-type gen_server_code_change_ret(State) ::
      {ok   , State}
    | {error, _    }
.

-type supervisor_ret() ::
      ignore
    | {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}
.

-spec
gen_reg_name_to_ref(gen_reg_name()) -> gen_ref().
gen_reg_name_to_ref({local, Name} ) -> Name;
gen_reg_name_to_ref(V={global, _} ) -> V;
gen_reg_name_to_ref(V={via, _, _} ) -> V. % Is this correct?

-spec gen_reg_name_to_pid(gen_reg_name()) ->
    pid() | undefined.
gen_reg_name_to_pid({global, Name}) ->
    global:whereis_name(Name);
gen_reg_name_to_pid({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_reg_name_to_pid({local, Name})  ->
    erlang:whereis(Name).

-spec gen_ref_to_pid(gen_ref()) ->
    pid() | undefined.
gen_ref_to_pid(Name) when is_atom(Name) ->
    erlang:whereis(Name);
gen_ref_to_pid({Name, Node}) when is_atom(Name) andalso is_atom(Node) ->
    erlang:exit(not_implemented);
gen_ref_to_pid({global, Name}) ->
    global:whereis_name(Name);
gen_ref_to_pid({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_ref_to_pid(Pid) when is_pid(Pid) ->
    Pid.

-spec msg_queue_len(gen_ref()) ->
    non_neg_integer() | undefined.
msg_queue_len(Ref) ->
    Pid = exit_if_undefined(gen_ref_to_pid(Ref), noproc),
    {message_queue_len, Len} = exit_if_undefined(erlang:process_info(Pid, message_queue_len), noproc),
    Len.

-spec check_overload(gen_ref(), pos_integer()) ->
    ok | no_return().
check_overload(Ref, Limit) ->
    case msg_queue_len(Ref) < Limit of
        true  -> ok;
        false -> exit(overload)
    end.

-type supervisor_old_spec()       :: {supervisor_old_flags(), supervisor_old_child_spec()}.
-type supervisor_old_flags()      :: _TODO.
-type supervisor_old_child_spec() :: _TODO.

-spec supervisor_old_spec({supervisor:sup_flags(), [supervisor:child_spec()]}) ->
    supervisor_old_spec().
supervisor_old_spec({Flags, ChildSpecs}) ->
    {supervisor_old_flags(Flags), lists:map(fun supervisor_old_child_spec/1, ChildSpecs)}.

-spec supervisor_old_flags(supervisor:sup_flags()) ->
    supervisor_old_flags().
supervisor_old_flags(Flags = #{strategy := Strategy}) ->
    {Strategy, maps:get(intensity, Flags, 1), maps:get(period, Flags, 5)}.

-spec supervisor_old_child_spec(supervisor:child_spec()) ->
    supervisor_old_child_spec().
supervisor_old_child_spec(ChildSpec = #{id := ChildID, start := Start = {M, _, _}}) ->
    {
        ChildID,
        Start,
        maps:get(restart , ChildSpec, permanent),
        maps:get(shutdown, ChildSpec, 5000     ),
        maps:get(type    , ChildSpec, worker   ),
        maps:get(modules , ChildSpec, [M]      )
    }.

%%
%% deadlines
%%
-type deadline() :: undefined | pos_integer().

-spec timeout_to_deadline(timeout()) ->
    deadline().
timeout_to_deadline(infinity) ->
    undefined;
timeout_to_deadline(Timeout) when is_integer(Timeout) ->
    now_ms() + Timeout;
timeout_to_deadline(Timeout) ->
    erlang:error(badarg, [Timeout]).

-spec deadline_to_timeout(deadline()) ->
    timeout().
deadline_to_timeout(undefined) ->
    infinity;
deadline_to_timeout(Deadline) when is_integer(Deadline) ->
    erlang:max(Deadline - now_ms(), 0);
deadline_to_timeout(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec unixtime_ms_to_deadline(non_neg_integer()) ->
    deadline().
unixtime_ms_to_deadline(Deadline) when is_integer(Deadline) ->
    Deadline;
unixtime_ms_to_deadline(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec deadline_to_unixtime_ms(deadline()) ->
    non_neg_integer().
deadline_to_unixtime_ms(Deadline) when is_integer(Deadline) ->
    Deadline;
deadline_to_unixtime_ms(Deadline) ->
    erlang:error(badarg, [Deadline]).

-spec is_deadline_reached(deadline()) ->
    boolean().
is_deadline_reached(undefined) ->
    false;
is_deadline_reached(Deadline) ->
    Deadline - now_ms() =< 0.

-spec default_deadline() ->
    deadline().
default_deadline() ->
    %% For testing purposes only
    timeout_to_deadline(30000).

%%

-spec now_ms() ->
    pos_integer().
now_ms() ->
    erlang:system_time(1000).

%%
%% Woody
%%
-type woody_handlers() :: [woody_handler()].
-type woody_handler () :: _.

%%
%% Other
%%
-type mod_opts() :: mod_opts(term()).
-type mod_opts(Options) :: {module(), Options} | module().

-spec apply_mod_opts(mod_opts(), atom()) ->
    _Result.
apply_mod_opts(ModOpts, Function) ->
    apply_mod_opts(ModOpts, Function, []).

-spec apply_mod_opts(mod_opts(), atom(), list(_Arg)) ->
    _Result.
apply_mod_opts(ModOpts, Function, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    erlang:apply(Mod, Function, [Arg | Args]).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default) ->
    _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default) ->
    apply_mod_opts_if_defined(ModOpts, Function, Default, []).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default, list(_Arg)) ->
    _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    FunctionArgs = [Arg | Args],
    case erlang:function_exported(Mod, Function, length(FunctionArgs)) of
        true ->
            erlang:apply(Mod, Function, FunctionArgs);
        false ->
            Default
    end.

-spec separate_mod_opts(mod_opts()) ->
    {module(), _Arg}.
separate_mod_opts(ModOpts={_, _}) ->
    ModOpts;
separate_mod_opts(Mod) ->
    {Mod, undefined}.

-spec throw_if_error
    (ok             ) -> ok;
    ({ok   , Result}) -> Result;
    ({error, _Error}) -> no_return().
throw_if_error(ok) ->
    ok;
throw_if_error({ok, R}) ->
    R;
throw_if_error({error, Error}) ->
    erlang:throw(Error).

-spec throw_if_error
    (ok             , _ExceptionTag) -> ok;
    ({ok   , Result}, _ExceptionTag) -> Result;
    ({error, _Error}, _ExceptionTag) -> no_return().
throw_if_error(ok, _) ->
    ok;
throw_if_error({ok, R}, _) ->
    R;
throw_if_error(error, Exception) ->
    erlang:throw(Exception);
throw_if_error({error, Error}, Exception) ->
    erlang:throw({Exception, Error}).

-spec throw_if_undefined(Result, _Reason) ->
    Result | no_return().
throw_if_undefined(undefined, Reason) ->
    erlang:throw(Reason);
throw_if_undefined(Value, _) ->
    Value.

-spec exit_if_undefined(Result, _Reason) ->
    Result.
exit_if_undefined(undefined, Reason) ->
    erlang:exit(Reason);
exit_if_undefined(Value, _) ->
    Value.

-type exception() :: {exit | error | throw, term(), list()}.

-spec raise(exception()) ->
    no_return().
raise({Class, Reason, Stacktrace}) ->
    erlang:raise(Class, Reason, Stacktrace).

-spec format_exception(exception()) ->
    iodata().
format_exception({Class, Reason, Stacktrace}) ->
    io_lib:format("~s:~p ~s", [Class, Reason, genlib_format:format_stacktrace(Stacktrace, [newlines])]).


-spec join(D, list(E)) ->
    list(D | E).
join(_    , []   ) -> [];
join(_    , [H]  ) ->  H;
join(Delim, [H|T]) -> [H, Delim, join(Delim, T)].

-spec lists_compact(list(T)) ->
    list(T).
lists_compact(List) ->
    lists:filter(
        fun
            (undefined) -> false;
            (_        ) -> true
        end,
        List
    ).

-spec stop_wait_all([pid()], _Reason, timeout()) ->
    ok.
stop_wait_all(Pids, Reason, Timeout) ->
    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok      -> ok;
                timeout -> exit(stop_timeout)
            end
        end,
        Pids
    ).

-spec stop_wait(pid(), _Reason, timeout()) ->
    ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),
    erlang:exit(Pid, Reason),
    R =
        receive
            {'EXIT', Pid, Reason} -> ok
        after
            Timeout -> timeout
        end,
    process_flag(trap_exit, OldTrap),
    R.

-spec concatenate_namespaces(mg:ns(), mg:ns()) ->
    mg:ns().
concatenate_namespaces(NamespaceA, NamespaceB) ->
    <<NamespaceA/binary, "_", NamespaceB/binary>>.
