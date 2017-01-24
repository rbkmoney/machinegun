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
-export([gen_reg_name2_ref/1]).
-export([gen_where        /1]).
-export([get_msg_queue_len/1]).

%% Woody
-export_type([woody_handlers/0]).
-export_type([woody_handler /0]).

%% Other
-export_type([mod_opts/0]).
-export_type([mod_opts/1]).
-export([apply_mod_opts   /3]).
-export([separate_mod_opts/1]).

-export([throw_if_error   /1]).
-export([throw_if_error   /2]).
-export([exit_if_undefined/2]).
-export_type([exception   /0]).
-export([raise            /1]).
-export([format_exception /1]).

-export([join/2]).

-export_type([genlib_retry_policy/0]).
-export([genlib_retry_new/1]).

-export([stop_wait_all/3]).

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
gen_reg_name2_ref(gen_reg_name()) -> gen_ref().
gen_reg_name2_ref({local, Name} ) -> Name;
gen_reg_name2_ref(V={global, _} ) -> V;
gen_reg_name2_ref(V={via, _, _} ) -> V. % Is this correct?

-spec gen_where(gen_reg_name()) ->
    pid().
gen_where({global, Name}) ->
    global:whereis_name(Name);
gen_where({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_where({local, Name})  ->
    erlang:whereis(Name).

-spec get_msg_queue_len(gen_reg_name()) ->
    pos_integer() | undefined.
get_msg_queue_len(Name) ->
    Pid = exit_if_undefined(gen_where(Name), noproc),
    {message_queue_len, Len} = exit_if_undefined(erlang:process_info(Pid, message_queue_len), noproc),
    Len.


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

-spec apply_mod_opts(mod_opts(), atom(), list(_Arg)) ->
    _Result.
apply_mod_opts(ModOpts, Function, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    erlang:apply(Mod, Function, [Arg | Args]).

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

-spec exit_if_undefined (Result, _Reason) ->
    Result | no_return().
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
    io_lib:format("~s:~p~n~p", [Class, Reason, Stacktrace]).


-spec join(D, list(E)) ->
    list(D | E).
join(_    , []   ) -> [];
join(_    , [H]  ) ->  H;
join(Delim, [H|T]) -> [H, Delim, join(Delim, T)].

-type retries_num() :: pos_integer() | infinity.
-type genlib_retry_policy() ::
      {linear, retries_num() | {max_total_timeout, pos_integer()}, pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer(), timeout()}
    | {intervals, [pos_integer(), ...]}
    | {timecap, timeout(), genlib_retry_policy()}
.
-spec genlib_retry_new(genlib_retry_policy()) ->
    genlib_retry:strategy().
genlib_retry_new({linear, Retries, Timeout}) ->
    genlib_retry:linear(Retries, Timeout);
genlib_retry_new({exponential, Retries, Factor, Timeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout);
genlib_retry_new({exponential, Retries, Factor, Timeout, MaxTimeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout, MaxTimeout);
genlib_retry_new({intervals, Array}) ->
    genlib_retry:intervals(Array);
genlib_retry_new({timecap, Timeout, Policy}) ->
    genlib_retry:timecap(Timeout, genlib_retry_new(Policy));
genlib_retry_new(BadPolicy) ->
    erlang:error(badarg, [BadPolicy]).

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
