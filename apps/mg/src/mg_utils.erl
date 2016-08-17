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

%% Woody
-export_type([woody_handlers/0]).
-export_type([woody_handler /0]).

%% Other
-export_type([mod_opts/0]).
-export([apply_mod_opts   /3]).
-export([separate_mod_opts/1]).

-export([throw_if_error/2]).

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
gen_reg_name2_ref({local, Name}) -> Name;
gen_reg_name2_ref(V={global, _}) -> V;
gen_reg_name2_ref(V={via, _, _}) -> V. % Is this correct?


%%
%% Woody
%%
-type woody_handlers() :: [woody_handler()].
-type woody_handler () :: _.

%%
%% Other
%%
-type mod_opts() :: {module(), _Opts} | module().
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

-spec throw_if_error(ok | error | tuple(), _) ->
    _.
throw_if_error(ok, _) ->
    ok;
throw_if_error({ok, R}, _) ->
    R;
throw_if_error(OK, _) when is_tuple(OK) andalso element(1, OK) =:= ok ->
    erlang:delete_element(1, OK);
throw_if_error(error, Exception) ->
    throw(Exception);
throw_if_error({error, Error}, Exception) ->
    throw({Exception, Error});
throw_if_error(Error, Exception) when is_tuple(Error) andalso element(1, Error) =:= error ->
    throw({Exception, erlang:delete_element(1, Error)}).
