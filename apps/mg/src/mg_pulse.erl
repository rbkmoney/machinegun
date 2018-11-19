%%%
%%% Copyright 2018 RBKmoney
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
-module(mg_pulse).

-include_lib("mg/include/pulse.hrl").

%% API
-export_type([beat         /0]).
-export_type([handler      /0]).
-export([handle_beat/2]).

-callback handle_beat(Options :: any(), beat()) ->
    ok.

%%
%% API
%%
-type beat() ::
    % Таймер
      #mg_timer_lifecycle_created{}
    | #mg_timer_lifecycle_rescheduled{}
    | #mg_timer_lifecycle_rescheduling_error{}
    | #mg_timer_lifecycle_removed{}
    % Планировщик
    | #mg_scheduler_error{}
    % Обработка таймера
    | #mg_timer_process_started{}
    | #mg_timer_process_finished{}
    % Состояние процесса машины
    | #mg_machine_lifecycle_committed_suicide{}
    | #mg_machine_lifecycle_failed{}
    | #mg_machine_lifecycle_loading_error{}
    % Обработка запроса машиной
    | #mg_machine_process_continuation_started{}
    | #mg_machine_process_continuation_finished{}
    | #mg_machine_process_transient_error{}
    | #mg_machine_process_retry{}
    | #mg_machine_process_retries_exhausted{}.

-type handler() :: mg_utils:mod_opts() | undefined.

-spec handle_beat(handler(), any()) ->
    ok.
handle_beat(undefined, _Beat) ->
    ok;
handle_beat(Handler, Beat) ->
    {Mod, Options} = mg_utils:separate_mod_opts(Handler),
    try
        ok = Mod:handle_beat(Options, Beat)
    catch
        Class:Reason ->
            Stacktrace = genlib_format:format_stacktrace(erlang:get_stacktrace()),
            Msg = "Pulse handler ~p failed at beat ~p: ~p:~p ~s",
            error_logger:error_msg(Msg, [{Mod, Options}, Beat, Class, Reason, Stacktrace])
    end.
