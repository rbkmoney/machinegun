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

-module(mg_machine_logger).

%% API
-export_type([event        /0]).
-export_type([request_event/0]).
-export_type([machine_event/0]).
-export_type([handler      /0]).
-export([handle_event/2]).

%%
%% API
%%
-type event() ::
      {request_event, _TODO | undefined, mg:request_context() | null, request_event()}
    | {machine_event, mg:id(), mg:request_context(), machine_event()}
.
-type request_event() ::
      {request_failed             , mg_utils:exception()}  % ошибка при обработке внешнего запроса
    | {timer_handling_failed      , mg_utils:exception()}  % ошибка при обработке таймера
    | {timer_retry_handling_failed, mg_utils:exception()}  % ошибка при повторной обработке таймера
    | {resuming_interrupted_failed, mg_utils:exception()}  % ошибка при переподнятии машины
    | {retrying                   , Delay::pos_integer()}
.
-type machine_event() ::
      {loading_failed , mg_utils:exception()}  % при загрузке машины произошла ошибка
    | {machine_failed , mg_utils:exception()}  % в работе машины произошла неожиданная ошибка
    | {transient_error, mg_utils:exception()}  % в работе машины произошла временная ошибка
    | {retrying       , Delay::pos_integer()}  % повтор предыдущей операции после временной ошибки
    | {machine_resheduled, TS::genlib_time:ts(), Attempt::non_neg_integer()}  % обработка таймера отложена на будущее
    | {machine_resheduling_failed, mg_utils:exception()}  % ошибка при планировании повторной обработки
    |  committed_suicide  % машина совершила преднамеренное самоубийство
.
-type handler() :: mg_utils:mod_opts() | undefined.

-callback handle_machine_logging_event(_Options, event()) ->
    ok.

-spec handle_event(handler(), event()) ->
    ok.
handle_event(undefined, _Event) ->
    ok;
handle_event(Handler, Event) ->
    {Mod, Options} = mg_utils:separate_mod_opts(Handler),
    try
        ok = Mod:handle_machine_logging_event(Options, Event)
    catch
        Class:Reason ->
            Msg = "Event handler ~p failed at event ~p: ~p:~p ~p",
            error_logger:error_msg(Msg, [{Mod, Options}, Event, Class, Reason, erlang:get_stacktrace()])
    end.
