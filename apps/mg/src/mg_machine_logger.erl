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
      {request_failed             , mg_utils:exception()}  % ошибка при обработки внешнего запроса
    | {timer_handling_failed      , mg_utils:exception()}  % ошибка при обработки таймера
    | {resuming_interrupted_failed, mg_utils:exception()}  % ошибка при переподнятии машины
    | {retrying                   , Delay::pos_integer()}
.
-type machine_event() ::
      {loading_failed             , mg_utils:exception()}  % при загрузке машины произошла ошибка
    | {machine_failed             , mg_utils:exception()}  % в работе машины произошла неожиданная ошибка
    | {transient_error            , mg_utils:exception()}  % в работе машины произошла временная ошибка
    | {retrying                   , Delay::pos_integer()}  % повтор предыдущей операции после временной ошибки
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
    ok = Mod:handle_machine_logging_event(Options, Event).
