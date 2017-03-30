-module(mg_machine_logger).

%% API
-export_type([event    /0]).
-export_type([sub_event/0]).
-export_type([handler  /0]).
-export([handle_event/2]).

%%
%% API
%%
-type event() :: {mg:ns(), mg:id(), mg:request_context(), sub_event()}.
-type sub_event() ::
      {loading_failed       , mg_utils:exception()}  % при загрузке машины произошла ошибка
    | {machine_failed       , mg_utils:exception()}  % в работе машины произошла неожиданная ошибка
    | {transient_error      , mg_utils:exception()}  % в работе машины произошла временная ошибка
    | {retrying             , Delay::pos_integer()}  % повтор предыдущей операции после временной ошибки
    | {timer_handling_failed, Reason::term      ()}  % ошибка при обработки таймера
    | {resuming_interrupted_failed, Reason::term()}  % ошибка при переподнятии машины
.
-type handler() :: mg_utils:mod_opts(event()) | undefined.

-callback handle_machine_logging_event(_Options, event()) ->
    ok.

-spec handle_event(handler(), event()) ->
    ok.
handle_event(undefined, _Event) ->
    ok;
handle_event(Handler, Event) ->
    {Mod, Options} = mg_utils:separate_mod_opts(Handler),
    ok = Mod:handle_machine_logging_event(Options, Event).
