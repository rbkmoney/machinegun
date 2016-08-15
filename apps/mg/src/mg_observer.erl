%%%
%%% Наблюдатель для машины.
%%% Получает все эвенты, которые производит машина.
%%% Один и тот же эвент может приходит несколько раз (например, при проблемах с машиной из-за ребута сервера.
%%%
-module(mg_observer).

%% API
-export([handle_event/3]).

%%
%% API
%%
-callback handle_event(_Options, mg:id(), mg:event()) ->
    ok.

-spec handle_event(_Options, mg:id(), mg:event()) ->
    ok.
handle_event(Options, ID, Event) ->
    mg_utils:apply_mod_opts(Options, handle_event, [ID, Event]).
