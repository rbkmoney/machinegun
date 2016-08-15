%%%
%%% Наблюдатель для машины.
%%% Получает все эвенты, которые производит машина.
%%% Один и тот же эвент может приходит несколько раз (например, при проблемах с машиной из-за ребута сервера.
%%%
-module(mg_observer).

%% API
-export([handle_events/3]).

%%
%% API
%%
-callback handle_events(_Options, mg:id(), [mg:event()]) ->
    ok.

%%

-spec handle_events(_Options, mg:id(), [mg:event()]) ->
    ok.
handle_events(Options, SourceID, Events) ->
    mg_utils:apply_mod_opts(Options, handle_events, [SourceID, Events]).
