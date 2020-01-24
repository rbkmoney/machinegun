-ifndef(__mg_ct_helper__).
-define(__mg_ct_helper__, 42).

-define(flushMailbox(__Acc0),
    (fun __Flush(__Acc) ->
        receive __M -> __Flush([__M | __Acc]) after 0 -> __Acc end
    end)(__Acc0)
).

-define(assertReceive(__Expr),
    ?assertReceive(__Expr, 1000)
).

-define(assertReceive(__Expr, __Timeout), (fun () ->
    receive (__Expr) = __V -> __V after (__Timeout) ->
        erlang:error({assertReceive, [
            {module, ?MODULE},
            {line, ?LINE},
            {expression, (??__Expr)},
            {mailbox, ?flushMailbox([])}
        ]})
    end
end)()).

-define(assertNoReceive(),
    ?assertNoReceive(1000)
).

-define(assertNoReceive(__Timeout), (fun () ->
    receive __Message ->
        erlang:error({assertNoReceive, [
            {module, ?MODULE},
            {line, ?LINE},
            {mailbox, ?flushMailbox([__Message])}
        ]})
    after (__Timeout) -> ok
    end
end)()).

-endif.
