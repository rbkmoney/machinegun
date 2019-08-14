-ifndef(__mg_ct_helper__).
-define(__mg_ct_helper__, 42).

-define(assertReceive(__Expr),
    ?assertReceive(__Expr, 1000)
).

-define(assertReceive(__Expr, __Timeout), (begin
    receive (__Expr) -> ok after (__Timeout) ->
        erlang:error({assertReceive, [
            {module, ?MODULE},
            {line, ?LINE},
            {expression, (??__Expr)},
            {mailbox, (fun __Flush(__Acc) ->
                receive __M -> __Flush([__M | __Acc]) after 0 -> __Acc end
            end)([])}
        ]})
    end
end)).

-endif.
