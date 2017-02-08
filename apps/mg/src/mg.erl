-module(mg).

%% API
-export_type([opaque   /0]).
-export_type([ns       /0]).
-export_type([id       /0]).

-type opaque() :: null | true | false | number() | binary() | [opaque()] | #{opaque() => opaque()}.
-type ns    () :: binary().
-type id    () :: binary().
