-module(mg).

%% API
-export_type([opaque   /0]).
-export_type([ns       /0]).
-export_type([id       /0]).

% it's a copy from msgpack_term()
-type opaque() :: null | true | false | number() | binary() | {string, string()} | [opaque()] | #{opaque() => opaque()}.
-type ns    () :: binary().
-type id    () :: binary().
