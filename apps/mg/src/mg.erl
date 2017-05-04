-module(mg).

%% API
-export_type([ns/0]).
-export_type([id/0]).
-export_type([request_context/0]).

-type ns() :: binary().
-type id() :: binary().
-type request_context() :: mg_storage:opaque().
