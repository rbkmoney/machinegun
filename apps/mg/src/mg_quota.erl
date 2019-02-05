%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_quota).

%% Менеджер ресурса, который пытается справедливо распределить ограниченный
%% запас этого ресурса (размером limit) между множеством потребителей.
%% Менеджер оперирует только целочисленным количеством ресурса.

%% Для каждого потребителя хранятся характеристики квоты:
%% * reserved – выделенное эксклюзивно ему количество ресурса
%% * usage – использование им этого ресурса
%% * expectation – количество ресурса, которое клиент желал бы использовать
%% * share – количество полагающегося этому клиенту ресурса (задается
%%   и хранится в виде целого числа долей). Гарантированная часть квоты для клиента
%%   вычисляется как limit * ( share / all_shares ).
%% * target – целевое значение, к которому нужно привести reserve

%% В процессе работы target периодически пересчитывается для всех
%% потребителей исходя из значений остальных характеристик.

%% Менеджер не хранит информацию о прошлом, поэтому количество потребителей
%% должно быть меньше количества ресурса. В противном случае, части клиентов
%% может никогда не достаться ни единицы ресурса.

%% В итоге, для менеджера и входных данных подразумеваются следующие
%% ограничения:
%% * сумма всех target никогда не превышает limit
%% * сумма всех reserved никогда не превышает limit
%% * сумма всех share никогда не превышает limit и равна limit с точностью
%%   до ошибок округления
%% * reserved никогда не может быть меньше usage
%% * если expectation меньше или равно reserve, то usage не больше expectation

%% API

-export([new/1]).
-export([reserve/4]).
-export([recalculate_targets/1]).
-export([remove_client/2]).

%% Types
-type options() :: #{
    limit := limit_options()
}.
-type client_options() :: #{
    client_id := client_id(),
    share := share()
}.
-type limit_options() :: #{
    value := resource()
}.
-record(state, {
    options :: options(),
    clients :: clients(),
    resource_state :: resource_state()
}).
-opaque state() :: #state{}.
-type share() :: non_neg_integer().
-type resource() :: non_neg_integer().
-type client_id() :: term().

-export_type([state/0]).
-export_type([share/0]).
-export_type([options/0]).
-export_type([resource/0]).
-export_type([client_id/0]).
-export_type([client_options/0]).
-export_type([limit_options/0]).

%% Internal types
-record(resource_state, {
    limit :: resource(),
    free :: resource()
}).
-record(client_state, {
    client_id :: client_id(),
    reserved :: resource(),
    usage :: resource(),
    expectation :: resource(),
    share :: share(),
    target :: resource() | undefined
}).
-type clients() :: #{client_id() => client_state()}.
-type client_state() :: #client_state{}.
-type resource_state() :: #resource_state{}.

%%
%% API
%%

-spec new(options()) ->
    state().
new(#{limit := #{value := Limit}} = Options) ->
    #state{
        options = Options,
        clients = #{},
        resource_state = new_resource_state(Limit)
    }.

-spec reserve(client_options(), Usage :: resource(), Expectation :: resource(), state()) ->
    {ok, NewTarget :: resource(), state()}.
reserve(#{client_id := ClientID} = Client, Usage, Expectation, State0) ->
    State = update_client_info(Client, Usage, Expectation, State0),
    #state{clients = Clients, resource_state = Resource} = State,
    ClientState = maps:get(ClientID, Clients),
    ReserveTarget = calc_reserve_target(ClientState),
    {ok, NewClientState, NewResource} = try_reserve(ReserveTarget, ClientState, Resource),
    NewTarget = calc_reserve_result(NewClientState),
    NewState = State#state{
        clients = Clients#{ClientID => NewClientState},
        resource_state = NewResource
    },
    {ok, NewTarget, NewState}.

-spec recalculate_targets(state()) ->
    {ok, state()}.
recalculate_targets(#state{clients = Clients} = State) when map_size(Clients) =:= 0 ->
    {ok, State};
recalculate_targets(State) ->
    #state{
        clients = Clients,
        resource_state = #resource_state{limit = Limit}
    } = State,
    NewClients = recalculate_clients_targets(Limit, Clients),
    {ok, State#state{clients = NewClients}}.

-spec remove_client(client_id(), state()) ->
    state().
remove_client(ClientID, State) ->
    #state{clients = Clients, resource_state = Resource} = State,
    case maps:find(ClientID, Clients) of
        {ok, #client_state{reserved = Reserved}} ->
            {ok, NewResource} = free_resource(Reserved, Resource),
            NewClients = maps:remove(ClientID, Clients),
            State#state{clients = NewClients, resource_state = NewResource};
        error ->
            State
    end.

%% Internals

% Resource state

-spec new_resource_state(Limit :: resource()) ->
    resource_state().
new_resource_state(Limit) ->
    #resource_state{
        limit = Limit,
        free = Limit
    }.

-spec change_resource(Amount :: integer(), resource_state()) ->
    {ok, Diff :: integer(), resource_state()}.
change_resource(0 = Amount, State) ->
    {ok, Amount, State};
change_resource(Amount, State) when Amount < 0 ->
    {ok, NewState} = free_resource(-Amount, State),
    {ok, Amount, NewState};
change_resource(Amount, State) when Amount > 0 ->
    allocate_resource(Amount, State).

-spec allocate_resource(Amount :: resource(), resource_state()) ->
    {ok, Allocated :: resource(), resource_state()}.
allocate_resource(Amount, #resource_state{free = Free} = State) ->
    Allocated = erlang:min(Amount, Free),
    {ok, Allocated, State#resource_state{free = Free - Allocated}}.

-spec free_resource(Amount :: resource(), resource_state()) ->
    {ok, resource_state()}.
free_resource(Amount, #resource_state{free = Free, limit = Limit} = State) ->
    NewFree = Free + Amount,
    true = Limit > NewFree,
    {ok, State#resource_state{free = NewFree}}.

% Client stats management

-spec update_client_info(client_options(), Usage :: resource(), Expectation :: resource(), state()) ->
    state().
update_client_info(ClientOptions, Usage, Expectation, State) ->
    #state{clients = Clients} = State,
    #{client_id := ClientID, share := Share} = ClientOptions,
    NewClientState = case maps:find(ClientID, Clients) of
        {ok, ClientState} ->
            ClientState#client_state{
                usage = Usage,
                expectation = Expectation,
                share = Share
            };
        error ->
            #client_state{
                client_id = ClientID,
                reserved = 0,
                usage = Usage,
                expectation = Expectation,
                share = Share,
                target = undefined
            }
    end,
    State#state{clients = Clients#{ClientID => NewClientState}}.

-spec calc_reserve_target(client_state()) ->
    ReserveTarget :: resource().
calc_reserve_target(#client_state{target = undefined, expectation = Expectation}) ->
    Expectation;
calc_reserve_target(#client_state{target = Target, expectation = Expectation, usage = Usage}) ->
    % Select number close to Expectation, but no more then Target, and no less then Usage
    erlang:min(erlang:max(Usage, Target), Expectation).

-spec calc_reserve_result(client_state()) ->
    NewTarget :: resource().
calc_reserve_result(#client_state{target = undefined, reserved = Reserved}) ->
    Reserved;
calc_reserve_result(#client_state{target = Target, reserved = Reserved}) ->
    erlang:min(Target, Reserved).

-spec try_reserve(ReserveTarget :: resource(), client_state(), resource_state()) ->
    {ok, client_state(), resource_state()}.
try_reserve(ReserveTarget, ClientState, Resource) ->
    #client_state{reserved = AlreadyReserved} = ClientState,
    Diff = ReserveTarget - AlreadyReserved,
    {ok, AcceptedChange, NewResource} = change_resource(Diff, Resource),
    NewReserved = AlreadyReserved + AcceptedChange,
    NewClientState = ClientState#client_state{reserved = NewReserved},
    {ok, NewClientState, NewResource}.

-spec recalculate_clients_targets(resource(), clients()) ->
    clients().
recalculate_clients_targets(Limit, ClientsMap) ->
    Clients = maps:values(ClientsMap),
    NewClients = case lists:sum([C#client_state.expectation || C <- Clients]) of
        TotalExpectation when TotalExpectation =< Limit ->
            % Turn off force resource management
            reset_client_targets(Clients);
        TotalExpectation when TotalExpectation > Limit ->
            % Turn on fair resource redistribution
            fair_share_resources(Clients, Limit)
    end,
    maps:from_list([{C#client_state.client_id, C} || C <- NewClients]).

-spec reset_client_targets([client_state()]) ->
    [client_state()].
reset_client_targets(Clients) ->
    [C#client_state{target = undefined} || C <- Clients].

-spec fair_share_resources([client_state()], resource()) ->
    [client_state()].
fair_share_resources(Clients, Limit) ->
    InitalClients = [
        C#client_state{target = 0} || C <- Clients
    ],
    do_fair_share_resources(InitalClients, Limit, []).

-spec do_fair_share_resources([client_state()], resource(), [client_state()]) ->
    [client_state()].
do_fair_share_resources([], _Limit, FinalClients) ->
    FinalClients;
do_fair_share_resources(Clients, 0, FinalClients) ->
    Clients ++ FinalClients;
do_fair_share_resources(Clients, Limit, FinalClients) ->
    {NeedyClients, NewFinalClients} = filter_needy_client(Clients, FinalClients),
    TotalGuaranteedShares = lists:sum([C#client_state.share || C <- NeedyClients]),
    do_fair_share_resources(TotalGuaranteedShares, NeedyClients, Limit, NewFinalClients).

-spec do_fair_share_resources(resource(), [client_state()], resource(), [client_state()]) ->
    [client_state()].
do_fair_share_resources(_TotalGuaranteedShares, [], _Limit, NewFinalClients) ->
    NewFinalClients;
do_fair_share_resources(0, Clients, Limit, FinalClients) ->
    Resource = Limit / erlang:length(Clients),
    NewClients = share_resource(fun get_no_share_target/2, Resource, Limit, Clients),
    do_fair_share_resources(NewClients, 0, FinalClients);
do_fair_share_resources(TotalGuaranteedShares, Clients, Limit, FinalClients) ->
    ResourcePerShare = Limit / TotalGuaranteedShares,
    NewClients = share_resource(fun get_share_target/2, ResourcePerShare, Limit, Clients),
    FreeResource = lists:sum([
        T - E
        || #client_state{expectation = E, target = T} <- NewClients, E < T
    ]),
    do_fair_share_resources(NewClients, FreeResource, FinalClients).

-spec filter_needy_client([client_state()], [client_state()]) ->
    {[client_state()], [client_state()]}.
filter_needy_client(Clients, FinalClients) ->
    {Needy, NotNeedy} = lists:partition(fun is_needy_client/1, Clients),
    {Needy, NotNeedy ++ FinalClients}.

-spec is_needy_client(client_state()) ->
    boolean().
is_needy_client(#client_state{expectation = E, target = T}) ->
    E > T.

-spec share_resource(Fun, Params, resource(), [client_state()]) -> [client_state()] when
    Params :: any(),
    Fun :: fun((client_state(), Params) -> resource()).
share_resource(Fun, Params, Limit, Clients) ->
    do_share_resource(Fun, Params, Limit, Clients, []).

-spec do_share_resource(Fun, Params, resource(), [client_state()], [client_state()]) -> [client_state()] when
    Params :: any(),
    Fun :: fun((client_state(), Params) -> resource()).
do_share_resource(_Fun, _Params, _Limit, [], Result) ->
    Result;
do_share_resource(_Fun, _Params, 0, Clients, Result) ->
    Clients ++ Result;
do_share_resource(Fun, Params, Limit, [Client | Clients], Result) when Limit > 0 ->
    TargetDiff = erlang:min(Fun(Client, Params), Limit),
    NewClient = Client#client_state{target = Client#client_state.target + TargetDiff},
    do_share_resource(Fun, Params, Limit - TargetDiff, Clients, [NewClient | Result]).

-spec get_share_target(client_state(), float()) ->
    resource().
get_share_target(#client_state{share = S}, ResourcePerShare) when S > 0 ->
    erlang:max(erlang:trunc(S * ResourcePerShare), 1);
get_share_target(#client_state{share = S}, _ResourcePerShare) when S =:= 0 ->
    0.

-spec get_no_share_target(client_state(), float()) ->
    resource().
get_no_share_target(_Client, Resource) ->
    erlang:max(erlang:trunc(Resource), 1).
