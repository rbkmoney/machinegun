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
%% * reserve – выделенное эксклюзивно ему количество ресурса
%% * usage – использование им этого ресурса
%% * expectation – количество ресурса, которое клиент желал бы использовать
%% * share – количество полагающегося этому клиенту ресурса (задается
%%   и хранится в виде целого числа долей)
%% * target – целевое значение, к которому нужно привести reserve

%% В процессе работы target периодически пересчитывается для всех
%% потребителей исходя из значений остальных характеристик.

%% Менеджер не хранит информацию о прошлом, поэтому количество потребителей
%% должно быть меньше количества ресурса. В противном случае, части клиентов
%% может никогда не достаться ни единицы ресурса.

%% В итоге, для менеджера и входных данных подразумеваются следующие
%% ограничения:
%% * сумма всех target никогда не превышает limit
%% * сумма всех reserve никогда не превышает limit
%% * сумма всех share никогда не превышает limit и равна limit с точностью
%%   до ошибок округления
%% * reserve никогда не может быть меньше usage
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
    reserve :: resource(),
    usage :: resource(),
    expectation :: resource(),
    share :: share(),
    target :: resource() | undefined
}).
-type clients() :: #{client_id() => client_state()}.
-type client_state() :: #client_state{}.
-type resource_part() :: float().
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
    TotalShares = lists:sum([S || #client_state{share = S} <- maps:values(Clients)]),
    ResourcePerShare = Limit / TotalShares,
    NewClients = recalculate_clients_targets(ResourcePerShare, Limit, Clients),
    {ok, State#state{clients = NewClients}}.

-spec remove_client(client_id(), state()) ->
    state().
remove_client(ClientID, State) ->
    #state{clients = Clients, resource_state = Resource} = State,
    case maps:find(ClientID, Clients) of
        {ok, #client_state{reserve = Reserve}} ->
            {ok, Reserve, NewResource} = free_resource(Reserve, Resource),
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
    {ok, Feed, NewState} = free_resource(-Amount, State),
    {ok, -Feed, NewState};
change_resource(Amount, State) when Amount > 0 ->
    allocate_resource(Amount, State).

-spec allocate_resource(Amount :: resource(), resource_state()) ->
    {ok, Allocated :: resource(), resource_state()}.
allocate_resource(Amount, #resource_state{free = Free} = State) ->
    Allocated = erlang:min(Amount, Free),
    {ok, Allocated, State#resource_state{free = Free - Allocated}}.

-spec free_resource(Amount :: resource(), resource_state()) ->
    {ok, Feed :: resource(), resource_state()}.
free_resource(Amount, #resource_state{free = Free, limit = Limit} = State) ->
    Feed = erlang:min(Amount, Limit - Free),
    {ok, Feed, State#resource_state{free = Free + Feed}}.

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
                reserve = 0,
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
    erlang:min(erlang:max(Usage, Expectation), Target).

-spec calc_reserve_result(client_state()) ->
    NewTarget :: resource().
calc_reserve_result(#client_state{target = undefined, reserve = Reserve}) ->
    Reserve;
calc_reserve_result(#client_state{target = Target, reserve = Reserve}) ->
    erlang:min(Target, Reserve).

-spec try_reserve(ReserveTarget :: resource(), client_state(), resource_state()) ->
    {ok, client_state(), resource_state()}.
try_reserve(ReserveTarget, ClientState, Resource) ->
    #client_state{reserve = AlreadyReserved} = ClientState,
    Diff = ReserveTarget - AlreadyReserved,
    {ok, AcceptedChange, NewResource} = change_resource(Diff, Resource),
    NewReserved = AlreadyReserved + AcceptedChange,
    NewClientState = ClientState#client_state{reserve = NewReserved},
    {ok, NewClientState, NewResource}.

% TODO: rework
-spec recalculate_clients_targets(resource_part(), resource(), clients()) ->
    clients().
recalculate_clients_targets(ResourcePerShare, Limit, Clients) ->
    States = maps:values(Clients),
    do_recalculate_clients_targets(ResourcePerShare, Limit, States, #{}).

-spec do_recalculate_clients_targets(resource_part(), resource(), [client_state()], clients()) ->
    clients().
do_recalculate_clients_targets(_ResourcePerShare, _Limit, [], Clients) ->
    Clients;
do_recalculate_clients_targets(ResourcePerShare, 0 = Limit, [S | States], Clients) ->
    % no more resources left
    NewClients = Clients#{S#client_state.client_id => S},
    do_recalculate_clients_targets(ResourcePerShare, Limit, States, NewClients);
do_recalculate_clients_targets(ResourcePerShare, Limit, [S | States], Clients) ->
    #client_state{client_id = ID, share = Share} = S,
    NewTraget = erlang:max(erlang:trunc(ResourcePerShare * Share), Limit),
    NewS = S#client_state{target = NewTraget},
    do_recalculate_clients_targets(ResourcePerShare, Limit - NewTraget, States, Clients#{ID => NewS}).
