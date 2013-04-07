%% -------------------------------------------------------------------
%%
%% paxos: A multi-paxos implementation for Erlang
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc The Replica process and behaviour. To implement a Replica,
%% create a module that implements the perform(Operation, State)
%% function and start the replica like so:
%%
%%     paxos_replica:start_link(Module, InitialState, LeaderPids)
%%
%% where Module is the module implementing this behaviour,
%% InitialState is the state upon which the callback will operate,
%% and LeaderPids is a list of pids which are the leaders.
%%
%% When the Replica commits an operation, it will call
%% Module:perform/2 to retrieve the modified state and result of the
%% operation.
%% @end
-module(paxos_replica).

-behaviour(gen_server).

%% API
-export([start_link/3,
         request/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Implementation module callbacks
%% -callback perform(Operation::term(), State::term()) -> {NewState::term(), Result::term()}.
-export([behaviour_info/1]).

-record(state,
        {
          mod :: module(),
          mod_state :: any(),
          leaders = [] :: [pid()],
          slot_num = 1 :: pos_integer(),
          proposals :: ets:tid(),
          decisions :: ets:tid()
        }).

%%%===================================================================
%%% API
%%%===================================================================

-spec behaviour_info(atom()) -> term().
behaviour_info(callbacks) ->
    [{perform,2}];
behaviour_info(_) ->
    undefined.

-spec start_link(module(), any(), [pid()]) -> {ok, pid()} | ignore | {error, Error::any()}.
start_link(CallbackMod, InitState, Leaders) ->
    gen_server:start_link(?MODULE, [CallbackMod, InitState, Leaders], []).

-spec request(pid(), any()) -> any().
request(Pid, Operation) ->
    gen_server:call(Pid, {request, Operation}, infinity).

-spec decision(pid(), pos_integer(), any()) -> ok.
decision(Pid, Slot, PVal) ->
    gen_server:cast(Pid, {decision, {Slot, PVal}}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, InitState, Leaders]) ->
    {ok,
     #state{mod = Mod,
            mod_state = InitState,
            leaders = Leaders,
            proposals = ets:new(proposals, [ordered_set]),
            decisions = ets:new(decisions, [ordered_set])}
    }.

handle_call({request, Op}, From, State) ->
    NewState = propose({Op, From}, State),
    {noreply, NewState};
handle_call(_, _, State) ->
    {stop, unknown_call, State}.

handle_cast({decision, {S, PVal}}, State) ->
    NewState = decide(S, PVal, State),
    {noreply, NewState};
handle_cast(_, State) ->
    {stop, unknown_cast, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
propose(PVal, #state{slot_num=Slot,
                     proposals=Proposals,
                     decisions=Decisions,
                     leaders=Leaders} = State) ->
    case ets:match(Decisions, {'$1', PVal}) of
        [[ _ ]] -> ok;  % This has already been decided
        _ ->
            %% Proposals and Decisions are ordered_sets, so the last
            %% of each will have the max slot number recorded in them.
            %% The max of those and the slot_num will be the upper
            %% bound of them all.
            MaxSeq = lists:max([Slot, safe_last(Proposals), safe_last(Decisions)]),
            S = MaxSeq+1,
            ets:insert(Proposals, {S, PVal}),
            riak_paxos_leader:propose(Leaders, {S, PVal}),
            State
    end.

decide(S, PVal, #state{slot_num=Slot, decisions=Decisions} = State) ->
    ets:insert(Decisions, {S, PVal}),
    process_decisions(ets:lookup(Decisions, Slot), State).

process_decisions([], #state{}=State) ->
    %% We have processed all decisions already, return the new state.
    State;
process_decisions([{S, PVal}], #state{proposals=Proposals,
                                      decisions=Decisions}=State) ->
    case ets:lookup(Proposals, S) of
        [ {S, PVal1} ] when PVal1 =/= PVal ->
            %% The slot number is the same, but proposal different, so
            %% we resend the proposal
            propose(PVal1, State);
        _ -> ok
    end,
    %% Perform the decision, updating the internal state.
    NewState = perform(PVal, State),
    %% Process the next decision that matches the new slot number.
    process_decisions(ets:lookup(Decisions, NewState#state.slot_num), NewState).

perform({Op, From}=PVal, #state{mod=Mod, mod_state=ModState,
                                slot_num=Slot, decisions=Decisions}=State) ->
    case ets:match(Decisions, {{'$1', PVal}, [{'<', '$1', Slot}], [true]}) of
        [true] ->
            %% There is a decision that has the same signature but a
            %% lower slot number, so we don't execute, just get the
            %% next decision.
            State#state{slot_num = Slot + 1};
        _ ->
            {Result, NewModState} = Mod:perform(Op, ModState),
            gen_server:reply(From, Result),
            State#state{slot_num=Slot + 1, mod_state=NewModState}
    end.

safe_last(Tid) ->
    case ets:last(Tid) of
        '$end_of_table' -> 0; %% returns '$end_of_table' when empty
        I -> I
    end.
