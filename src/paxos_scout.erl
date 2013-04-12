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
%% @doc The Scout process.
-module(paxos_scout).

-behaviour(gen_fsm).

%% API
-export([start_link/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% States
-export([start_phase1/2,
         start_phase1/3,
         waiting_p1b/2,
         waiting_p1b/3]).

-record(state, {
          leader :: pid(),
          acceptors :: [ pid() ],
          ballot,
          waitfor :: set(),
          pvalues = sets:new() :: set()
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Leader, Acceptors, Ballot) ->
    gen_fsm:start_link(?MODULE, [Leader, Acceptors, Ballot], []).

init([Leader, Acceptors, Ballot]) ->
    {ok, start_phase1,
     #state{
        leader = Leader,
        acceptors = Acceptors,
        ballot = Ballot,
        waitfor = sets:from_list(Acceptors)
       },
     0}.

start_phase1(timeout, #state{acceptors=Acceptors, ballot=Ballot}=State) ->
    [ paxos_acceptor:p1a(A, Ballot) || A <- Acceptors ],
    {next_state, collect, State}.

start_phase1(_Event, _From, State) ->
    {noreply, start_phase1, State}.

waiting_p1b({p1b,A,B0,R}, #state{leader=L, ballot=B0, waitfor=W, pvalues=PVals,
                                 acceptors=Acceptors}=State) ->
    case sets:is_element(A, W) of
        false ->
            {next_state, waiting_p1b, State};
        true ->
            NewState = State#state{waitfor = sets:del_element(A, W),
                                   pvalues = sets:union(R, PVals)},
            case sets:size(NewState#state.waitfor) < length(Acceptors) / 2 of
                false ->
                    {next_state, waiting_p1b, NewState};
                true ->
                    paxos_leader:adopted(L, B0, NewState#state.pvalues),
                    {stop, normal, NewState}
            end
    end;
waiting_p1b({p1b,A,B0,_R}, #state{leader=L, ballot=B, waitfor=W}=State) when B0 =/= B ->
    case sets:is_element(A,W) of
        false ->
            {next_state, waiting_p1b, State};
        true ->
            paxos_leader:preempted(L, B0),
            {stop, normal, State}
    end.

waiting_p1b(_Event, _From, State) ->
    {next_state, waiting_p1b, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
