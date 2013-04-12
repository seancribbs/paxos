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
%% @doc The Leader process.
%% @end
-module(paxos_leader).

-behaviour(gen_server).

%% API
-export([start_link/2,
         propose/3,
         adopted/3,
         preempted/2,
         p1b/3,
         p2b/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          acceptors :: [ pid() ],
          replicas :: [ pid() ],
          ballot_num,
          active = false,
          proposals = orddict:new() :: orddict:orddict()
         }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Acceptors, Replicas) ->
    gen_server:start_link(?MODULE, [Acceptors, Replicas], []).

propose(Pid, Slot, PVal) ->
    gen_server:cast(Pid, {propose, Slot, PVal}).

adopted(Pid, Ballot, PVals) ->
    gen_server:cast(Pid, {adopted, Ballot, PVals}).

preempted(Pid, Ballot) ->
    gen_server:cast(Pid, {preempted, Ballot}).

p1b(Pid, Ballot, Accepted) ->
    %% This is actually being sent to the scout, not the leader
    gen_fsm:send_event(Pid, {p1b, self(), Ballot, Accepted}).

p2b(Pid, Ballot) ->
    %% This is actually being sent to the commander, not the leader
    gen_fsm:send_event(Pid, {p2b, self(), Ballot}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([Acceptors, Replicas]) ->
    process_flag(trap_exit, true),
    Ballot = {0, self()},
    paxos_scout:start_link(self(), Acceptors, Ballot),
    {ok,
     inactive,
     #state{
        acceptors = Acceptors,
        replicas = Replicas,
        ballot_num = Ballot
       }}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({propose, S, P}, #state{acceptors=Acceptors,
                                    replicas=Replicas,
                                    ballot_num=Ballot,
                                    active=Active,
                                    proposals=Proposals}=State) ->
    NewState = case orddict:is_key(S, Proposals) of
                   true ->
                       State;
                   false ->
                       NewProposals = orddict:store(S, P, Proposals),
                       maybe_start_commander(Active, Acceptors, Replicas, {Ballot, S, P}),
                       State#state{proposals=NewProposals}
                   end,
    {noreply, NewState};
handle_cast({adopted, Ballot, PVals}, #state{acceptors=Acceptors,
                                             replicas=Replicas,
                                             ballot_num=Ballot,
                                             proposals=Proposals}=State) ->
    NewProposals = update_proposals(Proposals, pmax(PVals)),
    [ paxos_commander:start_link(self(), Acceptors, Replicas, {Ballot, S, P}) ||
        {S, P} <- orddict:to_list(NewProposals) ],
    {noreply, State#state{proposals=NewProposals, active=true}};
handle_cast({preempted, {R,_}=Ballot0}, #state{ballot_num=Ballot,
                                               acceptors=Acceptors}=State) when Ballot0 > Ballot->
    NewBallot = {R+1, self()},
    paxos_scout:start_link(self, Acceptors, NewBallot),
    {noreply, State#state{ballot_num=NewBallot, active=false}};
handle_cast(_, State) ->
    {noreply, State}.


maybe_start_commander(false, _, _, _) ->
    ok;
maybe_start_commander(true, Acceptors, Replicas, Job) ->
    paxos_commander:start_link(self(), Acceptors, Replicas, Job).

pmax(Set) ->
    orddict:from_list([ {S,P} || {B,S,P} <- sets:to_list(Set),
                                 {B1, S1, _P1} <- sets:to_list(Set),
                                 S =:= S1,
                                 B1 =< B ]).

update_proposals(X, Y) ->
    orddict:merge(X, Y, fun(_XV, YV) -> YV end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
