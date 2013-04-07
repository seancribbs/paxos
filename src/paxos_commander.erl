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
%% @doc The Commander process.
-module(paxos_commander).

-behaviour(gen_fsm).

%% API
-export([start_link/4]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%% States
-export([start_phase2/2,
         start_phase2/3,
         collect/2,
         collect/3]).

-record(state, {
          leader :: pid(),
          acceptors :: [pid()],
          replicas :: [pid()],
          pvalue :: tuple(),
          threshold :: non_neg_integer(),
          replies = 0 :: non_neg_integer()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Leader, Acceptors, Replicas, PVal) ->
    gen_fsm:start_link(?MODULE, [Leader, Acceptors, Replicas, PVal], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Leader, Acceptors, Replicas, PVal]) ->
    %% Instead of removing pids from a "waitfor" set, we'll just keep
    %% count.
    Threshold = length(Acceptors) - (length(Acceptors) div 2),
    {ok, start_phase2,
     #state{
        leader = Leader,
        acceptors = Acceptors,
        replicas = Replicas,
        pvalue = PVal,
        threshold = Threshold
       },
     0}.

start_phase2(timeout, #state{acceptors=Acceptors, pvalue=PVal}=State) ->
    [ paxos_acceptor:p2a(A, PVal) || A <- Acceptors ],
    {next_state, collect, State}.

start_phase2(_Event, _From, State) ->
    {noreply, start_phase2, State}.

collect({p2b, _A, B0}, #state{replies=R, pvalue={B0,_,_}}=State) ->
    %% The ballot is the same, so we mark a reply
    continue(State#state{replies=R+1});
collect({p2b, _A, B0}, #state{leader=L}=State) ->
    %% The ballot is newer, so we abort and notify the leader
    paxos_leader:preempted(L, B0),
    {stop, normal, State}.

collect(_Event, _From, State) ->
    {noreply, collect, State}.

continue(#state{threshold=T, replies=R}=State) when R < T ->
    %% We haven't waited for enough yet, return to collect state.
    {next_state, collect, State};
continue(#state{replicas=Replicas, pvalue={_,S,P}}=State) ->
    [ paxos_replica:decision(R, S, P) || R <- Replicas ],
    {stop, normal, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
