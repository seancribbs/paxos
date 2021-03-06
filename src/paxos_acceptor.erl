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
%% @doc The Acceptor process.
%% @end
-module(paxos_acceptor).

-behaviour(gen_server).

%% API
-export([start_link/0,
         p1a/2,
         p2a/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { ballot_num :: term(),
                 accepted :: set() }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

p1a(Pid, Ballot) ->
    gen_server:cast(Pid, {p1a, self(), Ballot}).

p2a(Pid, PVal) ->
    gen_server:cast(Pid, {p2a, self(), PVal}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% @todo Parameterize the memoization, perhaps with a callback module.
    %% Eventually we'd like this in something other than sets.
    {ok, #state{ accepted = sets:new() }}.

handle_call(_Msg, _From, State) ->
    {stop, unknown_call, State}.

handle_cast({p1a, Leader, B}, #state{ballot_num = BallotNum,
                                     accepted = Accepted}=State) ->
    NewState = case B > BallotNum of
                   true -> State#state{ballot_num=B};
                   false -> State
               end,
    paxos_leader:p1b(Leader, B, Accepted),
    {noreply, NewState};
handle_cast({p2a, Leader, {B,_,_}=Ballot}, #state{ballot_num=BallotNum,
                                                  accepted=Accepted}=State) ->
    NewState = case B >= BallotNum of
                   true ->
                       State#state{ballot_num=B,
                                   accepted=sets:add_element(Ballot, Accepted)};
                   false ->
                       State
               end,
    paxos_leader:p2b(Leader, NewState#state.ballot_num),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

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
handle_info(_Info, State) ->
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
