%% @doc The basic replica code from Paxos Made Moderately Complex
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

-spec request(pid(), any()) -> ok.
request(Pid, Operation) ->
    gen_server:call(Pid, {request, Operation}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, InitState, Leaders]) ->
    Proposals = ets:new(proposals, [ordered_set]),
    Decisions = ets:new(decisions, [ordered_set]),
    {ok,
     #state{
        mod = Mod,
        mod_state = InitState,
        leaders = Leaders,
        proposals = Proposals,
        decisions = Decisions
       }}.

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
process_decisions([{S, PVal}=D], #state{proposals=Proposals, 
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
            %% lower slot number.
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
