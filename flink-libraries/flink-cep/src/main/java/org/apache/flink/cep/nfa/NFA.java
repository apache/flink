/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.nfa;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import static org.apache.flink.cep.nfa.MigrationUtils.deserializeComputationStates;

/**
 * Non-deterministic finite automaton implementation.
 *
 * <p>The {@link AbstractKeyedCEPPatternOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *  <li>emitted (success)</li>
 *  <li>discarded (patterns containing NOT)</li>
 *  <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @param <T> Type of the processed events
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA<T> {

	/**
	 * A set of all the valid NFA states, as returned by the
	 * {@link NFACompiler NFACompiler}.
	 * These are directly derived from the user-specified pattern.
	 */
	private final Map<String, State<T>> states;

	/**
	 * The length of a windowed pattern, as specified using the
	 * {@link org.apache.flink.cep.pattern.Pattern#within(Time)}  Pattern.within(Time)}
	 * method.
	 */
	private final long windowTime;

	/**
	 * A flag indicating if we want timed-out patterns (in case of windowed patterns)
	 * to be emitted ({@code true}), or silently discarded ({@code false}).
	 */
	private final boolean handleTimeout;

	public NFA(
			final Collection<State<T>> validStates,
			final long windowTime,
			final boolean handleTimeout) {
		this.windowTime = windowTime;
		this.handleTimeout = handleTimeout;
		this.states = loadStates(validStates);
	}

	private Map<String, State<T>> loadStates(final Collection<State<T>> validStates) {
		Map<String, State<T>> tmp = new HashMap<>(4);
		for (State<T> state : validStates) {
			tmp.put(state.getName(), state);
		}
		return Collections.unmodifiableMap(tmp);
	}

	@VisibleForTesting
	public Collection<State<T>> getStates() {
		return states.values();
	}

	public NFAState createInitialNFAState() {
		Queue<ComputationState> startingStates = new LinkedList<>();
		for (State<T> state : states.values()) {
			if (state.isStart()) {
				startingStates.add(ComputationState.createStartState(state.getName()));
			}
		}
		return new NFAState(startingStates);
	}

	private State<T> getState(ComputationState state) {
		return states.get(state.getCurrentStateName());
	}

	private boolean isStartState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStart();
	}

	private boolean isStopState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStop();
	}

	private boolean isFinalState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isFinal();
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState The NFAState object that we need to affect while processing
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Map<String, List<T>>> process(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final T event,
			final long timestamp) throws Exception {
		return process(sharedBuffer, nfaState, event, timestamp, AfterMatchSkipStrategy.noSkip());
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState The NFAState object that we need to affect while processing
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @param afterMatchSkipStrategy The skip strategy to use after per match
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Map<String, List<T>>> process(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final T event,
			final long timestamp,
			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {
		try (EventWrapper eventWrapper = new EventWrapper(event, timestamp, sharedBuffer)) {
			return doProcess(sharedBuffer, nfaState, eventWrapper, afterMatchSkipStrategy);
		}
	}

	/**
	 * Prunes states assuming there will be no events with timestamp <b>lower</b> than the given one.
	 * It cleares the sharedBuffer and also emits all timed out partial matches.
	 *
	 * @param sharedBuffer the SharedBuffer object that we need to work upon while processing
	 * @param nfaState     The NFAState object that we need to affect while processing
	 * @param timestamp    timestamp that indicates that there will be no more events with lower timestamp
	 * @return all timed outed partial matches
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Tuple2<Map<String, List<T>>, Long>> advanceTime(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final long timestamp) throws Exception {

		Queue<ComputationState> computationStates = nfaState.getComputationStates();
		final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();

		final int numberComputationStates = computationStates.size();
		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState computationState = computationStates.poll();

			if (isStateTimedOut(computationState, timestamp)) {

				if (handleTimeout) {
					// extract the timed out event pattern
					Map<String, List<T>> timedOutPattern = extractCurrentMatches(sharedBuffer, computationState);
					timeoutResult.add(Tuple2.of(timedOutPattern, timestamp));
				}

				sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());

				nfaState.setStateChanged();
			} else {
				computationStates.add(computationState);
			}
		}

		sharedBuffer.advanceTime(timestamp);

		return timeoutResult;
	}

	private boolean isStateTimedOut(final ComputationState state, final long timestamp) {
		return !isStartState(state) && windowTime > 0L && timestamp - state.getStartTimestamp() >= windowTime;
	}

	private Collection<Map<String, List<T>>> doProcess(
			final SharedBuffer<T> sharedBuffer,
			final NFAState nfaState,
			final EventWrapper event,
			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {

		Queue<ComputationState> computationStates = nfaState.getComputationStates();

		final int numberComputationStates = computationStates.size();
		final Collection<Map<String, List<T>>> result = new ArrayList<>();

		// iterate over all current computations
		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState computationState = computationStates.poll();

			final Collection<ComputationState> newComputationStates = computeNextStates(
				sharedBuffer,
				computationState,
				event,
				event.getTimestamp());

			if (newComputationStates.size() != 1) {
				nfaState.setStateChanged();
			} else if (!newComputationStates.iterator().next().equals(computationState)) {
				nfaState.setStateChanged();
			}

			//delay adding new computation states in case a stop state is reached and we discard the path.
			final Collection<ComputationState> statesToRetain = new ArrayList<>();
			//if stop state reached in this path
			boolean shouldDiscardPath = false;
			for (final ComputationState newComputationState : newComputationStates) {

				if (isFinalState(newComputationState)) {
					// we've reached a final state and can thus retrieve the matching event sequence
					Map<String, List<T>> matchedPattern = extractCurrentMatches(sharedBuffer, newComputationState);
					result.add(matchedPattern);

					// remove found patterns because they are no longer needed
					sharedBuffer.releaseNode(newComputationState.getPreviousBufferEntry());
				} else if (isStopState(newComputationState)) {
					//reached stop state. release entry for the stop state
					shouldDiscardPath = true;
					sharedBuffer.releaseNode(newComputationState.getPreviousBufferEntry());
				} else {
					// add new computation state; it will be processed once the next event arrives
					statesToRetain.add(newComputationState);
				}
			}

			if (shouldDiscardPath) {
				// a stop state was reached in this branch. release branch which results in removing previous event from
				// the buffer
				for (final ComputationState state : statesToRetain) {
					sharedBuffer.releaseNode(state.getPreviousBufferEntry());
				}
			} else {
				computationStates.addAll(statesToRetain);
			}
		}

		discardComputationStatesAccordingToStrategy(
			sharedBuffer, computationStates, result, afterMatchSkipStrategy);

		return result;
	}

	private void discardComputationStatesAccordingToStrategy(
			final SharedBuffer<T> sharedBuffer,
			final Queue<ComputationState> computationStates,
			final Collection<Map<String, List<T>>> matchedResult,
			final AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {

		Set<T> discardEvents = new HashSet<>();
		switch(afterMatchSkipStrategy.getStrategy()) {
			case SKIP_TO_LAST:
				for (Map<String, List<T>> resultMap: matchedResult) {
					for (Map.Entry<String, List<T>> keyMatches : resultMap.entrySet()) {
						if (keyMatches.getKey().equals(afterMatchSkipStrategy.getPatternName())) {
							discardEvents.addAll(keyMatches.getValue().subList(0, keyMatches.getValue().size() - 1));
							break;
						} else {
							discardEvents.addAll(keyMatches.getValue());
						}
					}
				}
				break;
			case SKIP_TO_FIRST:
				for (Map<String, List<T>> resultMap: matchedResult) {
					for (Map.Entry<String, List<T>> keyMatches : resultMap.entrySet()) {
						if (keyMatches.getKey().equals(afterMatchSkipStrategy.getPatternName())) {
							break;
						} else {
							discardEvents.addAll(keyMatches.getValue());
						}
					}
				}
				break;
			case SKIP_PAST_LAST_EVENT:
				for (Map<String, List<T>> resultMap: matchedResult) {
					for (List<T> eventList: resultMap.values()) {
						discardEvents.addAll(eventList);
					}
				}
				break;
		}
		if (!discardEvents.isEmpty()) {
			List<ComputationState> discardStates = new ArrayList<>();
			for (ComputationState computationState : computationStates) {
				boolean discard = false;
				Map<String, List<T>> partialMatch = extractCurrentMatches(sharedBuffer, computationState);
				for (List<T> list: partialMatch.values()) {
					for (T e: list) {
						if (discardEvents.contains(e)) {
							// discard the computation state.
							discard = true;
							break;
						}
					}
					if (discard) {
						break;
					}
				}
				if (discard) {
					sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());
					discardStates.add(computationState);
				}
			}
			computationStates.removeAll(discardStates);
		}
	}

	private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
		return s1.getName().equals(s2.getName());
	}

	/**
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
	private static class OutgoingEdges<T> {
		private List<StateTransition<T>> edges = new ArrayList<>();

		private final State<T> currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State<T> currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition<T> edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}

		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition<T>> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition<T> edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}

	/**
	 * Helper class that ensures event is registered only once throughout the life of this object and released on close
	 * of this object. This allows to wrap whole processing of the event with try-with-resources block.
	 */
	private class EventWrapper implements AutoCloseable {

		private final T event;

		private long timestamp;

		private final SharedBuffer<T> sharedBuffer;

		private EventId eventId;

		EventWrapper(T event, long timestamp, SharedBuffer<T> sharedBuffer) {
			this.event = event;
			this.timestamp = timestamp;
			this.sharedBuffer = sharedBuffer;
		}

		EventId getEventId() throws Exception {
			if (eventId == null) {
				this.eventId = sharedBuffer.registerEvent(event, timestamp);
			}

			return eventId;
		}

		T getEvent() {
			return event;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public void close() throws Exception {
			if (eventId != null) {
				sharedBuffer.releaseEvent(eventId);
			}
		}
	}

	/**
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 *<ol>
	 *     <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}</li>
	 * 	   <li>Perform transitions:
	 * 	   	<ol>
	 *          <li>IGNORE (links in {@link SharedBuffer} will still point to the previous event)</li>
	 *          <ul>
	 *              <li>do not perform for Start State - special case</li>
	 *          	<li>if stays in the same state increase the current stage for future use with number of outgoing edges</li>
	 *          	<li>if after PROCEED increase current stage and add new stage (as we change the state)</li>
	 *          	<li>lock the entry in {@link SharedBuffer} as it is needed in the created branch</li>
	 *      	</ul>
	 *      	<li>TAKE (links in {@link SharedBuffer} will point to the current event)</li>
	 *          <ul>
	 *              <li>add entry to the shared buffer with version of the current computation state</li>
	 *              <li>add stage and then increase with number of takes for the future computation states</li>
	 *              <li>peek to the next state if it has PROCEED path to a Final State, if true create Final
	 *              ComputationState to emit results</li>
	 *          </ul>
	 *      </ol>
	 *     </li>
	 * 	   <li>Handle the Start State, as it always have to remain </li>
	 *     <li>Release the corresponding entries in {@link SharedBuffer}.</li>
	 *</ol>
	 *
	 * @param sharedBuffer The shared buffer that we need to change
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timestamp Timestamp of the current event
	 * @return Collection of computation states which result from the current one
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Collection<ComputationState> computeNextStates(
			final SharedBuffer<T> sharedBuffer,
			final ComputationState computationState,
			final EventWrapper event,
			final long timestamp) throws Exception {

		final ConditionContext<T> context = new ConditionContext<>(this, sharedBuffer, computationState);

		final OutgoingEdges<T> outgoingEdges = createDecisionGraph(context, computationState, event.getEvent());

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition<T>> edges = outgoingEdges.getEdges();
		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
		int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

		final List<ComputationState> resultingComputationStates = new ArrayList<>();
		for (StateTransition<T> edge : edges) {
			switch (edge.getAction()) {
				case IGNORE: {
					if (!isStartState(computationState)) {
						final DeweyNumber version;
						if (isEquivalentState(edge.getTargetState(), getState(computationState))) {
							//Stay in the same state (it can be either looping one or singleton)
							final int toIncrease = calculateIncreasingSelfState(
								outgoingEdges.getTotalIgnoreBranches(),
								outgoingEdges.getTotalTakeBranches());
							version = computationState.getVersion().increase(toIncrease);
						} else {
							//IGNORE after PROCEED
							version = computationState.getVersion()
								.increase(totalTakeToSkip + ignoreBranchesToVisit)
								.addStage();
							ignoreBranchesToVisit--;
						}

						addComputationState(
								sharedBuffer,
								resultingComputationStates,
								edge.getTargetState(),
								computationState.getPreviousBufferEntry(),
								version,
								computationState.getStartTimestamp()
						);
					}
				}
				break;
				case TAKE:
					final State<T> nextState = edge.getTargetState();
					final State<T> currentState = edge.getSourceState();

					final NodeId previousEntry = computationState.getPreviousBufferEntry();

					final DeweyNumber currentVersion = computationState.getVersion().increase(takeBranchesToVisit);
					final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
					takeBranchesToVisit--;

					final NodeId newEntry = sharedBuffer.put(
						currentState.getName(),
						event.getEventId(),
						previousEntry,
						currentVersion);

					final long startTimestamp;
					if (isStartState(computationState)) {
						startTimestamp = timestamp;
					} else {
						startTimestamp = computationState.getStartTimestamp();
					}

					addComputationState(
							sharedBuffer,
							resultingComputationStates,
							nextState,
							newEntry,
							nextVersion,
							startTimestamp);

					//check if newly created state is optional (have a PROCEED path to Final state)
					final State<T> finalState = findFinalStateAfterProceed(context, nextState, event.getEvent());
					if (finalState != null) {
						addComputationState(
								sharedBuffer,
								resultingComputationStates,
								finalState,
								newEntry,
								nextVersion,
								startTimestamp);
					}
					break;
			}
		}

		if (isStartState(computationState)) {
			int totalBranches = calculateIncreasingSelfState(
					outgoingEdges.getTotalIgnoreBranches(),
					outgoingEdges.getTotalTakeBranches());

			DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
			ComputationState startState = ComputationState.createStartState(computationState.getCurrentStateName(), startVersion);
			resultingComputationStates.add(startState);
		}

		if (computationState.getPreviousBufferEntry() != null) {
			// release the shared entry referenced by the current computation state.
			sharedBuffer.releaseNode(computationState.getPreviousBufferEntry());
		}

		return resultingComputationStates;
	}

	private void addComputationState(
			SharedBuffer<T> sharedBuffer,
			List<ComputationState> computationStates,
			State<T> currentState,
			NodeId previousEntry,
			DeweyNumber version,
			long startTimestamp) throws Exception {
		ComputationState computationState = ComputationState.createState(
				currentState.getName(), previousEntry, version, startTimestamp);
		computationStates.add(computationState);

		sharedBuffer.lockNode(previousEntry);
	}

	private State<T> findFinalStateAfterProceed(
			ConditionContext<T> context,
			State<T> state,
			T event) {
		final Stack<State<T>> statesToCheck = new Stack<>();
		statesToCheck.push(state);
		try {
			while (!statesToCheck.isEmpty()) {
				final State<T> currentState = statesToCheck.pop();
				for (StateTransition<T> transition : currentState.getStateTransitions()) {
					if (transition.getAction() == StateTransitionAction.PROCEED &&
							checkFilterCondition(context, transition.getCondition(), event)) {
						if (transition.getTargetState().isFinal()) {
							return transition.getTargetState();
						} else {
							statesToCheck.push(transition.getTargetState());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failure happened in filter function.", e);
		}

		return null;
	}

	private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
		return takeBranches == 0 && ignoreBranches == 0 ? 0 : ignoreBranches + Math.max(1, takeBranches);
	}

	private OutgoingEdges<T> createDecisionGraph(
			ConditionContext<T> context,
			ComputationState computationState,
			T event) {
		State<T> state = getState(computationState);
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);

		final Stack<State<T>> states = new Stack<>();
		states.push(state);

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state
			for (StateTransition<T> stateTransition : stateTransitions) {
				try {
					if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new FlinkRuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}

	private boolean checkFilterCondition(
			ConditionContext<T> context,
			IterativeCondition<T> condition,
			T event) throws Exception {
		return condition == null || condition.filter(event, context);
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param sharedBuffer The {@link SharedBuffer} from which to extract the matches
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Map<String, List<T>> extractCurrentMatches(
			final SharedBuffer<T> sharedBuffer,
			final ComputationState computationState) throws Exception {
		if (computationState.getPreviousBufferEntry() == null) {
			return new HashMap<>();
		}

		List<Map<String, List<T>>> paths = sharedBuffer.extractPatterns(
				computationState.getPreviousBufferEntry(),
				computationState.getVersion());

		if (paths.isEmpty()) {
			return new HashMap<>();
		}
		// for a given computation state, we cannot have more than one matching patterns.
		Preconditions.checkState(paths.size() == 1);

		Map<String, List<T>> result = new LinkedHashMap<>();
		Map<String, List<T>> path = paths.get(0);
		for (String key: path.keySet()) {
			List<T> events = path.get(key);

			List<T> values = result.computeIfAbsent(key, k -> new ArrayList<>(events.size()));
			values.addAll(events);
		}
		return result;
	}

	/**
	 * The context used when evaluating this computation state.
	 */
	private static class ConditionContext<T> implements IterativeCondition.Context<T> {

		/** The current computation state. */
		private ComputationState computationState;

		/**
		 * The matched pattern so far. A condition will be evaluated over this
		 * pattern. This is evaluated <b>only once</b>, as this is an expensive
		 * operation that traverses a path in the {@link SharedBuffer}.
		 */
		private Map<String, List<T>> matchedEvents;

		private NFA<T> nfa;

		private SharedBuffer<T> sharedBuffer;

		ConditionContext(
				final NFA<T> nfa,
				final SharedBuffer<T> sharedBuffer,
				final ComputationState computationState) {
			this.computationState = computationState;
			this.nfa = nfa;
			this.sharedBuffer = sharedBuffer;
		}

		@Override
		public Iterable<T> getEventsForPattern(final String key) throws Exception {
			Preconditions.checkNotNull(key);

			// the (partially) matched pattern is computed lazily when this method is called.
			// this is to avoid any overheads when using a simple, non-iterative condition.

			if (matchedEvents == null) {
				this.matchedEvents = nfa.extractCurrentMatches(sharedBuffer, computationState);
			}

			return new Iterable<T>() {
				@Override
				public Iterator<T> iterator() {
					List<T> elements = matchedEvents.get(key);
					return elements == null
						? Collections.EMPTY_LIST.<T>iterator()
						: elements.iterator();
				}
			};
		}
	}

	////////////////////				DEPRECATED/MIGRATION UTILS

	/**
	 * Wrapper for migrated state.
	 */
	public static class MigratedNFA<T> {

		private final Queue<ComputationState> computationStates;
		private final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer;

		public org.apache.flink.cep.nfa.SharedBuffer<T> getSharedBuffer() {
			return sharedBuffer;
		}

		public Queue<ComputationState> getComputationStates() {
			return computationStates;
		}

		MigratedNFA(
				final Queue<ComputationState> computationStates,
				final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer) {
			this.sharedBuffer = sharedBuffer;
			this.computationStates = computationStates;
		}
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} serializer configuration to be stored with the managed state.
	 */
	@Deprecated
	public static final class NFASerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty constructor is required for deserializing the configuration. */
		public NFASerializerConfigSnapshot() {}

		public NFASerializerConfigSnapshot(
				TypeSerializer<T> eventSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {

			super(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	/**
	 * Only for backward compatibility with <=1.5.
	 */
	@Deprecated
	public static class NFASerializer<T> extends TypeSerializer<MigratedNFA<T>> {

		private static final long serialVersionUID = 2098282423980597010L;

		private final TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer;

		private final TypeSerializer<T> eventSerializer;

		public NFASerializer(TypeSerializer<T> typeSerializer) {
			this(typeSerializer,
				new org.apache.flink.cep.nfa.SharedBuffer.SharedBufferSerializer<>(
					StringSerializer.INSTANCE,
					typeSerializer));
		}

		NFASerializer(
				TypeSerializer<T> typeSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = sharedBufferSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public NFASerializer<T> duplicate() {
			return new NFASerializer<>(eventSerializer.duplicate());
		}

		@Override
		public MigratedNFA<T> createInstance() {
			return null;
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from, MigratedNFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(MigratedNFA<T> record, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> deserialize(DataInputView source) throws IOException {
			MigrationUtils.skipSerializedStates(source);
			source.readLong();
			source.readBoolean();

			org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer = sharedBufferSerializer.deserialize(source);
			Queue<ComputationState> computationStates = deserializeComputationStates(sharedBuffer, eventSerializer, source);

			return new MigratedNFA<>(computationStates, sharedBuffer);
		}

		@Override
		public MigratedNFA<T> deserialize(
				MigratedNFA<T> reuse,
				DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()) &&
					sharedBufferSerializer.equals(((NFASerializer) obj).sharedBufferSerializer) &&
					eventSerializer.equals(((NFASerializer) obj).eventSerializer));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new NFASerializerConfigSnapshot<>(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public CompatibilityResult<MigratedNFA<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof NFASerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
					((NFASerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T> eventCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					serializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					serializersAndConfigs.get(0).f1,
					eventSerializer);

				CompatibilityResult<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufCompatResult =
					CompatibilityUtil.resolveCompatibilityResult(
						serializersAndConfigs.get(1).f0,
						UnloadableDummyTypeSerializer.class,
						serializersAndConfigs.get(1).f1,
						sharedBufferSerializer);

				if (!sharedBufCompatResult.isRequiresMigration() && !eventCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else {
					if (eventCompatResult.getConvertDeserializer() != null &&
						sharedBufCompatResult.getConvertDeserializer() != null) {
						return CompatibilityResult.requiresMigration(
							new NFASerializer<>(
								new TypeDeserializerAdapter<>(eventCompatResult.getConvertDeserializer()),
								new TypeDeserializerAdapter<>(sharedBufCompatResult.getConvertDeserializer())));
					}
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}
}
