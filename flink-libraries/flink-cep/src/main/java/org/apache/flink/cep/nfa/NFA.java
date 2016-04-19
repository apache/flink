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

import com.google.common.collect.LinkedHashMultimap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.NonDuplicatingTypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Non-deterministic finite automaton implementation.
 * <p>
 * The NFA processes input events which will chnage the internal state machine. Whenever a final
 * state is reached, the matching sequence of events is emitted.
 *
 * The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <T> Type of the processed events
 */
public class NFA<T> implements Serializable {

	private static final Pattern namePattern = Pattern.compile("^(.*\\[)(\\])$");
	private static final long serialVersionUID = 2957674889294717265L;

	private final NonDuplicatingTypeSerializer<T> nonDuplicatingTypeSerializer;

	// Buffer used to store the matched events
	private final SharedBuffer<State<T>, T> sharedBuffer;

	// Set of all NFA states
	private final Set<State<T>> states;

	// Length of the window
	private final long windowTime;

	// Current starting index for the next dewey version number
	private int startEventCounter;

	// Current set of computation states within the state machine
	private transient Queue<ComputationState<T>> computationStates;

	public NFA(final TypeSerializer<T> eventSerializer, final long windowTime) {
		this.nonDuplicatingTypeSerializer = new NonDuplicatingTypeSerializer<>(eventSerializer);
		this.windowTime = windowTime;
		sharedBuffer = new SharedBuffer<>(nonDuplicatingTypeSerializer);
		computationStates = new LinkedList<>();

		states = new HashSet<>();
		startEventCounter = 1;
	}

	public Set<State<T>> getStates() {
		return states;
	}

	public void addStates(final Collection<State<T>> newStates) {
		for (State<T> state: newStates) {
			addState(state);
		}
	}

	public void addState(final State<T> state) {
		states.add(state);

		if (state.isStart()) {
			computationStates.add(new ComputationState<>(state, null, -1L, null, -1L));
		}
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned.
	 *
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the current event
	 * @return The collection of matched patterns (e.g. the result of computations which have
	 * reached a final state)
	 */
	public Collection<Map<String, T>> process(final T event, final long timestamp) {
		final int numberComputationStates = computationStates.size();
		final List<Map<String, T>> result = new ArrayList<>();

		// iterate over all current computations
		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = computationStates.poll();

			final Collection<ComputationState<T>> newComputationStates;

			if (!computationState.isStartState() &&
				windowTime > 0 &&
				timestamp - computationState.getStartTimestamp() >= windowTime) {
				// remove computation state which has exceeded the window length
				sharedBuffer.release(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
				sharedBuffer.remove(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());

				newComputationStates = Collections.emptyList();
			} else {
				newComputationStates = computeNextStates(computationState, event, timestamp);
			}

			for (ComputationState<T> newComputationState: newComputationStates) {
				if (newComputationState.isFinalState()) {
					// we've reached a final state and can thus retrieve the matching event sequence
					Collection<Map<String, T>> matches = extractPatternMatches(newComputationState);
					result.addAll(matches);

					// remove found patterns because they are no longer needed
					sharedBuffer.release(newComputationState.getState(), newComputationState.getEvent(), newComputationState.getTimestamp());
					sharedBuffer.remove(newComputationState.getState(), newComputationState.getEvent(), newComputationState.getTimestamp());
				} else {
					// add new computation state; it will be processed once the next event arrives
					computationStates.add(newComputationState);
				}
			}

			// prune shared buffer based on window length
			if(windowTime > 0) {
				long pruningTimestamp = timestamp - windowTime;

				// sanity check to guard against underflows
				if (pruningTimestamp >= timestamp) {
					throw new IllegalStateException("Detected an underflow in the pruning timestamp. This indicates that" +
						" either the window length is too long (" + windowTime + ") or that the timestamp has not been" +
						" set correctly (e.g. Long.MIN_VALUE).");
				}

				// remove all elements which are expired with respect to the window length
				sharedBuffer.prune(pruningTimestamp);
			}
		}

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NFA) {
			@SuppressWarnings("unchecked")
			NFA<T> other = (NFA<T>) obj;

			return nonDuplicatingTypeSerializer.equals(other.nonDuplicatingTypeSerializer) &&
				sharedBuffer.equals(other.sharedBuffer) &&
				states.equals(other.states) &&
				windowTime == other.windowTime &&
				startEventCounter == other.startEventCounter;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(nonDuplicatingTypeSerializer, sharedBuffer, states, windowTime, startEventCounter);
	}

	/**
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine.
	 *
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timestamp Timestamp of the current event
	 * @return Collection of computation states which result from the current one
	 */
	private Collection<ComputationState<T>> computeNextStates(
			final ComputationState<T> computationState,
			final T event,
			final long timestamp) {
		Stack<State<T>> states = new Stack<>();
		ArrayList<ComputationState<T>> resultingComputationStates = new ArrayList<>();
		State<T> state = computationState.getState();

		states.push(state);

		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state
			for (StateTransition<T> stateTransition: stateTransitions) {
				try {
					if (stateTransition.getCondition() == null || stateTransition.getCondition().filter(event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
								resultingComputationStates.add(computationState);

								// we have a new computation state referring to the same the shared entry
								// the lock of the current computation is released later on
								sharedBuffer.lock(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
								break;
							case TAKE:
								final State<T> newState = stateTransition.getTargetState();
								final DeweyNumber oldVersion;
								final DeweyNumber newComputationStateVersion;
								final State<T> previousState = computationState.getState();
								final T previousEvent = computationState.getEvent();
								final long previousTimestamp;
								final long startTimestamp;

								if (computationState.isStartState()) {
									oldVersion = new DeweyNumber(startEventCounter++);
									newComputationStateVersion = oldVersion.addStage();
									startTimestamp = timestamp;
									previousTimestamp = -1L;

								} else {
									startTimestamp = computationState.getStartTimestamp();
									previousTimestamp = computationState.getTimestamp();
									oldVersion = computationState.getVersion();

									if (newState.equals(computationState.getState())) {
										newComputationStateVersion = oldVersion.increase();
									} else {
										newComputationStateVersion = oldVersion.addStage();
									}
								}

								if (previousState.isStart()) {
									sharedBuffer.put(
										newState,
										event,
										timestamp,
										oldVersion);
								} else {
									sharedBuffer.put(
										newState,
										event,
										timestamp,
										previousState,
										previousEvent,
										previousTimestamp,
										oldVersion);
								}

								// a new computation state is referring to the shared entry
								sharedBuffer.lock(newState, event, timestamp);

								resultingComputationStates.add(new ComputationState<T>(
									newState,
									event,
									timestamp,
									newComputationStateVersion,
									startTimestamp));
								break;
						}
					}
				} catch (Exception e) {
					throw new RuntimeException("Failure happened in filter function.", e);
				}
			}
		}

		if (computationState.isStartState()) {
			// a computation state is always kept if it refers to a starting state because every
			// new element can start a new pattern
			resultingComputationStates.add(computationState);
		} else {
			// release the shared entry referenced by the current computation state.
			sharedBuffer.release(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
			// try to remove unnecessary shared buffer entries
			sharedBuffer.remove(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
		}

		return resultingComputationStates;
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 */
	private Collection<Map<String, T>> extractPatternMatches(final ComputationState<T> computationState) {
		Collection<LinkedHashMultimap<State<T>, T>> paths = sharedBuffer.extractPatterns(
			computationState.getState(),
			computationState.getEvent(),
			computationState.getTimestamp(),
			computationState.getVersion());

		ArrayList<Map<String, T>> result = new ArrayList<>();

		TypeSerializer<T> serializer = nonDuplicatingTypeSerializer.getTypeSerializer();

		// generate the correct names from the collection of LinkedHashMultimaps
		for (LinkedHashMultimap<State<T>, T> path: paths) {
			Map<String, T> resultPath = new HashMap<>();
			for (State<T> key: path.keySet()) {
				int counter = 0;
				Set<T> events = path.get(key);

				// we iterate over the elements in insertion order
				for (T event: events) {
					resultPath.put(
						events.size() > 1 ? generateStateName(key.getName(), counter): key.getName(),
						// copy the element so that the user can change it
						serializer.isImmutableType() ? event : serializer.copy(event)
					);
				}
			}

			result.add(resultPath);
		}

		return result;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		oos.writeInt(computationStates.size());

		for(ComputationState<T> computationState: computationStates) {
			writeComputationState(computationState, oos);
		}

		nonDuplicatingTypeSerializer.clearReferences();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		int numberComputationStates = ois.readInt();

		computationStates = new LinkedList<>();

		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = readComputationState(ois);

			computationStates.offer(computationState);
		}

		nonDuplicatingTypeSerializer.clearReferences();
	}

	private void writeComputationState(final ComputationState<T> computationState, final ObjectOutputStream oos) throws IOException {
		oos.writeObject(computationState.getState());
		oos.writeLong(computationState.getTimestamp());
		oos.writeObject(computationState.getVersion());
		oos.writeLong(computationState.getStartTimestamp());

		if (computationState.getEvent() == null) {
			// write that we don't have an event associated
			oos.writeBoolean(false);
		} else {
			// write that we have an event associated
			oos.writeBoolean(true);
			DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(oos);
			nonDuplicatingTypeSerializer.serialize(computationState.getEvent(), output);
		}
	}

	@SuppressWarnings("unchecked")
	private ComputationState<T> readComputationState(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		final State<T> state = (State<T>)ois.readObject();
		final long timestamp = ois.readLong();
		final DeweyNumber version = (DeweyNumber)ois.readObject();
		final long startTimestamp = ois.readLong();

		final boolean hasEvent = ois.readBoolean();
		final T event;

		if (hasEvent) {
			DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(ois);
			event = nonDuplicatingTypeSerializer.deserialize(input);
		} else {
			event = null;
		}

		return new ComputationState<>(state, event, timestamp, version, startTimestamp);
	}

	/**
	 * Generates a state name from a given name template and an index.
	 * <p>
	 * If the template ends with "[]" the index is inserted in between the square brackets.
	 * Otherwise, an underscore and the index is appended to the name.
	 *
	 * @param name Name template
	 * @param index Index of the state
	 * @return Generated state name from the given state name template
	 */
	static String generateStateName(final String name, final int index) {
		Matcher matcher = namePattern.matcher(name);

		if (matcher.matches()) {
			return matcher.group(1) + index + matcher.group(2);
		} else {
			return name + "_" + index;
		}
	}
}
