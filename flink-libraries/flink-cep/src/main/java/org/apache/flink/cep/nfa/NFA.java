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
import org.apache.flink.cep.ReferenceTypeSerializer;
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

public class NFA<T> implements Serializable {

	private static final Pattern namePattern = Pattern.compile("^(.*\\[)(\\])$");
	private static final long serialVersionUID = 2957674889294717265L;

	private final ReferenceTypeSerializer<T> referenceTypeSerializer;
	private final SharedBuffer<State<T>, T> sharedBuffer;
	private final Set<State<T>> states;
	private final long timeWindow;
	private int startEventCounter;

	private transient Queue<ComputationState<T>> computationStates;

	public NFA(final TypeSerializer<T> eventSerializer, final long timeWindow) {
		this.referenceTypeSerializer = new ReferenceTypeSerializer<>(eventSerializer);
		this.timeWindow = timeWindow;
		sharedBuffer = new SharedBuffer<>(referenceTypeSerializer);
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

	public NFA<T> duplicate() {
		NFA<T> result = new NFA<>(referenceTypeSerializer.getTypeSerializer().duplicate(), timeWindow);

		result.addStates(states);

		return result;
	}

	public Collection<Map<String, T>> process(final T event, final long timestamp) {
		final int numberComputationStates = computationStates.size();
		final List<Map<String, T>> result = new ArrayList<>();

		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = computationStates.poll();

			final Collection<ComputationState<T>> newComputationStates;

			if (!computationState.isStartState() &&
				timeWindow > 0 &&
				timestamp - computationState.getStartTimestamp() > timeWindow) {
				sharedBuffer.release(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
				sharedBuffer.remove(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());

				newComputationStates = Collections.emptyList();
			} else {
				newComputationStates = computeNextStates(computationState, event, timestamp);
			}

			for (ComputationState<T> newComputationState: newComputationStates) {
				if (newComputationState.isFinalState()) {
					Collection<Map<String, T>> matches = extractPatternMatches(newComputationState);
					result.addAll(matches);

					// remove found patterns if no longer needed
					sharedBuffer.release(newComputationState.getState(), newComputationState.getEvent(), newComputationState.getTimestamp());
					sharedBuffer.remove(newComputationState.getState(), newComputationState.getEvent(), newComputationState.getTimestamp());
				} else {
					computationStates.add(newComputationState);
				}
			}

			// prune shared buffer based on window length
			if(timeWindow > 0) {
				long pruningTimestamp = timestamp - timeWindow;

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

			return referenceTypeSerializer.equals(other.referenceTypeSerializer) &&
				sharedBuffer.equals(other.sharedBuffer) &&
				states.equals(other.states) &&
				timeWindow == other.timeWindow &&
				startEventCounter == other.startEventCounter;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(referenceTypeSerializer, sharedBuffer, states, timeWindow, startEventCounter);
	}

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

			for (StateTransition<T> stateTransition: stateTransitions) {
				try {
					if (stateTransition.getCondition() == null || stateTransition.getCondition().filter(event)) {
						switch (stateTransition.getAction()) {
							case PROCEED:
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
								resultingComputationStates.add(computationState);

								// we have a new computation state referencing the shared entry
								sharedBuffer.lock(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
								break;
							case TAKE:
								final State<T> newState = stateTransition.getTargetState();
								final DeweyNumber newVersion;
								final State<T> previousState = computationState.getState();
								final T previousEvent = computationState.getEvent();
								final long previousTimestamp;
								final long startTimestamp;

								if (computationState.isStartState()) {
									newVersion = new DeweyNumber(startEventCounter++);
									startTimestamp = timestamp;
									previousTimestamp = -1L;

								} else {
									startTimestamp = computationState.getStartTimestamp();
									previousTimestamp = computationState.getTimestamp();

									if (newState.equals(computationState.getState())) {
										newVersion = computationState.getVersion().increase();
									} else {
										newVersion = computationState.getVersion().addStage();
									}
								}

								sharedBuffer.put(
									newState,
									event,
									timestamp,
									previousState,
									previousEvent,
									previousTimestamp,
									newVersion);

								sharedBuffer.lock(newState, event, timestamp);

								resultingComputationStates.add(new ComputationState<T>(
									newState,
									event,
									timestamp,
									newVersion,
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
			resultingComputationStates.add(computationState);
		} else {
			// release the shared entry referenced by the current computation state.
			sharedBuffer.release(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
			// try to remove unnecessary shared buffer entries
			sharedBuffer.remove(computationState.getState(), computationState.getEvent(), computationState.getTimestamp());
		}

		return resultingComputationStates;
	}

	private Collection<Map<String, T>> extractPatternMatches(final ComputationState<T> computationState) {
		Collection<LinkedHashMultimap<State<T>, T>> paths = sharedBuffer.extractPatterns(
			computationState.getState(),
			computationState.getEvent(),
			computationState.getTimestamp(),
			computationState.getVersion());

		ArrayList<Map<String, T>> result = new ArrayList<>();

		for (LinkedHashMultimap<State<T>, T> path: paths) {
			Map<String, T> resultPath = new HashMap<>();
			for (State<T> key: path.keySet()) {
				int counter = 0;
				Set<T> events = path.get(key);
				for (T event: events) {
					resultPath.put(
						events.size() > 1 ? generateStateName(key.getName(), counter): key.getName(),
						event);
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

		referenceTypeSerializer.clearReferences();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		int numberComputationStates = ois.readInt();

		computationStates = new LinkedList<>();

		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = readComputationState(ois);

			computationStates.offer(computationState);
		}

		referenceTypeSerializer.clearReferences();
	}

	private void writeComputationState(final ComputationState<T> computationState, final ObjectOutputStream oos) throws IOException {
		oos.writeObject(computationState.getState());
		oos.writeLong(computationState.getTimestamp());
		oos.writeObject(computationState.getVersion());
		oos.writeLong(computationState.getStartTimestamp());

		DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(oos);

		referenceTypeSerializer.serialize(computationState.getEvent(), output);
	}

	@SuppressWarnings("unchecked")
	private ComputationState<T> readComputationState(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		final State<T> state = (State<T>)ois.readObject();
		final long timestamp = ois.readLong();
		final DeweyNumber version = (DeweyNumber)ois.readObject();
		final long startTimestamp = ois.readLong();

		DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(ois);
		final T event = referenceTypeSerializer.deserialize(input);

		return new ComputationState<>(state, event, timestamp, version, startTimestamp);
	}

	static String generateStateName(final String name, final int index) {
		Matcher matcher = namePattern.matcher(name);

		if (matcher.matches()) {
			return matcher.group(1) + index + matcher.group(2);
		} else {
			return name + "_" + index;
		}
	}
}
