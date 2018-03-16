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

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class which encapsulates the state of the NFA computation. It points to the current state,
 * the last taken event, its occurrence timestamp, the current version and the starting timestamp
 * of the overall pattern.
 *
 * @param <T> Type of the input events
 */
public class ComputationState<T> {
	// pointer to the NFA state of the computation
	private final State<T> state;

	// the last taken event
	private final T event;

	private final int counter;

	// timestamp of the last taken event
	private final long timestamp;

	// The current version of the state to discriminate the valid pattern paths in the SharedBuffer
	private final DeweyNumber version;

	// Timestamp of the first element in the pattern
	private final long startTimestamp;

	private final State<T> previousState;

	private final ConditionContext conditionContext;

	private ComputationState(
			final NFA<T> nfa,
			final State<T> currentState,
			final State<T> previousState,
			final T event,
			final int counter,
			final long timestamp,
			final DeweyNumber version,
			final long startTimestamp) {
		this.state = currentState;
		this.event = event;
		this.counter = counter;
		this.timestamp = timestamp;
		this.version = version;
		this.startTimestamp = startTimestamp;
		this.previousState = previousState;
		this.conditionContext = new ConditionContext(nfa, this);
	}

	public int getCounter() {
		return counter;
	}

	public ConditionContext getConditionContext() {
		return conditionContext;
	}

	public boolean isFinalState() {
		return state.isFinal();
	}

	public boolean isStartState() {
		return state.isStart() && event == null;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public State<T> getState() {
		return state;
	}

	public State<T> getPreviousState() {
		return previousState;
	}

	public T getEvent() {
		return event;
	}

	public DeweyNumber getVersion() {
		return version;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ComputationState) {
			ComputationState other = (ComputationState) obj;
			return Objects.equals(state, other.state) &&
				Objects.equals(event, other.event) &&
				counter == other.counter &&
				timestamp == other.timestamp &&
				Objects.equals(version, other.version) &&
				startTimestamp == other.startTimestamp &&
				Objects.equals(previousState, other.previousState);

		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(state, event, counter, timestamp, version, startTimestamp, previousState);
	}

	public static <T> ComputationState<T> createStartState(final NFA<T> nfa, final State<T> state) {
		Preconditions.checkArgument(state.isStart());
		return new ComputationState<>(nfa, state, null, null, 0, -1L, new DeweyNumber(1), -1L);
	}

	public static <T> ComputationState<T> createStartState(final NFA<T> nfa, final State<T> state, final DeweyNumber version) {
		Preconditions.checkArgument(state.isStart());
		return new ComputationState<>(nfa, state, null, null, 0, -1L, version, -1L);
	}

	public static <T> ComputationState<T> createState(
			final NFA<T> nfa,
			final State<T> currentState,
			final State<T> previousState,
			final T event,
			final int counter,
			final long timestamp,
			final DeweyNumber version,
			final long startTimestamp) {
		return new ComputationState<>(nfa, currentState, previousState, event, counter, timestamp, version, startTimestamp);
	}

	public boolean isStopState() {
		return state.isStop();
	}

	/**
	 * The context used when evaluating this computation state.
	 */
	public class ConditionContext implements IterativeCondition.Context<T> {

		private static final long serialVersionUID = -6733978464782277795L;

		/**
		 * A flag indicating if we should recompute the matching pattern, so that
		 * the {@link IterativeCondition iterative condition} can be evaluated.
		 */
		private boolean shouldUpdate;

		/** The current computation state. */
		private transient ComputationState<T> computationState;

		/** The owning {@link NFA} of this computation state. */
		private final NFA<T> nfa;

		/**
		 * The matched pattern so far. A condition will be evaluated over this
		 * pattern. This is evaluated <b>only once</b>, as this is an expensive
		 * operation that traverses a path in the {@link SharedBuffer}.
		 */
		private transient Map<String, List<T>> matchedEvents;

		public ConditionContext(NFA<T> nfa, ComputationState<T> computationState) {
			this.nfa = nfa;
			this.computationState = computationState;
			this.shouldUpdate = true;
		}

		@Override
		public Iterable<T> getEventsForPattern(final String key) {
			Preconditions.checkNotNull(key);

			// the (partially) matched pattern is computed lazily when this method is called.
			// this is to avoid any overheads when using a simple, non-iterative condition.

			if (shouldUpdate) {
				this.matchedEvents = nfa.extractCurrentMatches(computationState);
				shouldUpdate = false;
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
}
