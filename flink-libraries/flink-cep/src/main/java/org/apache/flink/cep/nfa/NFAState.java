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

import org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator;

import java.util.LinkedList;
import java.util.Queue;

/**
 * State kept for a {@link NFA}.
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
public class NFAState<T> {

	/**
	 * Current set of {@link ComputationState computation states} within the state machine.
	 * These are the "active" intermediate states that are waiting for new matching
	 * events to transition to new valid states.
	 */
	private Queue<ComputationState<T>> computationStates;

	/**
	 * Buffer used to store the matched events.
	 */
	private SharedBuffer<String, T> eventSharedBuffer;

	/**
	 * Flag indicating whether the matching status of the state machine has changed.
	 */
	private boolean stateChanged;

	public NFAState(
			Queue<ComputationState<T>> computationStates,
			SharedBuffer<String, T> eventSharedBuffer,
			boolean stateChanged) {
		this.computationStates = computationStates;
		this.eventSharedBuffer = eventSharedBuffer;
		this.stateChanged = stateChanged;
	}

	public NFAState() {
		this(new LinkedList<>(), new SharedBuffer<>(), false);
	}

	public NFAState(Iterable<ComputationState<T>> startingStates) {
		this();

		for (ComputationState<T> startingState : startingStates) {
			computationStates.add(startingState);
		}
	}

	/**
	 * Check if the NFA has finished processing all incoming data so far. That is
	 * when the buffer keeping the matches is empty.
	 *
	 * @return {@code true} if there are no elements in the {@link SharedBuffer},
	 * {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return eventSharedBuffer.isEmpty();
	}

	/**
	 * Check if the matching status of the NFA has changed so far.
	 *
	 * @return {@code true} if matching status has changed, {@code false} otherwise
	 */
	public boolean isStateChanged() {
		return stateChanged;
	}

	/**
	 * Reset the changed bit checked via {@link #isStateChanged()} to {@code false}.
	 */
	public void resetStateChanged() {
		this.stateChanged = false;
	}

	public void setStateChanged(boolean stateChanged) {
		this.stateChanged = stateChanged;
	}

	public Queue<ComputationState<T>> getComputationStates() {
		return computationStates;
	}

	public SharedBuffer<String, T> getEventSharedBuffer() {
		return eventSharedBuffer;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NFAState) {
			@SuppressWarnings("unchecked")
			NFAState<T> other = (NFAState<T>) obj;

			return eventSharedBuffer.equals(other.eventSharedBuffer);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return eventSharedBuffer.hashCode();
	}
}
