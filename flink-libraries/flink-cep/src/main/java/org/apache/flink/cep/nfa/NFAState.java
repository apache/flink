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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * State kept for a {@link NFA}.
 */
public class NFAState {

	/**
	 * Current set of {@link ComputationState computation states} within the state machine.
	 * These are the "active" intermediate states that are waiting for new matching
	 * events to transition to new valid states.
	 */
	private Queue<ComputationState> partialMatches;

	private Queue<ComputationState> completedMatches;

	/**
	 * Flag indicating whether the matching status of the state machine has changed.
	 */
	private boolean stateChanged;

	public static final Comparator<ComputationState> COMPUTATION_STATE_COMPARATOR =
		Comparator.<ComputationState>comparingLong(c ->
				c.getStartEventID() != null ? c.getStartEventID().getTimestamp() : Long.MAX_VALUE)
			.thenComparingInt(c ->
				c.getStartEventID() != null ? c.getStartEventID().getId() : Integer.MAX_VALUE);

	public NFAState(Iterable<ComputationState> states) {
		this.partialMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
		for (ComputationState startingState : states) {
			partialMatches.add(startingState);
		}

		this.completedMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
	}

	public NFAState(Queue<ComputationState> partialMatches, Queue<ComputationState> completedMatches) {
		this.partialMatches = partialMatches;
		this.completedMatches = completedMatches;
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

	/**
	 * Set the changed bit checked via {@link #isStateChanged()} to {@code true}.
	 */
	public void setStateChanged() {
		this.stateChanged = true;
	}

	public Queue<ComputationState> getPartialMatches() {
		return partialMatches;
	}

	public Queue<ComputationState> getCompletedMatches() {
		return completedMatches;
	}

	public void setNewPartialMatches(PriorityQueue<ComputationState> newPartialMatches) {
		this.partialMatches = newPartialMatches;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		NFAState nfaState = (NFAState) o;
		return Arrays.equals(partialMatches.toArray(), nfaState.partialMatches.toArray()) &&
			Arrays.equals(completedMatches.toArray(), nfaState.completedMatches.toArray());
	}

	@Override
	public int hashCode() {
		return Objects.hash(partialMatches, completedMatches);
	}

	@Override
	public String toString() {
		return "NFAState{" +
			"partialMatches=" + partialMatches +
			", completedMatches=" + completedMatches +
			", stateChanged=" + stateChanged +
			'}';
	}
}
