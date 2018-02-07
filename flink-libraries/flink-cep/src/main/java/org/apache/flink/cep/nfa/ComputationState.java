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

import javax.annotation.Nullable;

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
	private final String state;

	// the last taken event
	private final T event;

	private final int counter;

	// timestamp of the last taken event
	private final long timestamp;

	// The current version of the state to discriminate the valid pattern paths in the SharedBuffer
	private final DeweyNumber version;

	// Timestamp of the first element in the pattern
	private final long startTimestamp;

	@Nullable
	private final String previousState;

	private ComputationState(
			final String currentState,
			@Nullable final String previousState,
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
	}

	public int getCounter() {
		return counter;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public String getState() {
		return state;
	}

	@Nullable
	public String getPreviousState() {
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

	public static <T> ComputationState<T> createStartState(final String state) {
		return new ComputationState<>(state, null, null, 0, -1L, new DeweyNumber(1), -1L);
	}

	public static <T> ComputationState<T> createStartState(final String state, final DeweyNumber version) {
		return new ComputationState<T>(state, null, null, 0, -1L, version, -1L);
	}

	public static <T> ComputationState<T> createState(
			final String currentState,
			final String previousState,
			final T event,
			final int counter,
			final long timestamp,
			final DeweyNumber version,
			final long startTimestamp) {
		return new ComputationState<>(currentState, previousState, event, counter, timestamp, version, startTimestamp);
	}
}
