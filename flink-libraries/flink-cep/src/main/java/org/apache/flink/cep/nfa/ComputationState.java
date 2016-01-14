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

public class ComputationState<T> {
	private final State<T> state;
	private final T event;
	private final long timestamp;
	private final DeweyNumber version;
	private final long startTimestamp;

	public ComputationState(
		final State<T> currentState,
		final T event,
		final long timestamp,
		final DeweyNumber version,
		final long startTimestamp) {
		this.state = currentState;
		this.event = event;
		this.timestamp = timestamp;
		this.version = version;
		this.startTimestamp = startTimestamp;
	}

	public boolean isFinalState() {
		return state.isFinal();
	}

	public boolean isStartState() {
		return state.isStart();
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

	public T getEvent() {
		return event;
	}

	public DeweyNumber getVersion() {
		return version;
	}
}
