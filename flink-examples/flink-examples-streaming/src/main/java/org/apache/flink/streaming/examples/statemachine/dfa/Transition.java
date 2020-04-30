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

package org.apache.flink.streaming.examples.statemachine.dfa;

import org.apache.flink.streaming.examples.statemachine.event.EventType;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A possible transition on a given event into a target state. The transition
 * belongs to its originating state and has an associated probability that is
 * used to generate random transition events.
 */
public class Transition implements Serializable {

	// this class is serializable to be able to interact cleanly with enums.
	private static final long serialVersionUID = 1L;

	/** The event that triggers the transition. */
	private final EventType eventType;

	/** The target state after the transition. */
	private final State targetState;

	/** The probability of the transition. */
	private final float prob;

	/**
	 * Creates a new transition.
	 *
	 * @param eventType The event that triggers the transition.
	 * @param targetState The target state after the transition.
	 * @param prob The probability of the transition.
	 */
	public Transition(EventType eventType, State targetState, float prob) {
		this.eventType = checkNotNull(eventType);
		this.targetState = checkNotNull(targetState);
		this.prob = prob;
	}

	// ------------------------------------------------------------------------

	public EventType eventType() {
		return eventType;
	}

	public State targetState() {
		return targetState;
	}

	public float prob() {
		return prob;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final Transition that = (Transition) obj;
			return this.eventType == that.eventType &&
					this.targetState == that.targetState &&
					Float.compare(this.prob, that.prob) == 0;
		}
	}

	@Override
	public int hashCode() {
		int code = 31 * eventType.hashCode() + targetState.hashCode();
		return 31 * code + (prob != +0.0f ? Float.floatToIntBits(prob) : 0);
	}

	@Override
	public String toString() {
		return "--[" + eventType.name() + "]--> " + targetState.name() + " (" + prob + ')';
	}
}
