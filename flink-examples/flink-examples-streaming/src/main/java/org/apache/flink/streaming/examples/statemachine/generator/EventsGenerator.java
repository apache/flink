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

package org.apache.flink.streaming.examples.statemachine.generator;

import org.apache.flink.streaming.examples.statemachine.dfa.EventTypeAndState;
import org.apache.flink.streaming.examples.statemachine.dfa.State;
import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.streaming.examples.statemachine.event.EventType;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A generator for events. The generator internally maintains a series of state
 * machines (addresses and current associated state) and returns transition events
 * from those state machines. Each time the next event is generators, this
 * generator picks a random state machine and creates a random transition on that
 * state machine.
 *
 * <p>The generator randomly adds new state machines, and removes state machines as
 * soon as they reach the terminal state. This implementation maintains up to
 * 1000 state machines concurrently.
 */
public class EventsGenerator {

	/** The random number generator. */
	private final Random rnd;

	/** The currently active state machines. */
	private final LinkedHashMap<Integer, State> states;

	/** Probability with this generator generates an illegal state transition. */
	private final double errorProb;

	public EventsGenerator() {
		this(0.0);
	}

	public EventsGenerator(double errorProb) {
		checkArgument(errorProb >= 0.0 && errorProb <= 1.0, "Invalid error probability");
		this.errorProb = errorProb;

		this.rnd = new Random();
		this.states = new LinkedHashMap<>();
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a new random event. This method randomly pick either
	 * one of its currently running state machines, or start a new state machine for
	 * a random IP address.
	 *
	 * <p>With {@link #errorProb} probability, the generated event will be from an illegal state
	 * transition of one of the currently running state machines.
	 *
	 * @param minIp The lower bound for the range from which a new IP address may be picked.
	 * @param maxIp The upper bound for the range from which a new IP address may be picked.
	 * @return A next random event.
	 */
	public Event next(int minIp, int maxIp) {
		final double p = rnd.nextDouble();

		if (p * 1000 >= states.size()) {
			// create a new state machine
			final int nextIP = rnd.nextInt(maxIp - minIp) + minIp;

			if (!states.containsKey(nextIP)) {
				EventTypeAndState eventAndState = State.Initial.randomTransition(rnd);
				states.put(nextIP, eventAndState.state);
				return new Event(eventAndState.eventType, nextIP);
			}
			else {
				// collision on IP address, try again
				return next(minIp, maxIp);
			}
		}
		else {
			// pick an existing state machine

			// skip over some elements in the linked map, then take the next
			// update it, and insert it at the end

			int numToSkip = Math.min(20, rnd.nextInt(states.size()));
			Iterator<Entry<Integer, State>> iter = states.entrySet().iterator();

			for (int i = numToSkip; i > 0; --i) {
				iter.next();
			}

			Entry<Integer, State> entry = iter.next();
			State currentState = entry.getValue();
			int address = entry.getKey();

			iter.remove();

			if (p < errorProb) {
				EventType event = currentState.randomInvalidTransition(rnd);
				return new Event(event, address);
			}
			else {
				EventTypeAndState eventAndState = currentState.randomTransition(rnd);
				if (!eventAndState.state.isTerminal()) {
					// reinsert
					states.put(address, eventAndState.state);
				}

				return new Event(eventAndState.eventType, address);
			}
		}
	}

	/**
	 * Creates an event for an illegal state transition of one of the internal
	 * state machines. If the generator has not yet started any state machines
	 * (for example, because no call to {@link #next(int, int)} was made, yet), this
	 * will return null.
	 *
	 * @return An event for a illegal state transition, or null, if not possible.
	 */
	@Nullable
	public Event nextInvalid() {
		final Iterator<Entry<Integer, State>> iter = states.entrySet().iterator();
		if (iter.hasNext()) {
			final Entry<Integer, State> entry = iter.next();

			State currentState = entry.getValue();
			int address = entry.getKey();
			iter.remove();

			EventType event = currentState.randomInvalidTransition(rnd);
			return new Event(event, address);
		}
		else {
			return null;
		}
	}

	public int numActiveEntries() {
		return states.size();
	}
}
