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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.StreamEvent;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NFATest extends TestLogger {
	@Test
	public void testSimpleNFA() {
		NFA<Event> nfa = new NFA<>(Event.createTypeSerializer(), 0);
		List<StreamEvent<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(StreamEvent.of(new Event(1, "start", 1.0), 1L));
		streamEvents.add(StreamEvent.of(new Event(2, "bar", 2.0), 2L));
		streamEvents.add(StreamEvent.of(new Event(3, "start", 3.0), 3L));
		streamEvents.add(StreamEvent.of(new Event(4, "end", 4.0), 4L));

		State<Event> startingState = new State<>("", State.StateType.Start);
		State<Event> startState = new State<>("start", State.StateType.Normal);
		State<Event> endState = new State<>("end", State.StateType.Final);
		StateTransition<Event> starting2Start = new StateTransition<>(
			StateTransitionAction.TAKE,
			startState,
			new FilterFunction<Event>() {
				private static final long serialVersionUID = -4869589195918650396L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			}
		);

		StateTransition<Event> start2End = new StateTransition<>(
			StateTransitionAction.TAKE,
			endState,
			new FilterFunction<Event>() {
				private static final long serialVersionUID = 2979804163709590673L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			}
		);

		StateTransition<Event> start2Start = new StateTransition<>(StateTransitionAction.IGNORE, startState, null);

		startingState.addStateTransition(starting2Start);
		startState.addStateTransition(start2End);
		startState.addStateTransition(start2Start);

		nfa.addState(startingState);
		nfa.addState(startState);
		nfa.addState(endState);

		Set<Map<String, Event>> expectedPatterns = new HashSet<>();

		Map<String, Event> firstPattern = new HashMap<>();
		firstPattern.put("start", new Event(1, "start", 1.0));
		firstPattern.put("end", new Event(4, "end", 4.0));

		Map<String, Event> secondPattern = new HashMap<>();
		secondPattern.put("start", new Event(3, "start", 3.0));
		secondPattern.put("end", new Event(4, "end", 4.0));

		expectedPatterns.add(firstPattern);
		expectedPatterns.add(secondPattern);

		Collection<Map<String, Event>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	@Test
	public void testTimeoutWindowPruning() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamEvent<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(StreamEvent.of(new Event(1, "start", 1.0), 1L));
		streamEvents.add(StreamEvent.of(new Event(2, "bar", 2.0), 2L));
		streamEvents.add(StreamEvent.of(new Event(3, "start", 3.0), 3L));
		streamEvents.add(StreamEvent.of(new Event(4, "end", 4.0), 4L));

		Set<Map<String, Event>> expectedPatterns = new HashSet<>();

		Map<String, Event> secondPattern = new HashMap<>();
		secondPattern.put("start", new Event(3, "start", 3.0));
		secondPattern.put("end", new Event(4, "end", 4.0));

		expectedPatterns.add(secondPattern);

		Collection<Map<String, Event>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	/**
	 * Tests that elements whose timestamp difference is exactly the window length are not matched.
	 * The reaon is that the right window side (later elements) is exclusive.
	 */
	@Test
	public void testWindowBorders() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamEvent<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(StreamEvent.of(new Event(1, "start", 1.0), 1L));
		streamEvents.add(StreamEvent.of(new Event(2, "end", 2.0), 3L));

		Set<Map<String, Event>> expectedPatterns = Collections.emptySet();

		Collection<Map<String, Event>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	/**
	 * Tests that pruning shared buffer elements and computations state use the same window border
	 * semantics (left side inclusive and right side exclusive)
	 */
	@Test
	public void testTimeoutWindowPruningWindowBorders() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamEvent<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(StreamEvent.of(new Event(1, "start", 1.0), 1L));
		streamEvents.add(StreamEvent.of(new Event(2, "start", 2.0), 2L));
		streamEvents.add(StreamEvent.of(new Event(3, "foobar", 3.0), 3L));
		streamEvents.add(StreamEvent.of(new Event(4, "end", 4.0), 3L));

		Set<Map<String, Event>> expectedPatterns = new HashSet<>();

		Map<String, Event> secondPattern = new HashMap<>();
		secondPattern.put("start", new Event(2, "start", 2.0));
		secondPattern.put("end", new Event(4, "end", 4.0));

		expectedPatterns.add(secondPattern);

		Collection<Map<String, Event>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	@Test
	public void testStateNameGeneration() {
		String expectedName1 = "a[2]";
		String expectedName2 = "a_3";
		String expectedName3 = "a[][42]";

		String generatedName1 = NFA.generateStateName("a[]", 2);
		String generatedName2 = NFA.generateStateName("a", 3);
		String generatedName3 = NFA.generateStateName("a[][]", 42);


		assertEquals(expectedName1, generatedName1);
		assertEquals(expectedName2, generatedName2);
		assertEquals(expectedName3, generatedName3);
	}

	public <T> Collection<Map<String, T>> runNFA(NFA<T> nfa, List<StreamEvent<T>> inputs) {
		Set<Map<String, T>> actualPatterns = new HashSet<>();

		for (StreamEvent<T> streamEvent: inputs) {
			Collection<Map<String, T>> matchedPatterns = nfa.process(streamEvent.getEvent(), streamEvent.getTimestamp());

			actualPatterns.addAll(matchedPatterns);
		}

		return actualPatterns;
	}

	@Test
	public void testNFASerialization() throws IOException, ClassNotFoundException {
		NFA<Event> nfa = new NFA<>(Event.createTypeSerializer(), 0);

		State<Event> startingState = new State<>("", State.StateType.Start);
		State<Event> startState = new State<>("start", State.StateType.Normal);
		State<Event> endState = new State<>("end", State.StateType.Final);

		StateTransition<Event> starting2Start = new StateTransition<>(
			StateTransitionAction.TAKE,
			startState,
			new NameFilter("start"));

		StateTransition<Event> start2End = new StateTransition<>(
			StateTransitionAction.TAKE,
			endState,
			new NameFilter("end"));

		StateTransition<Event> start2Start = new StateTransition<>(
			StateTransitionAction.IGNORE,
			startState,
			null);

		startingState.addStateTransition(starting2Start);
		startState.addStateTransition(start2End);
		startState.addStateTransition(start2Start);

		nfa.addState(startingState);
		nfa.addState(startState);
		nfa.addState(endState);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(nfa);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);

		@SuppressWarnings("unchecked")
		NFA<Event> copy = (NFA<Event>) ois.readObject();

		assertEquals(nfa, copy);
	}

	private NFA<Event> createStartEndNFA(long windowLength) {
		NFA<Event> nfa = new NFA<>(Event.createTypeSerializer(), windowLength);

		State<Event> startingState = new State<>("", State.StateType.Start);
		State<Event> startState = new State<>("start", State.StateType.Normal);
		State<Event> endState = new State<>("end", State.StateType.Final);
		StateTransition<Event> starting2Start = new StateTransition<>(
			StateTransitionAction.TAKE,
			startState,
			new FilterFunction<Event>() {
				private static final long serialVersionUID = -4869589195918650396L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			});

		StateTransition<Event> start2End = new StateTransition<>(
			StateTransitionAction.TAKE,
			endState,
			new FilterFunction<Event>() {
				private static final long serialVersionUID = 2979804163709590673L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		StateTransition<Event> start2Start = new StateTransition<>(
			StateTransitionAction.IGNORE,
			startState,
			null);

		startingState.addStateTransition(starting2Start);
		startState.addStateTransition(start2End);
		startState.addStateTransition(start2Start);

		nfa.addState(startingState);
		nfa.addState(startState);
		nfa.addState(endState);

		return nfa;
	}

	private static class NameFilter implements FilterFunction<Event> {

		private static final long serialVersionUID = 7472112494752423802L;

		private final String name;

		public NameFilter(final String name) {
			this.name = name;
		}

		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals(name);
		}
	}
}
