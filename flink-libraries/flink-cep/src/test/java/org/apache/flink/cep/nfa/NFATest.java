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

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.BooleanConditions;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link NFA}.
 */
public class NFATest extends TestLogger {
	@Test
	public void testSimpleNFA() {
		NFA<Event> nfa = new NFA<>(Event.createTypeSerializer(), 0, false);
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
		streamEvents.add(new StreamRecord<>(new Event(2, "bar", 2.0), 2L));
		streamEvents.add(new StreamRecord<>(new Event(3, "start", 3.0), 3L));
		streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 4L));

		State<Event> startState = new State<>("start", State.StateType.Start);
		State<Event> endState = new State<>("end", State.StateType.Normal);
		State<Event> endingState = new State<>("", State.StateType.Final);

		startState.addTake(
			endState,
			new SimpleCondition<Event>() {
				private static final long serialVersionUID = -4869589195918650396L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			});
		endState.addTake(
			endingState,
			new SimpleCondition<Event>() {
				private static final long serialVersionUID = 2979804163709590673L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});
		endState.addIgnore(BooleanConditions.<Event>trueFunction());

		nfa.addState(startState);
		nfa.addState(endState);
		nfa.addState(endingState);

		Set<Map<String, List<Event>>> expectedPatterns = new HashSet<>();

		Map<String, List<Event>> firstPattern = new HashMap<>();
		firstPattern.put("start", Collections.singletonList(new Event(1, "start", 1.0)));
		firstPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

		Map<String, List<Event>> secondPattern = new HashMap<>();
		secondPattern.put("start", Collections.singletonList(new Event(3, "start", 3.0)));
		secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

		expectedPatterns.add(firstPattern);
		expectedPatterns.add(secondPattern);

		Collection<Map<String, List<Event>>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	@Test
	public void testTimeoutWindowPruning() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
		streamEvents.add(new StreamRecord<>(new Event(2, "bar", 2.0), 2L));
		streamEvents.add(new StreamRecord<>(new Event(3, "start", 3.0), 3L));
		streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 4L));

		Set<Map<String, List<Event>>> expectedPatterns = new HashSet<>();

		Map<String, List<Event>> secondPattern = new HashMap<>();
		secondPattern.put("start", Collections.singletonList(new Event(3, "start", 3.0)));
		secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

		expectedPatterns.add(secondPattern);

		Collection<Map<String, List<Event>>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	/**
	 * Tests that elements whose timestamp difference is exactly the window length are not matched.
	 * The reaon is that the right window side (later elements) is exclusive.
	 */
	@Test
	public void testWindowBorders() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
		streamEvents.add(new StreamRecord<>(new Event(2, "end", 2.0), 3L));

		Set<Map<String, List<Event>>> expectedPatterns = Collections.emptySet();

		Collection<Map<String, List<Event>>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	/**
	 * Tests that pruning shared buffer elements and computations state use the same window border
	 * semantics (left side inclusive and right side exclusive).
	 */
	@Test
	public void testTimeoutWindowPruningWindowBorders() {
		NFA<Event> nfa = createStartEndNFA(2);
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(new StreamRecord<>(new Event(1, "start", 1.0), 1L));
		streamEvents.add(new StreamRecord<>(new Event(2, "start", 2.0), 2L));
		streamEvents.add(new StreamRecord<>(new Event(3, "foobar", 3.0), 3L));
		streamEvents.add(new StreamRecord<>(new Event(4, "end", 4.0), 3L));

		Set<Map<String, List<Event>>> expectedPatterns = new HashSet<>();

		Map<String, List<Event>> secondPattern = new HashMap<>();
		secondPattern.put("start", Collections.singletonList(new Event(2, "start", 2.0)));
		secondPattern.put("end", Collections.singletonList(new Event(4, "end", 4.0)));

		expectedPatterns.add(secondPattern);

		Collection<Map<String, List<Event>>> actualPatterns = runNFA(nfa, streamEvents);

		assertEquals(expectedPatterns, actualPatterns);
	}

	@Test
	public void testTimeoutWindowPruning2() throws IOException {
		NFA<Event> nfa = createLoopingNFA(2);
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		streamEvents.add(new StreamRecord<>(new Event(1, "loop", 1.0), 101L));
		streamEvents.add(new StreamRecord<>(new Event(2, "loop", 2.0), 102L));
		streamEvents.add(new StreamRecord<>(new Event(3, "loop", 3.0), 103L));
		streamEvents.add(new StreamRecord<>(new Event(4, "loop", 4.0), 104L));
		streamEvents.add(new StreamRecord<>(new Event(5, "loop", 5.0), 105L));
		runNFA(nfa, streamEvents);

		NFA.NFASerializer<Event> serializer = new NFA.NFASerializer<>(Event.createTypeSerializer());

		//serialize
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(nfa, new DataOutputViewStreamWrapper(baos));
		baos.close();
	}

	public <T> Collection<Map<String, List<T>>> runNFA(NFA<T> nfa, List<StreamRecord<T>> inputs) {
		Set<Map<String, List<T>>> actualPatterns = new HashSet<>();

		for (StreamRecord<T> streamEvent : inputs) {
			Collection<Map<String, List<T>>> matchedPatterns = nfa.process(
				streamEvent.getValue(),
				streamEvent.getTimestamp()).f0;

			actualPatterns.addAll(matchedPatterns);
		}

		return actualPatterns;
	}

	@Test
	public void testNFASerialization() throws IOException, ClassNotFoundException {
		Pattern<Event, ?> pattern1 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 1858562682635302605L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).oneOrMore().optional().allowCombinations().followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		Pattern<Event, ?> pattern2 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 1858562682635302605L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("not").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -6085237016591726715L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).oneOrMore().optional().allowCombinations().followedByAny("end").where(new IterativeCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				double sum = 0.0;
				for (Event e : ctx.getEventsForPattern("middle")) {
					sum += e.getPrice();
				}
				return sum > 5.0;
			}
		});

		Pattern<Event, ?> pattern3 = Pattern.<Event>begin("start")
				.notFollowedBy("not").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -6085237016591726715L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).oneOrMore().allowCombinations().followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 8061969839441121955L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		List<Pattern<Event, ?>> patterns = new ArrayList<>();
		patterns.add(pattern1);
		patterns.add(pattern2);
		patterns.add(pattern3);

		for (Pattern<Event, ?> p: patterns) {
			NFACompiler.NFAFactory<Event> nfaFactory = NFACompiler.compileFactory(p, Event.createTypeSerializer(), false);
			NFA<Event> nfa = nfaFactory.createNFA();

			Event a = new Event(40, "a", 1.0);
			Event b = new Event(41, "b", 2.0);
			Event c = new Event(42, "c", 3.0);
			Event b1 = new Event(41, "b", 3.0);
			Event b2 = new Event(41, "b", 4.0);
			Event b3 = new Event(41, "b", 5.0);
			Event d = new Event(43, "d", 4.0);

			nfa.process(a, 1);
			nfa.process(b, 2);
			nfa.process(c, 3);
			nfa.process(b1, 4);
			nfa.process(b2, 5);
			nfa.process(b3, 6);
			nfa.process(d, 7);
			nfa.process(a, 8);

			NFA.NFASerializer<Event> serializer = new NFA.NFASerializer<>(Event.createTypeSerializer());

			//serialize
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serializer.serialize(nfa, new DataOutputViewStreamWrapper(baos));
			baos.close();

			// copy
			NFA.NFASerializer<Event> copySerializer = new NFA.NFASerializer<>(Event.createTypeSerializer());
			ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			copySerializer.duplicate().copy(new DataInputViewStreamWrapper(in), new DataOutputViewStreamWrapper(out));
			in.close();
			out.close();

			// deserialize
			ByteArrayInputStream bais = new ByteArrayInputStream(out.toByteArray());
			NFA<Event> copy = serializer.duplicate().deserialize(new DataInputViewStreamWrapper(bais));
			bais.close();

			assertEquals(nfa, copy);
		}
	}

	private NFA<Event> createStartEndNFA(long windowLength) {
		NFA<Event> nfa = new NFA<>(Event.createTypeSerializer(), windowLength, false);

		State<Event> startState = new State<>("start", State.StateType.Start);
		State<Event> endState = new State<>("end", State.StateType.Normal);
		State<Event> endingState = new State<>("", State.StateType.Final);

		startState.addTake(
			endState,
			new SimpleCondition<Event>() {
				private static final long serialVersionUID = -4869589195918650396L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			});
		endState.addTake(
			endingState,
			new SimpleCondition<Event>() {
				private static final long serialVersionUID = 2979804163709590673L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});
		endState.addIgnore(BooleanConditions.<Event>trueFunction());

		nfa.addState(startState);
		nfa.addState(endState);
		nfa.addState(endingState);

		return nfa;
	}

	private NFA<Event> createLoopingNFA(long windowLength) {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("loop").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("loop");
			}
		}).timesOrMore(3).within(Time.milliseconds(windowLength));

		return NFACompiler.compile(pattern, Event.createTypeSerializer(), false);
	}
}
