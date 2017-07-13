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
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.nfa.NFATestUtilities.compareMaps;
import static org.apache.flink.cep.nfa.NFATestUtilities.feedNFA;

/**
 * IT tests covering {@link Pattern#greedy()}.
 */
public class GreedyITCase extends TestLogger {

	@Test
	public void testGreedyFollowedBy() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event c = new Event(40, "c", 1.0);
		Event a1 = new Event(41, "a", 2.0);
		Event a2 = new Event(42, "a", 2.0);
		Event a3 = new Event(43, "a", 2.0);
		Event d = new Event(44, "d", 3.0);

		inputEvents.add(new StreamRecord<>(c, 1));
		inputEvents.add(new StreamRecord<>(a1, 2));
		inputEvents.add(new StreamRecord<>(a2, 3));
		inputEvents.add(new StreamRecord<>(a3, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		// c a* d
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().greedy().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(c, a1, a2, a3, d)
		));
	}

	@Test
	public void testGreedyUtil() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event c = new Event(40, "c", 1.0);
		Event a1 = new Event(41, "a", 2.0);
		Event a2 = new Event(42, "a", 3.0);
		Event a3 = new Event(43, "a", 4.0);
		Event d = new Event(44, "d", 3.0);

		inputEvents.add(new StreamRecord<>(c, 1));
		inputEvents.add(new StreamRecord<>(a1, 2));
		inputEvents.add(new StreamRecord<>(a2, 3));
		inputEvents.add(new StreamRecord<>(a3, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		// c a* d
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().greedy().until(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getPrice() > 3.0;
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(c, a1, a2, d)
		));
	}

	@Test
	public void testEndWithGreedy() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event c = new Event(40, "c", 1.0);
		Event a1 = new Event(41, "a", 2.0);
		Event a2 = new Event(42, "a", 2.0);
		Event a3 = new Event(43, "a", 2.0);
		Event d = new Event(44, "d", 2.0);

		inputEvents.add(new StreamRecord<>(c, 1));
		inputEvents.add(new StreamRecord<>(a1, 2));
		inputEvents.add(new StreamRecord<>(a2, 3));
		inputEvents.add(new StreamRecord<>(a3, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		// c a*
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().greedy();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(c, a1, a2, a3)
		));
	}

	@Test
	public void testGreedyTimesRange() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event c = new Event(40, "c", 1.0);
		Event a1 = new Event(41, "a", 2.0);
		Event a2 = new Event(42, "a", 2.0);
		Event a3 = new Event(43, "a", 2.0);
		Event a4 = new Event(44, "a", 2.0);
		Event d = new Event(45, "d", 2.0);

		inputEvents.add(new StreamRecord<>(c, 1));
		inputEvents.add(new StreamRecord<>(a1, 2));
		inputEvents.add(new StreamRecord<>(a2, 3));
		inputEvents.add(new StreamRecord<>(a3, 4));
		inputEvents.add(new StreamRecord<>(a4, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		// c a*
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2, 5).greedy();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(c, a1, a2, a3, a4)
		));
	}

	@Test
	public void testGreedyGroup() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event c = new Event(40, "c", 1.0);
		Event a1 = new Event(41, "a", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event a2 = new Event(43, "a", 4.0);
		Event b2 = new Event(44, "b", 5.0);
		Event d = new Event(45, "d", 6.0);

		inputEvents.add(new StreamRecord<>(c, 1));
		inputEvents.add(new StreamRecord<>(a1, 2));
		inputEvents.add(new StreamRecord<>(b1, 4));
		inputEvents.add(new StreamRecord<>(a2, 5));
		inputEvents.add(new StreamRecord<>(b2, 6));
		inputEvents.add(new StreamRecord<>(d, 11));

		// c (a b)* d
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy(Pattern.<Event>begin("middle1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedBy("middle2").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		})).oneOrMore().optional().greedy().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(c, a1, b1, a2, b2, d)
		));
	}
}
