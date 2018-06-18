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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.nfa.NFATestUtilities.compareMaps;
import static org.apache.flink.cep.nfa.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;

/**
 * IT tests covering {@link AfterMatchSkipStrategy}.
 */
public class AfterMatchSkipITCase extends TestLogger{

	@Test
	public void testSkipToNext() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a", 0.0);
		Event a2 = new Event(2, "a", 0.0);
		Event a3 = new Event(3, "a", 0.0);
		Event a4 = new Event(4, "a", 0.0);
		Event a5 = new Event(5, "a", 0.0);
		Event a6 = new Event(6, "a", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(a3));
		streamEvents.add(new StreamRecord<Event>(a4));
		streamEvents.add(new StreamRecord<Event>(a5));
		streamEvents.add(new StreamRecord<Event>(a6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.noSkip())
			.where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			}).times(3);

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(a1, a2, a3),
			Lists.newArrayList(a2, a3, a4),
			Lists.newArrayList(a3, a4, a5),
			Lists.newArrayList(a4, a5, a6)
		));
	}

	@Test
	public void testSkipPastLast() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a", 0.0);
		Event a2 = new Event(2, "a", 0.0);
		Event a3 = new Event(3, "a", 0.0);
		Event a4 = new Event(4, "a", 0.0);
		Event a5 = new Event(5, "a", 0.0);
		Event a6 = new Event(6, "a", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(a3));
		streamEvents.add(new StreamRecord<Event>(a4));
		streamEvents.add(new StreamRecord<Event>(a5));
		streamEvents.add(new StreamRecord<Event>(a6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
			.where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			}).times(3);

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(a1, a2, a3),
			Lists.newArrayList(a4, a5, a6)
		));
	}

	@Test
	public void testSkipToFirst() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event ab1 = new Event(1, "ab", 0.0);
		Event ab2 = new Event(2, "ab", 0.0);
		Event ab3 = new Event(3, "ab", 0.0);
		Event ab4 = new Event(4, "ab", 0.0);
		Event ab5 = new Event(5, "ab", 0.0);
		Event ab6 = new Event(6, "ab", 0.0);

		streamEvents.add(new StreamRecord<Event>(ab1));
		streamEvents.add(new StreamRecord<Event>(ab2));
		streamEvents.add(new StreamRecord<Event>(ab3));
		streamEvents.add(new StreamRecord<Event>(ab4));
		streamEvents.add(new StreamRecord<Event>(ab5));
		streamEvents.add(new StreamRecord<Event>(ab6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start",
			AfterMatchSkipStrategy.skipToFirst("end"))
			.where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("a");
				}
			}).times(2).next("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("b");
				}
			}).times(2);

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(ab1, ab2, ab3, ab4),
			Lists.newArrayList(ab3, ab4, ab5, ab6)
		));
	}

	@Test
	public void testSkipToLast() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event ab1 = new Event(1, "ab", 0.0);
		Event ab2 = new Event(2, "ab", 0.0);
		Event ab3 = new Event(3, "ab", 0.0);
		Event ab4 = new Event(4, "ab", 0.0);
		Event ab5 = new Event(5, "ab", 0.0);
		Event ab6 = new Event(6, "ab", 0.0);
		Event ab7 = new Event(7, "ab", 0.0);

		streamEvents.add(new StreamRecord<Event>(ab1));
		streamEvents.add(new StreamRecord<Event>(ab2));
		streamEvents.add(new StreamRecord<Event>(ab3));
		streamEvents.add(new StreamRecord<Event>(ab4));
		streamEvents.add(new StreamRecord<Event>(ab5));
		streamEvents.add(new StreamRecord<Event>(ab6));
		streamEvents.add(new StreamRecord<Event>(ab7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipToLast("end")).where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("a");
			}
		}).times(2).next("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("b");
			}
		}).times(2);
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(ab1, ab2, ab3, ab4),
			Lists.newArrayList(ab4, ab5, ab6, ab7)
		));
	}

	@Test
	public void testSkipPastLast2() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a1", 0.0);
		Event a2 = new Event(2, "a2", 0.0);
		Event b1 = new Event(3, "b1", 0.0);
		Event b2 = new Event(4, "b2", 0.0);
		Event c1 = new Event(5, "c1", 0.0);
		Event c2 = new Event(6, "c2", 0.0);
		Event d1 = new Event(7, "d1", 0.0);
		Event d2 = new Event(7, "d2", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(b1));
		streamEvents.add(new StreamRecord<Event>(b2));
		streamEvents.add(new StreamRecord<Event>(c1));
		streamEvents.add(new StreamRecord<Event>(c2));
		streamEvents.add(new StreamRecord<Event>(d1));
		streamEvents.add(new StreamRecord<Event>(d2));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent()).where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("a");
			}
		}).followedByAny("b").where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("b");
				}
			}
		).followedByAny("c").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("c");
			}
		}).followedByAny("d").where(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("d");
				}
		});
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(a1, b1, c1, d1),
			Lists.newArrayList(a1, b1, c2, d1),
			Lists.newArrayList(a1, b2, c1, d1),
			Lists.newArrayList(a1, b2, c2, d1),
			Lists.newArrayList(a2, b1, c1, d1),
			Lists.newArrayList(a2, b1, c2, d1),
			Lists.newArrayList(a2, b2, c1, d1),
			Lists.newArrayList(a2, b2, c2, d1)
		));
	}

	@Test
	public void testSkipPastLast3() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a1", 0.0);
		Event c = new Event(2, "c", 0.0);
		Event a2 = new Event(3, "a2", 0.0);
		Event b2 = new Event(4, "b2", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(c));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(b2));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent()
		).where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("a");
			}
		}).next("b").where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("b");
				}
			}
		);
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a2, b2)
		));
	}

	@Test
	public void testSkipToFirstWithOptionalMatch() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event ab1 = new Event(1, "ab1", 0.0);
		Event c1 = new Event(2, "c1", 0.0);
		Event ab2 = new Event(3, "ab2", 0.0);
		Event c2 = new Event(4, "c2", 0.0);

		streamEvents.add(new StreamRecord<Event>(ab1));
		streamEvents.add(new StreamRecord<Event>(c1));
		streamEvents.add(new StreamRecord<Event>(ab2));
		streamEvents.add(new StreamRecord<Event>(c2));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("x", AfterMatchSkipStrategy.skipToFirst("b")
		).where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("x");
			}
		}).oneOrMore().optional().next("b").where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("b");
				}
			}
		).next("c").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("c");
			}
		});
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(ab1, c1),
			Lists.newArrayList(ab2, c2)
		));
	}

	@Test
	public void testSkipToFirstAtStartPosition() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event ab1 = new Event(1, "ab1", 0.0);
		Event c1 = new Event(2, "c1", 0.0);
		Event ab2 = new Event(3, "ab2", 0.0);
		Event c2 = new Event(4, "c2", 0.0);

		streamEvents.add(new StreamRecord<Event>(ab1));
		streamEvents.add(new StreamRecord<Event>(c1));
		streamEvents.add(new StreamRecord<Event>(ab2));
		streamEvents.add(new StreamRecord<Event>(c2));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("b", AfterMatchSkipStrategy.skipToFirst("b")
		).where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("b");
				}
			}
		).next("c").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("c");
			}
		});
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(ab1, c1),
			Lists.newArrayList(ab2, c2)
		));
	}

	@Test
	public void testSkipToFirstWithOneOrMore() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a1", 0.0);
		Event b1 = new Event(2, "b1", 0.0);
		Event a2 = new Event(3, "a2", 0.0);
		Event b2 = new Event(4, "b2", 0.0);
		Event b3 = new Event(5, "b3", 0.0);
		Event a3 = new Event(3, "a3", 0.0);
		Event b4 = new Event(4, "b4", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(b1));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(b2));
		streamEvents.add(new StreamRecord<Event>(b3));
		streamEvents.add(new StreamRecord<Event>(a3));
		streamEvents.add(new StreamRecord<Event>(b4));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToFirst("b")
		).where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("a");
				}
			}
		).next("b").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("b");
			}
		}).oneOrMore().consecutive();
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(a1, b1),
			Lists.newArrayList(a2, b2),
			Lists.newArrayList(a3, b4)
		));
	}

	@Test
	public void testSkipToLastWithOneOrMore() throws Exception {
		List<StreamRecord<Event>> streamEvents = new ArrayList<>();

		Event a1 = new Event(1, "a1", 0.0);
		Event b1 = new Event(2, "b1", 0.0);
		Event a2 = new Event(3, "a2", 0.0);
		Event b2 = new Event(4, "b2", 0.0);
		Event b3 = new Event(5, "b3", 0.0);
		Event a3 = new Event(3, "a3", 0.0);
		Event b4 = new Event(4, "b4", 0.0);

		streamEvents.add(new StreamRecord<Event>(a1));
		streamEvents.add(new StreamRecord<Event>(b1));
		streamEvents.add(new StreamRecord<Event>(a2));
		streamEvents.add(new StreamRecord<Event>(b2));
		streamEvents.add(new StreamRecord<Event>(b3));
		streamEvents.add(new StreamRecord<Event>(a3));
		streamEvents.add(new StreamRecord<Event>(b4));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToLast("b")
		).where(
			new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().contains("a");
				}
			}
		).next("b").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().contains("b");
			}
		}).oneOrMore().consecutive();
		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(streamEvents, nfa, pattern.getAfterMatchSkipStrategy());

		compareMaps(resultingPatterns, Lists.newArrayList(
			Lists.newArrayList(a1, b1),
			Lists.newArrayList(a2, b2),
			Lists.newArrayList(a3, b4)
		));
	}
}
