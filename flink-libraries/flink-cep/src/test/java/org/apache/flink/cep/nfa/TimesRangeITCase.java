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

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;

/**
 * Tests for {@link Pattern#times(int, int)}.
 */
@SuppressWarnings("unchecked")
public class TimesRangeITCase extends TestLogger {

	@Test
	public void testTimesRange() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(1, 3).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent1, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent1, end1)
		));
	}

	@Test
	public void testTimesRangeFromZero() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(0, 2).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent1, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent1, end1),
			Lists.newArrayList(startEvent, end1)
		));
	}

	@Test
	public void testTimesRangeNonStrict() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(1, 3).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeStrict() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(0, 3).consecutive().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeStrictOptional() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(1, 3).consecutive().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeStrictOptional1() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(1, 3).consecutive().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNonStrictOptional1() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

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
		}).times(1, 3).optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNonStrictOptional2() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2, 3).allowCombinations().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNonStrictOptional3() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2, 3).optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNonStrictWithNext() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2, 3).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNotStrictWithFollowedBy() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

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
		}).times(2, 3).followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesRangeNotStrictWithFollowedByAny() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2, 3).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	private static class ConsecutiveData {
		private static final Event startEvent = new Event(40, "c", 1.0);
		private static final Event middleEvent1 = new Event(41, "a", 2.0);
		private static final Event middleEvent2 = new Event(42, "a", 3.0);
		private static final Event middleEvent3 = new Event(43, "a", 4.0);
		private static final Event end = new Event(44, "b", 5.0);

		private ConsecutiveData() {
		}
	}
}
