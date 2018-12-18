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
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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
 * IT tests covering {@link IterativeCondition} usage.
 */
@SuppressWarnings("unchecked")
public class IterativeConditionsITCase extends TestLogger {

	//////////////////////			Iterative BooleanConditions			/////////////////////////

	private final Event startEvent1 = new Event(40, "start", 1.0);
	private final Event startEvent2 = new Event(40, "start", 2.0);
	private final Event startEvent3 = new Event(40, "start", 3.0);
	private final Event startEvent4 = new Event(40, "start", 4.0);
	private final SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10);
	private final SubEvent middleEvent2 = new SubEvent(42, "foo2", 2.0, 10);
	private final SubEvent middleEvent3 = new SubEvent(43, "foo3", 3.0, 10);
	private final SubEvent middleEvent4 = new SubEvent(43, "foo4", 1.0, 10);
	private final Event nextOne = new Event(44, "next-one", 1.0);
	private final Event endEvent = new Event(46, "end", 1.0);

	@Test
	public void testIterativeWithBranchingPatternEager() throws Exception {
		List<List<Event>> actual = testIterativeWithBranchingPattern(true);

		compareMaps(actual,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, endEvent, middleEvent1, middleEvent2, middleEvent4),
				Lists.newArrayList(startEvent1, endEvent, middleEvent2, middleEvent1),
				Lists.newArrayList(startEvent1, endEvent, middleEvent1),
				Lists.newArrayList(startEvent2, endEvent, middleEvent3, middleEvent4),
				Lists.newArrayList(startEvent2, endEvent, middleEvent3)
			)
		);
	}

	@Test
	public void testIterativeWithBranchingPatternCombinations() throws Exception {
		List<List<Event>> actual = testIterativeWithBranchingPattern(false);

		compareMaps(actual,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, endEvent, middleEvent1, middleEvent2, middleEvent4),
				Lists.newArrayList(startEvent1, endEvent, middleEvent2, middleEvent1),
				Lists.newArrayList(startEvent1, endEvent, middleEvent3, middleEvent1),
				Lists.newArrayList(startEvent2, endEvent, middleEvent3, middleEvent4),
				Lists.newArrayList(startEvent1, endEvent, middleEvent4, middleEvent1),
				Lists.newArrayList(startEvent1, endEvent, middleEvent1),
				Lists.newArrayList(startEvent2, endEvent, middleEvent3)
			)
		);
	}

	private List<List<Event>> testIterativeWithBranchingPattern(boolean eager) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(startEvent1, 1));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(startEvent2, 4));
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<Event>(middleEvent4, 5));
		inputEvents.add(new StreamRecord<>(nextOne, 6));
		inputEvents.add(new StreamRecord<>(endEvent, 8));

		Pattern<Event, ?> pattern = eager
			? Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
			.followedBy("middle").subtype(SubEvent.class).where(new MySubeventIterCondition()).oneOrMore()
			.followedBy("end").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 7056763917392056548L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			})
			: Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
			.followedBy("middle").subtype(SubEvent.class).where(new MySubeventIterCondition()).oneOrMore().allowCombinations()
			.followedBy("end").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 7056763917392056548L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}

	private static class MySubeventIterCondition extends IterativeCondition<SubEvent> {

		private static final long serialVersionUID = 6215754202506583964L;

		@Override
		public boolean filter (SubEvent value, Context < SubEvent > ctx) throws Exception {
			if (!value.getName().startsWith("foo")) {
				return false;
			}

			double sum = 0.0;
			for (Event event : ctx.getEventsForPattern("middle")) {
				sum += event.getPrice();
			}
			sum += value.getPrice();
			return Double.compare(sum, 5.0) < 0;
		}
	}

	@Test
	public void testIterativeWithLoopingStartingEager() throws Exception {
		List<List<Event>> actual = testIterativeWithLoopingStarting(true);

		compareMaps(actual,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, startEvent2, endEvent),
				Lists.newArrayList(startEvent1, endEvent),
				Lists.newArrayList(startEvent2, endEvent),
				Lists.newArrayList(startEvent3, endEvent),
				Lists.newArrayList(endEvent)
			)
		);
	}

	@Test
	public void testIterativeWithLoopingStartingCombination() throws Exception {
		List<List<Event>> actual = testIterativeWithLoopingStarting(false);

		compareMaps(actual,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, startEvent2, endEvent),
				Lists.newArrayList(startEvent1, startEvent3, endEvent),
				Lists.newArrayList(startEvent1, endEvent),
				Lists.newArrayList(startEvent2, endEvent),
				Lists.newArrayList(startEvent3, endEvent),
				Lists.newArrayList(endEvent)
			)
		);
	}

	private List<List<Event>> testIterativeWithLoopingStarting(boolean eager) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(startEvent1, 1L));
		inputEvents.add(new StreamRecord<>(startEvent2, 2L));
		inputEvents.add(new StreamRecord<>(startEvent3, 3L));
		inputEvents.add(new StreamRecord<>(endEvent, 4L));

		// for now, a pattern inherits its continuity property from the followedBy() or next(), and the default
		// behavior (which is the one applied in the case that the pattern graph starts with such a pattern)
		// of a looping pattern is with relaxed continuity (as in followedBy).

		Pattern<Event, ?> pattern = eager
			? Pattern.<Event>begin("start").where(new MyEventIterCondition()).oneOrMore().optional()
			.followedBy("end").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 7056763917392056548L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			})
			: Pattern.<Event>begin("start").where(new MyEventIterCondition()).oneOrMore().allowCombinations().optional()
			.followedBy("end").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 7056763917392056548L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}

	private static class MyEventIterCondition extends IterativeCondition<Event> {

		private static final long serialVersionUID = 6215754202506583964L;

		@Override
		public boolean filter(Event value, Context<Event> ctx) throws Exception {
			if (!value.getName().equals("start")) {
				return false;
			}

			double sum = 0.0;
			for (Event event : ctx.getEventsForPattern("start")) {
				sum += event.getPrice();
			}
			sum += value.getPrice();
			return Double.compare(sum, 5.0) < 0;
		}
	}

	@Test
	public void testIterativeWithPrevPatternDependency() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(startEvent1, 1L));
		inputEvents.add(new StreamRecord<>(startEvent2, 2L));
		inputEvents.add(new StreamRecord<>(endEvent, 4L));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).oneOrMore().followedBy("end").where(new IterativeCondition<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				if (!value.getName().equals("end")) {
					return false;
				}

				double sum = 0.0;
				for (Event event : ctx.getEventsForPattern("start")) {
					sum += event.getPrice();
				}
				return Double.compare(sum, 2.0) >= 0;
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, startEvent2, endEvent),
				Lists.newArrayList(startEvent2, endEvent)
			)
		);
	}

	@Test
	public void testIterativeWithABACPattern() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(startEvent1, 1L)); //1
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 2L)); //1

		inputEvents.add(new StreamRecord<>(startEvent2, 2L)); //2
		inputEvents.add(new StreamRecord<>(startEvent3, 2L)); //3
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 2L)); //2

		inputEvents.add(new StreamRecord<>(startEvent4, 2L)); //4
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 2L)); //3
		inputEvents.add(new StreamRecord<Event>(middleEvent4, 2L)); //1
		inputEvents.add(new StreamRecord<>(endEvent, 4L));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle1").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 2178338526904474690L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getName().startsWith("foo");
			}
		}).followedBy("middle2").where(new IterativeCondition<Event>() {
			private static final long serialVersionUID = -1223388426808292695L;

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				if (!value.getName().equals("start")) {
					return false;
				}

				double sum = 0.0;
				for (Event e: ctx.getEventsForPattern("middle2")) {
					sum += e.getPrice();
				}
				sum += value.getPrice();
				return Double.compare(sum, 5.0) <= 0;
			}
		}).oneOrMore().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 562590474115118323L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, startEvent2, startEvent3, middleEvent1, endEvent),
				Lists.newArrayList(startEvent1, middleEvent1, startEvent2, endEvent),
				Lists.newArrayList(startEvent1, middleEvent2, startEvent4, endEvent),
				Lists.newArrayList(startEvent2, middleEvent2, startEvent4, endEvent),
				Lists.newArrayList(startEvent3, middleEvent2, startEvent4, endEvent)
			)
		);
	}

	@Test
	public void testIterativeWithPrevPatternDependencyAfterBranching() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(startEvent1, 1L));
		inputEvents.add(new StreamRecord<>(startEvent2, 2L));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 4L));
		inputEvents.add(new StreamRecord<>(startEvent3, 5L));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 6L));
		inputEvents.add(new StreamRecord<>(endEvent, 7L));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).oneOrMore().followedByAny("middle1").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 2178338526904474690L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getName().startsWith("foo");
			}
		}).followedByAny("end").where(new IterativeCondition<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value, Context<Event> ctx) throws Exception {
				if (!value.getName().equals("end")) {
					return false;
				}

				double sum = 0.0;
				for (Event event : ctx.getEventsForPattern("start")) {
					sum += event.getPrice();
				}
				return Double.compare(sum, 2.0) >= 0;
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns,
			Lists.<List<Event>>newArrayList(
				Lists.newArrayList(startEvent1, startEvent2, middleEvent1, endEvent),
				Lists.newArrayList(startEvent2, middleEvent1, endEvent),
				Lists.newArrayList(startEvent1, startEvent2, middleEvent2, endEvent),
				Lists.newArrayList(startEvent1, startEvent2, startEvent3, middleEvent2, endEvent),
				Lists.newArrayList(startEvent2, startEvent3, middleEvent2, endEvent),
				Lists.newArrayList(startEvent2, middleEvent2, endEvent),
				Lists.newArrayList(startEvent3, middleEvent2, endEvent)
			)
		);
	}
}
