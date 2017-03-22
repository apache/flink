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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NFAITCase extends TestLogger {

	@Test
	public void testSimplePatternNFA() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(41, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent = new Event(43, "end", 1.0);

		inputEvents.add(new StreamRecord<Event>(startEvent, 1));
		inputEvents.add(new StreamRecord<Event>(new Event(43, "foobar", 1.0), 2));
		inputEvents.add(new StreamRecord<Event>(new SubEvent(41, "barfoo", 1.0, 5.0), 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent, 3));
		inputEvents.add(new StreamRecord<Event>(new Event(43, "start", 1.0), 4));
		inputEvents.add(new StreamRecord<Event>(endEvent, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
		.followedBy("middle").subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
				private static final long serialVersionUID = 6215754202506583964L;

				@Override
				public boolean filter(SubEvent value) throws Exception {
					return value.getVolume() > 5.0;
				}
			})
		.followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<Map<String, Event>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent: inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			resultingPatterns.addAll(patterns);
		}

		assertEquals(1, resultingPatterns.size());
		Map<String, Event> patternMap = resultingPatterns.get(0);

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));
	}

	@Test
	public void testStrictContinuityWithResults() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event middleEvent1 = new Event(41, "a", 2.0);
		Event end = new Event(42, "b", 4.0);

		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(end, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).next("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(1, allPatterns.size());
		assertEquals(Sets.<Set<Event>>newHashSet(
			Sets.newHashSet(middleEvent1, end)
		), resultingPatterns);
	}

	@Test
	public void testStrictContinuityNoResults() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "c", 3.0);
		Event end = new Event(43, "b", 4.0);

		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(end, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).next("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
			}
		}

		assertEquals(Sets.newHashSet(), resultingPatterns);
	}

	/**
	 * Tests that the NFA successfully filters out expired elements with respect to the window
	 * length
	 */
	@Test
	public void testSimplePatternWithTimeWindowNFA() {
		List<StreamRecord<Event>> events = new ArrayList<>();
		List<Map<String, Event>> resultingPatterns = new ArrayList<>();

		final Event startEvent;
		final Event middleEvent;
		final Event endEvent;

		events.add(new StreamRecord<Event>(new Event(1, "start", 1.0), 1));
		events.add(new StreamRecord<Event>(startEvent = new Event(2, "start", 1.0), 2));
		events.add(new StreamRecord<Event>(middleEvent = new Event(3, "middle", 1.0), 3));
		events.add(new StreamRecord<Event>(new Event(4, "foobar", 1.0), 4));
		events.add(new StreamRecord<Event>(endEvent = new Event(5, "end", 1.0), 11));
		events.add(new StreamRecord<Event>(new Event(6, "end", 1.0), 13));


		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 7907391379273505897L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -3268741540234334074L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -8995174172182138608L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(10));


		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		for (StreamRecord<Event> event: events) {
			Collection<Map<String, Event>> patterns = nfa.process(event.getValue(), event.getTimestamp()).f0;

			resultingPatterns.addAll(patterns);
		}

		assertEquals(1, resultingPatterns.size());

		Map<String, Event> patternMap = resultingPatterns.get(0);

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));
	}

	/**
	 * Tests that the NFA successfully returns partially matched event sequences when they've timed
	 * out.
	 */
	@Test
	public void testSimplePatternWithTimeoutHandling() {
		List<StreamRecord<Event>> events = new ArrayList<>();
		List<Map<String, Event>> resultingPatterns = new ArrayList<>();
		Set<Tuple2<Map<String, Event>, Long>> resultingTimeoutPatterns = new HashSet<>();
		Set<Tuple2<Map<String, Event>, Long>> expectedTimeoutPatterns = new HashSet<>();


		events.add(new StreamRecord<Event>(new Event(1, "start", 1.0), 1));
		events.add(new StreamRecord<Event>(new Event(2, "start", 1.0), 2));
		events.add(new StreamRecord<Event>(new Event(3, "middle", 1.0), 3));
		events.add(new StreamRecord<Event>(new Event(4, "foobar", 1.0), 4));
		events.add(new StreamRecord<Event>(new Event(5, "end", 1.0), 11));
		events.add(new StreamRecord<Event>(new Event(6, "end", 1.0), 13));

		Map<String, Event> timeoutPattern1 = new HashMap<>();
		timeoutPattern1.put("start", new Event(1, "start", 1.0));
		timeoutPattern1.put("middle", new Event(3, "middle", 1.0));

		Map<String, Event> timeoutPattern2 = new HashMap<>();
		timeoutPattern2.put("start", new Event(2, "start", 1.0));
		timeoutPattern2.put("middle", new Event(3, "middle", 1.0));

		Map<String, Event> timeoutPattern3 = new HashMap<>();
		timeoutPattern3.put("start", new Event(1, "start", 1.0));

		Map<String, Event> timeoutPattern4 = new HashMap<>();
		timeoutPattern4.put("start", new Event(2, "start", 1.0));

		expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern1, 11L));
		expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern2, 13L));
		expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern3, 11L));
		expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern4, 13L));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 7907391379273505897L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -3268741540234334074L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = -8995174172182138608L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(10));


		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), true);

		for (StreamRecord<Event> event: events) {
			Tuple2<Collection<Map<String, Event>>, Collection<Tuple2<Map<String, Event>, Long>>> patterns =
				nfa.process(event.getValue(), event.getTimestamp());

			Collection<Map<String, Event>> matchedPatterns = patterns.f0;
			Collection<Tuple2<Map<String, Event>, Long>> timeoutPatterns = patterns.f1;

			resultingPatterns.addAll(matchedPatterns);
			resultingTimeoutPatterns.addAll(timeoutPatterns);
		}

		assertEquals(1, resultingPatterns.size());
		assertEquals(expectedTimeoutPatterns.size(), resultingTimeoutPatterns.size());

		assertEquals(expectedTimeoutPatterns, resultingTimeoutPatterns);
	}

	@Test
	public void testBranchingPattern() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "start", 1.0);
		SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
		SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
		SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
		SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
		SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
		Event endEvent=  new Event(46, "end", 1.0);

		inputEvents.add(new StreamRecord<Event>(startEvent, 1));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
		inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
		inputEvents.add(new StreamRecord<Event>(endEvent, 8));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
			.followedBy("middle-first").subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
				private static final long serialVersionUID = 6215754202506583964L;

				@Override
				public boolean filter(SubEvent value) throws Exception {
					return value.getVolume() > 5.0;
				}
			})
			.followedBy("middle-second").subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
				private static final long serialVersionUID = 6215754202506583964L;

				@Override
				public boolean filter(SubEvent value) throws Exception {
					return value.getName().equals("next-one");
				}
			})
			.followedBy("end").where(new FilterFunction<Event>() {
				private static final long serialVersionUID = 7056763917392056548L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<Map<String, Event>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent: inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			resultingPatterns.addAll(patterns);
		}

		assertEquals(6, resultingPatterns.size());

		final Set<Set<Event>> patterns = new HashSet<>();
		for (Map<String, Event> resultingPattern : resultingPatterns) {
			patterns.add(new HashSet<>(resultingPattern.values()));
		}

		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, nextOne1, endEvent),
			Sets.newHashSet(startEvent, middleEvent2, nextOne1, endEvent),
			Sets.newHashSet(startEvent, middleEvent3, nextOne1, endEvent),
			Sets.newHashSet(startEvent, middleEvent1, nextOne2, endEvent),
			Sets.newHashSet(startEvent, middleEvent2, nextOne2, endEvent),
			Sets.newHashSet(startEvent, middleEvent3, nextOne2, endEvent)
		), patterns);
	}

	@Test
	public void testComplexBranchingAfterZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);
		Event end2 = new Event(45, "d", 6.0);
		Event end3 = new Event(46, "d", 7.0);
		Event end4 = new Event(47, "e", 8.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));
		inputEvents.add(new StreamRecord<>(end2, 7));
		inputEvents.add(new StreamRecord<>(end3, 8));
		inputEvents.add(new StreamRecord<>(end4, 9));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(false).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		})
			.followedBy("end2").where(new FilterFunction<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("d");
				}
			})
			.followedBy("end3").where(new FilterFunction<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("e");
				}
			});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(16, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, middleEvent3, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent3, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent2, middleEvent3, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent1, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent2, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent3, end1, end2, end4),
			Sets.newHashSet(startEvent, end1, end2, end4),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, middleEvent3, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent3, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent2, middleEvent3, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent1, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent2, end1, end3, end4),
			Sets.newHashSet(startEvent, middleEvent3, end1, end3, end4),
			Sets.newHashSet(startEvent, end1, end3, end4)
		), resultingPatterns);
	}

	@Test
	public void testZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(false).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(4, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1),
			Sets.newHashSet(startEvent, middleEvent1, end1),
			Sets.newHashSet(startEvent, middleEvent2, end1),
			Sets.newHashSet(startEvent, end1)
		), resultingPatterns);
	}

	@Test
	public void testEagerZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(true).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(4, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1),
			Sets.newHashSet(startEvent, middleEvent1, end1),
			Sets.newHashSet(startEvent, end1)
		), resultingPatterns);
	}


	@Test
	public void testBeginWithZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event middleEvent1 = new Event(40, "a", 2.0);
		Event middleEvent2 = new Event(41, "a", 3.0);
		Event middleEvent3 = new Event(41, "a", 3.0);
		Event end = new Event(42, "b", 4.0);

		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore().followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(7, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(middleEvent1, middleEvent2, middleEvent3, end),
			Sets.newHashSet(middleEvent1, middleEvent2, end),
			Sets.newHashSet(middleEvent2, middleEvent3, end),
			Sets.newHashSet(middleEvent1, end),
			Sets.newHashSet(middleEvent2, end),
			Sets.newHashSet(middleEvent3, end),
			Sets.newHashSet(end)
		), resultingPatterns);
	}

	@Test
	public void testZeroOrMoreAfterZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "d", 3.0);
		Event middleEvent3 = new Event(43, "d", 4.0);
		Event end = new Event(44, "e", 4.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle-first").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(false).followedBy("middle-second").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).zeroOrMore(false).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("e");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(8, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, middleEvent3, end),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent3, end),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end),
			Sets.newHashSet(startEvent, middleEvent2, middleEvent3, end),
			Sets.newHashSet(startEvent, middleEvent3, end),
			Sets.newHashSet(startEvent, middleEvent2, end),
			Sets.newHashSet(startEvent, middleEvent1, end),
			Sets.newHashSet(startEvent, end)
		), resultingPatterns);
	}

	@Test
	public void testZeroOrMoreAfterBranching() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event merging = new Event(42, "f", 3.0);
		Event kleene1 = new Event(43, "d", 4.0);
		Event kleene2 = new Event(44, "d", 4.0);
		Event end = new Event(45, "e", 4.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(merging, 5));
		inputEvents.add(new StreamRecord<>(kleene1, 6));
		inputEvents.add(new StreamRecord<>(kleene2, 7));
		inputEvents.add(new StreamRecord<>(end, 8));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("branching").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedBy("merging").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("f");
			}
		}).followedBy("kleene").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).zeroOrMore(false).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("e");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(8, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, merging, end),
			Sets.newHashSet(startEvent, middleEvent1, merging, kleene1, end),
			Sets.newHashSet(startEvent, middleEvent1, merging, kleene2, end),
			Sets.newHashSet(startEvent, middleEvent1, merging, kleene1, kleene2, end),
			Sets.newHashSet(startEvent, middleEvent2, merging, end),
			Sets.newHashSet(startEvent, middleEvent2, merging, kleene1, end),
			Sets.newHashSet(startEvent, middleEvent2, merging, kleene2, end),
			Sets.newHashSet(startEvent, middleEvent2, merging, kleene1, kleene2, end)
		), resultingPatterns);
	}

	@Test
	public void testStrictContinuityNoResultsAfterZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event start = new Event(40, "d", 2.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 2.0);
		Event middleEvent3 = new Event(43, "c", 3.0);
		Event end = new Event(44, "b", 4.0);

		inputEvents.add(new StreamRecord<>(start, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(end, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore()
			.next("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();


		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
			}
		}

		assertEquals(Sets.newHashSet(), resultingPatterns);
	}

	@Test
	public void testStrictContinuityResultsAfterZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event start = new Event(40, "d", 2.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 2.0);
		Event end = new Event(43, "b", 4.0);

		inputEvents.add(new StreamRecord<>(start, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(end, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(false)
			.next("end").where(new FilterFunction<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("b");
				}
			});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(2, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(start, middleEvent1, middleEvent2, end),
			Sets.newHashSet(start, middleEvent2, end)
		), resultingPatterns);
	}

	@Test
	public void testAtLeastOne() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore(false).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(3, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1),
			Sets.newHashSet(startEvent, middleEvent1, end1),
			Sets.newHashSet(startEvent, middleEvent2, end1)
		), resultingPatterns);
	}

	@Test
	public void testBeginWithAtLeastOne() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent1 = new Event(41, "a", 2.0);
		Event startEvent2 = new Event(42, "a", 3.0);
		Event startEvent3 = new Event(42, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent1, 3));
		inputEvents.add(new StreamRecord<>(startEvent2, 4));
		inputEvents.add(new StreamRecord<>(startEvent3, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore(false).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(7, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent1, startEvent2, startEvent3, end1),
			Sets.newHashSet(startEvent1, startEvent2, end1),
			Sets.newHashSet(startEvent1, startEvent3, end1),
			Sets.newHashSet(startEvent2, startEvent3, end1),
			Sets.newHashSet(startEvent1, end1),
			Sets.newHashSet(startEvent2, end1),
			Sets.newHashSet(startEvent3, end1)
		), resultingPatterns);
	}

	@Test
	public void testNextZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "start", 1.0);
		Event middleEvent1 = new Event(40, "middle", 2.0);
		Event middleEvent2 = new Event(40, "middle", 3.0);
		Event middleEvent3 = new Event(40, "middle", 4.0);
		Event endEvent = new Event(46, "end", 1.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1L));
		inputEvents.add(new StreamRecord<>(new Event(1, "event", 1.0), 2L));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3L));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4L));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5L));
		inputEvents.add(new StreamRecord<>(endEvent, 6L));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).next("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).zeroOrMore(false).followedBy("end").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(1, allPatterns.size());
		assertEquals(Sets.<Set<Event>>newHashSet(
			Sets.newHashSet(startEvent, endEvent)
		), resultingPatterns);
	}

	@Test
	public void testAtLeastOneEager() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore(true).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(3, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1),
			Sets.newHashSet(startEvent, middleEvent1, end1)
		), resultingPatterns);
	}

	@Test
	public void testOptional() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).optional().followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(2, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent, middleEvent, end1),
			Sets.newHashSet(startEvent, end1)
		), resultingPatterns);
	}


	@Test
	public void testTimes() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(1, allPatterns.size());
		assertEquals(Sets.<Set<Event>>newHashSet(
			Sets.newHashSet(startEvent, middleEvent1, middleEvent2, end1)
		), resultingPatterns);
	}

	@Test
	public void testStartWithTimes() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(middleEvent2, 3));
		inputEvents.add(new StreamRecord<>(middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(2, allPatterns.size());
		assertEquals(Sets.<Set<Event>>newHashSet(
			Sets.newHashSet(middleEvent1, middleEvent2, end1),
			Sets.newHashSet(middleEvent2, middleEvent3, end1)
		), resultingPatterns);
	}

	@Test
	public void testStartWithOptional() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).optional().followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(2, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent,  end1),
			Sets.newHashSet(end1)
		), resultingPatterns);
	}

	@Test
	public void testEndWithZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(4, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent,  middleEvent1, middleEvent2, middleEvent3),
			Sets.newHashSet(startEvent,  middleEvent1, middleEvent2),
			Sets.newHashSet(startEvent,  middleEvent1),
			Sets.newHashSet(startEvent)
		), resultingPatterns);
	}

	@Test
	public void testStartAndEndWithZeroOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "d", 5.0);
		Event end2 = new Event(45, "d", 5.0);
		Event end3 = new Event(46, "d", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<>(end1, 6));
		inputEvents.add(new StreamRecord<>(end2, 6));
		inputEvents.add(new StreamRecord<>(end3, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(6, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(middleEvent1,  middleEvent2, middleEvent3),
			Sets.newHashSet(middleEvent1,  middleEvent2),
			Sets.newHashSet(middleEvent1),
			Sets.newHashSet(middleEvent2,  middleEvent3),
			Sets.newHashSet(middleEvent2),
			Sets.newHashSet(middleEvent3)
		), resultingPatterns);
	}

	@Test
	public void testEndWithOptional() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).optional();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(2, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent,  middleEvent1),
			Sets.newHashSet(startEvent)
		), resultingPatterns);
	}

	@Test
	public void testEndWithOneOrMore() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(middleEvent3, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		Set<Set<Event>> resultingPatterns = new HashSet<>();
		List<Collection<Event>> allPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> foundPattern : patterns) {
				resultingPatterns.add(new HashSet<>(foundPattern.values()));
				allPatterns.add(foundPattern.values());
			}
		}

		assertEquals(3, allPatterns.size());
		assertEquals(Sets.newHashSet(
			Sets.newHashSet(startEvent,  middleEvent1, middleEvent2, middleEvent3),
			Sets.newHashSet(startEvent,  middleEvent1, middleEvent2),
			Sets.newHashSet(startEvent,  middleEvent1)
		), resultingPatterns);
	}

	/**
	 * Clearing SharedBuffer
	 */

	@Test
	public void testTimesClearingBuffer() {
		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).next("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).within(Time.milliseconds(8));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		nfa.process(startEvent, 1);
		nfa.process(middleEvent1, 2);
		nfa.process(middleEvent2, 3);
		nfa.process(middleEvent3, 4);
		nfa.process(end1, 6);

		//pruning element
		nfa.process(null, 10);

		assertEquals(true, nfa.isEmpty());
	}

	@Test
	public void testOptionalClearingBuffer() {
		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).optional().followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).within(Time.milliseconds(8));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		nfa.process(startEvent, 1);
		nfa.process(middleEvent, 5);
		nfa.process(end1, 6);

		//pruning element
		nfa.process(null, 10);

		assertEquals(true, nfa.isEmpty());
	}

	@Test
	public void testAtLeastOneClearingBuffer() {
		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event end1 = new Event(44, "b", 5.0);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore(false).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).within(Time.milliseconds(8));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		nfa.process(startEvent, 1);
		nfa.process(middleEvent1, 3);
		nfa.process(middleEvent2, 4);
		nfa.process(end1, 6);

		//pruning element
		nfa.process(null, 10);

		assertEquals(true, nfa.isEmpty());
	}


	@Test
	public void testZeroOrMoreClearingBuffer() {
		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event end1 = new Event(44, "b", 5.0);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).zeroOrMore(false).followedBy("end1").where(new FilterFunction<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).within(Time.milliseconds(8));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		nfa.process(startEvent, 1);
		nfa.process(middleEvent1, 3);
		nfa.process(middleEvent2, 4);
		nfa.process(end1, 6);

		//pruning element
		nfa.process(null, 10);

		assertEquals(true, nfa.isEmpty());
	}

}
