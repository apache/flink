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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class NFAITCase extends TestLogger {

	@Test
	public void testSimplePatternNFA() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(41, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent = new Event(43, "end", 1.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(43, "foobar", 1.0), 2));
		inputEvents.add(new StreamRecord<Event>(new SubEvent(41, "barfoo", 1.0, 5.0), 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent, 3));
		inputEvents.add(new StreamRecord<>(new Event(43, "start", 1.0), 4));
		inputEvents.add(new StreamRecord<>(endEvent, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getVolume() > 5.0;
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).next("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).next("end").where(new SimpleCondition<Event>() {
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

		events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
		events.add(new StreamRecord<>(startEvent = new Event(2, "start", 1.0), 2));
		events.add(new StreamRecord<>(middleEvent = new Event(3, "middle", 1.0), 3));
		events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
		events.add(new StreamRecord<>(endEvent = new Event(5, "end", 1.0), 11));
		events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));


		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7907391379273505897L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -3268741540234334074L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -8995174172182138608L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(10));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		for (StreamRecord<Event> event: events) {
			Collection<Map<String, Event>> patterns = nfa.process(
					event.getValue(),
					event.getTimestamp()).f0;

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

		events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
		events.add(new StreamRecord<>(new Event(2, "start", 1.0), 2));
		events.add(new StreamRecord<>(new Event(3, "middle", 1.0), 3));
		events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
		events.add(new StreamRecord<>(new Event(5, "end", 1.0), 11));
		events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));

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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7907391379273505897L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -3268741540234334074L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
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

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
		inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
		inputEvents.add(new StreamRecord<>(endEvent, 8));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle-first").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getVolume() > 5.0;
			}
		}).followedByAny("middle-second").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getName().equals("next-one");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().allowCombinations().optional().followedByAny("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("end2").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).followedByAny("end3").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().allowCombinations().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent1, end1),
			Lists.newArrayList(startEvent, middleEvent2, end1),
			Lists.newArrayList(startEvent, end1)
		));
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
		inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
		inputEvents.add(new StreamRecord<>(middleEvent3, 6));
		inputEvents.add(new StreamRecord<>(end1, 7));

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
		}).oneOrMore().optional().followedBy("end1").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().followedBy("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("middle-first").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().allowCombinations().optional().followedByAny("middle-second").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).oneOrMore().allowCombinations().optional().followedBy("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("branching").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedByAny("merging").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("f");
			}
		}).followedByAny("kleene").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).oneOrMore().allowCombinations().optional().followedBy("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional()
			.next("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().allowCombinations().next("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(start, middleEvent1, middleEvent2, end),
			Lists.newArrayList(start, middleEvent2, end)
		));
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
		}).oneOrMore().allowCombinations().followedByAny("end1").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().allowCombinations().followedBy("end").where(new SimpleCondition<Event>() {
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).next("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).oneOrMore().optional().consecutive().followedBy("end").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().followedByAny("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent2, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent3, end1),
			Lists.newArrayList(startEvent, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent1, end1)
		));
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
		}).optional().followedBy("end1").where(new SimpleCondition<Event>() {
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
		}).times(2).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
			Lists.newArrayList(startEvent, middleEvent1, middleEvent3, end1)
		));
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).times(2).consecutive().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(middleEvent1, middleEvent2, end1),
			Lists.newArrayList(middleEvent2, middleEvent3, end1)
		));

	}

	@Test
	public void testTimesNonStrictWithNext() {
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
		}).times(2).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesNotStrictWithFollowedByEager() {
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
		}).times(2).followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesNotStrictWithFollowedByNotEager() {
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
		}).times(2).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent1, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesStrictWithNextAndConsecutive() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
		inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
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
		}).times(2).consecutive().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList());
	}

	@Test
	public void testStartWithOptional() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event end1 = new Event(44, "b", 5.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(end1, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).optional().followedBy("end1").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().optional();

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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(middleEvent1,  middleEvent2, middleEvent3),
			Lists.newArrayList(middleEvent1,  middleEvent2),
			Lists.newArrayList(middleEvent1),
			Lists.newArrayList(middleEvent2,  middleEvent3),
			Lists.newArrayList(middleEvent2),
			Lists.newArrayList(middleEvent3)
		));
	}

	@Test
	public void testEndWithOptional() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<>(middleEvent1, 3));

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
		}).oneOrMore();

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent,  middleEvent1, middleEvent2, middleEvent3),
			Lists.newArrayList(startEvent,  middleEvent1, middleEvent2),
			Lists.newArrayList(startEvent,  middleEvent1)
		));
	}

	///////////////////////////////         Optional           ////////////////////////////////////////

	@Test
	public void testTimesNonStrictOptional1() {
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
		}).times(3).optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesNonStrictOptional2() {
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
		}).times(2).allowCombinations().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesNonStrictOptional3() {
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
		}).times(2).optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end), // this exists because of the optional()
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesStrictOptional() {
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
		}).times(2).consecutive().optional().followedBy("end1").where(new SimpleCondition<Event>() { // TODO: 4/4/17 also check order consecutive() vs optional()
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testOneOrMoreStrictOptional() {
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
		}).oneOrMore().consecutive().optional().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesStrictOptional1() {
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
		}).times(2).consecutive().optional().followedBy("end1").where(new SimpleCondition<Event>() { // TODO: 4/4/17 also check order consecutive() vs optional()
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testOptionalTimesNonStrictWithNext() {
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
		}).times(2).allowCombinations().optional().followedBy("end1").where(new SimpleCondition<Event>() { // TODO: 4/4/17 also check order consecutive() vs optional()
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
				Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	///////////////////////////////         Consecutive           ////////////////////////////////////////

	private static class ConsecutiveData {
		static final Event startEvent = new Event(40, "c", 1.0);
		static final Event middleEvent1 = new Event(41, "a", 2.0);
		static final Event middleEvent2 = new Event(42, "a", 3.0);
		static final Event middleEvent3 = new Event(43, "a", 4.0);
		static final Event middleEvent4 = new Event(43, "a", 5.0);
		static final Event end = new Event(44, "b", 5.0);

		private ConsecutiveData() {
		}
	}

	@Test
	public void testStrictOneOrMore() {
		List<List<Event>> resultingPatterns = testOneOrMore(Quantifier.ConsumingStrategy.STRICT);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end)
		));
	}

	@Test
	public void testSkipTillNextOneOrMore() {
		List<List<Event>> resultingPatterns = testOneOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end)
		));
	}

	@Test
	public void testSkipTillAnyOneOrMore() {
		List<List<Event>> resultingPatterns = testOneOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_ANY);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end)
		));
	}

	private List<List<Event>> testOneOrMore(Quantifier.ConsumingStrategy strategy) {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent4, 6));
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
		}).oneOrMore();

		switch (strategy) {
			case STRICT:
				pattern = pattern.consecutive();
				break;
			case SKIP_TILL_NEXT:
				break;
			case SKIP_TILL_ANY:
				pattern = pattern.allowCombinations();
				break;
		}

		pattern = pattern.followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		return feedNFA(inputEvents, nfa);
	}

	@Test
	public void testStrictEagerZeroOrMore() {
		List<List<Event>> resultingPatterns = testZeroOrMore(Quantifier.ConsumingStrategy.STRICT);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testSkipTillAnyZeroOrMore() {
		List<List<Event>> resultingPatterns = testZeroOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_ANY);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	@Test
	public void testSkipTillNextZeroOrMore() {
		List<List<Event>> resultingPatterns = testZeroOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.middleEvent4, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)
		));
	}

	private List<List<Event>> testZeroOrMore(Quantifier.ConsumingStrategy strategy) {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 2));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 4));
		inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent4, 6));
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
		}).oneOrMore().optional();

		switch (strategy) {
			case STRICT:
				pattern = pattern.consecutive();
				break;
			case SKIP_TILL_NEXT:
				break;
			case SKIP_TILL_ANY:
				pattern = pattern.allowCombinations();
				break;
		}

		pattern = pattern.followedBy("end1").where(new SimpleCondition<Event>() {
					private static final long serialVersionUID = 5726188262756267490L;

					@Override
					public boolean filter(Event value) throws Exception {
						return value.getName().equals("b");
					}
				});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		return feedNFA(inputEvents, nfa);
	}

	@Test
	public void testTimesStrict() {
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
		}).times(2).consecutive().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	@Test
	public void testTimesNonStrict() {
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
		}).times(2).allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent2, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent1, ConsecutiveData.middleEvent3, ConsecutiveData.end),
			Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3, ConsecutiveData.end)
		));
	}

	@Test
	public void testStartWithZeroOrMoreStrict() {
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().optional().consecutive();

		testStartWithOneOrZeroOrMoreStrict(pattern);
	}

	@Test
	public void testStartWithOneOrMoreStrict() {

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).oneOrMore().consecutive();

		testStartWithOneOrZeroOrMoreStrict(pattern);
	}

	private void testStartWithOneOrZeroOrMoreStrict(Pattern<Event, ?> pattern) {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 4));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
		inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(ConsecutiveData.middleEvent1),
			Lists.newArrayList(ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3),
			Lists.newArrayList(ConsecutiveData.middleEvent2),
			Lists.newArrayList(ConsecutiveData.middleEvent3)
		));
	}

	///////////////////////////////     Clearing SharedBuffer     ////////////////////////////////////////

	@Test
	public void testTimesClearingBuffer() {
		Event startEvent = new Event(40, "c", 1.0);
		Event middleEvent1 = new Event(41, "a", 2.0);
		Event middleEvent2 = new Event(42, "a", 3.0);
		Event middleEvent3 = new Event(43, "a", 4.0);
		Event end1 = new Event(44, "b", 5.0);

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
		}).times(2).followedBy("end1").where(new SimpleCondition<Event>() {
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
		}).optional().followedBy("end1").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().allowCombinations().followedBy("end1").where(new SimpleCondition<Event>() {
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
		}).oneOrMore().allowCombinations().optional().followedBy("end1").where(new SimpleCondition<Event>() {
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
	public void testIterativeWithBranchingPatternEager() {
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
	public void testIterativeWithBranchingPatternCombinations() {
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

	private List<List<Event>> testIterativeWithBranchingPattern(boolean eager) {
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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

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
	public void testIterativeWithLoopingStartingEager() {
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
	public void testIterativeWithLoopingStartingCombination() {
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

	private List<List<Event>> testIterativeWithLoopingStarting(boolean eager) {
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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

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
	public void testIterativeWithPrevPatternDependency() {
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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

		compareMaps(resultingPatterns,
				Lists.<List<Event>>newArrayList(
						Lists.newArrayList(startEvent1, startEvent2, endEvent),
						Lists.newArrayList(startEvent2, endEvent)
				)
		);
	}

	@Test
	public void testIterativeWithABACPattern() {
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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

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
	public void testIterativeWithPrevPatternDependencyAfterBranching() {
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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

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


	///////////////////////////////////////   Skip till next     /////////////////////////////

	@Test
	public void testBranchingPatternSkipTillNext() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "start", 1.0);
		SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
		SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
		SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
		SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
		SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
		Event endEvent=  new Event(46, "end", 1.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
		inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
		inputEvents.add(new StreamRecord<>(endEvent, 8));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle-first").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getVolume() > 5.0;
			}
		}).followedBy("middle-second").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getName().equals("next-one");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> patterns = feedNFA(inputEvents, nfa);

		compareMaps(patterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, nextOne1, endEvent)
		));
	}

	@Test
	public void testBranchingPatternMixedFollowedBy() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(40, "start", 1.0);
		SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
		SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
		SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
		SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
		SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
		Event endEvent=  new Event(46, "end", 1.0);

		inputEvents.add(new StreamRecord<>(startEvent, 1));
		inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
		inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
		inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
		inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
		inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
		inputEvents.add(new StreamRecord<>(endEvent, 8));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle-first").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getVolume() > 5.0;
			}
		}).followedBy("middle-second").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			private static final long serialVersionUID = 6215754202506583964L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getName().equals("next-one");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7056763917392056548L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer(), false);

		final List<List<Event>> patterns = feedNFA(inputEvents, nfa);

		compareMaps(patterns, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(startEvent, middleEvent1, nextOne1, endEvent),
			Lists.newArrayList(startEvent, middleEvent2, nextOne1, endEvent),
			Lists.newArrayList(startEvent, middleEvent3, nextOne1, endEvent)
		));
	}


	/////////////////////////////////////////    Utility      ////////////////////////////////
	private List<List<Event>> feedNFA(List<StreamRecord<Event>> inputEvents, NFA<Event> nfa) {
		List<List<Event>> resultingPatterns = new ArrayList<>();

		for (StreamRecord<Event> inputEvent : inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getValue(),
				inputEvent.getTimestamp()).f0;

			for (Map<String, Event> p: patterns) {
				resultingPatterns.add(new ArrayList<>(p.values()));
			}
		}
		return resultingPatterns;
	}

	private void compareMaps(List<List<Event>> actual, List<List<Event>> expected) {
		Assert.assertEquals(expected.size(), actual.size());

		for (List<Event> p: actual) {
			Collections.sort(p, new EventComparator());
		}

		for (List<Event> p: expected) {
			Collections.sort(p, new EventComparator());
		}

		Collections.sort(actual, new ListEventComparator());
		Collections.sort(expected, new ListEventComparator());
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}

	private class ListEventComparator implements Comparator<List<Event>> {

		@Override
		public int compare(List<Event> o1, List<Event> o2) {
			int sizeComp = Integer.compare(o1.size(), o2.size());
			if (sizeComp == 0) {
				EventComparator comp = new EventComparator();
				for (int i = 0; i < o1.size(); i++) {
					int eventComp = comp.compare(o1.get(i), o2.get(i));
					if (eventComp != 0) {
						return eventComp;
					}
				}
				return 0;
			} else {
				return sizeComp;
			}
		}
	}

	private class EventComparator implements Comparator<Event> {

		@Override
		public int compare(Event o1, Event o2) {
			int nameComp = o1.getName().compareTo(o2.getName());
			int priceComp = Doubles.compare(o1.getPrice(), o2.getPrice());
			int idComp = Integer.compare(o1.getId(), o2.getId());
			if (nameComp == 0) {
				if (priceComp == 0) {
					return idComp;
				} else {
					return priceComp;
				}
			} else {
				return nameComp;
			}
		}
	}
}
