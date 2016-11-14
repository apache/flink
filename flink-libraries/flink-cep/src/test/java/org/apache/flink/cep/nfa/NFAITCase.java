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

public class NFAITCase extends TestLogger {

	@Test
	public void testSimplePatternNFA() {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(43, "end", 1.0);

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

}
