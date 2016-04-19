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
import org.apache.flink.cep.Event;
import org.apache.flink.cep.StreamEvent;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NFAITCase extends TestLogger {

	@Test
	public void testSimplePatternNFA() {
		List<StreamEvent<Event>> inputEvents = new ArrayList<StreamEvent<Event>>();

		Event startEvent = new Event(42, "start", 1.0);
		SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
		Event endEvent=  new Event(43, "end", 1.0);

		inputEvents.add(new StreamEvent<Event>(startEvent, 1));
		inputEvents.add(new StreamEvent<Event>(new Event(43, "foobar", 1.0), 2));
		inputEvents.add(new StreamEvent<Event>(new SubEvent(41, "barfoo", 1.0, 5.0), 3));
		inputEvents.add(new StreamEvent<Event>(middleEvent, 3));
		inputEvents.add(new StreamEvent<Event>(new Event(43, "start", 1.0), 4));
		inputEvents.add(new StreamEvent<Event>(endEvent, 5));

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

		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer());
		List<Map<String, Event>> resultingPatterns = new ArrayList<>();

		for (StreamEvent<Event> inputEvent: inputEvents) {
			Collection<Map<String, Event>> patterns = nfa.process(
				inputEvent.getEvent(),
				inputEvent.getTimestamp());

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
		List<StreamEvent<Event>> events = new ArrayList<>();
		List<Map<String, Event>> resultingPatterns = new ArrayList<>();

		final Event startEvent;
		final Event middleEvent;
		final Event endEvent;

		events.add(new StreamEvent<Event>(new Event(1, "start", 1.0), 1));
		events.add(new StreamEvent<Event>(startEvent = new Event(2, "start", 1.0), 2));
		events.add(new StreamEvent<Event>(middleEvent = new Event(3, "middle", 1.0), 3));
		events.add(new StreamEvent<Event>(new Event(4, "foobar", 1.0), 4));
		events.add(new StreamEvent<Event>(endEvent = new Event(5, "end", 1.0), 11));
		events.add(new StreamEvent<Event>(new Event(6, "end", 1.0), 13));


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


		NFA<Event> nfa = NFACompiler.compile(pattern, Event.createTypeSerializer());

		for (StreamEvent<Event> event: events) {
			Collection<Map<String, Event>> patterns = nfa.process(event.getEvent(), event.getTimestamp());

			resultingPatterns.addAll(patterns);
		}

		assertEquals(1, resultingPatterns.size());

		Map<String, Event> patternMap = resultingPatterns.get(0);

		assertEquals(startEvent, patternMap.get("start"));
		assertEquals(middleEvent, patternMap.get("middle"));
		assertEquals(endEvent, patternMap.get("end"));
	}
}
