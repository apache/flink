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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SharedBuffer}.
 */
public class SharedBufferTest extends TestLogger {

	@Test
	public void testSharedBuffer() throws Exception {
		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		EventId[] eventIds = new EventId[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
			eventIds[i] = sharedBuffer.registerEvent(events[i], timestamp);
		}

		Map<String, List<Event>> expectedPattern1 = new HashMap<>();
		expectedPattern1.put("a1", new ArrayList<>());
		expectedPattern1.get("a1").add(events[2]);

		expectedPattern1.put("a[]", new ArrayList<>());
		expectedPattern1.get("a[]").add(events[3]);

		expectedPattern1.put("b", new ArrayList<>());
		expectedPattern1.get("b").add(events[5]);

		Map<String, List<Event>> expectedPattern2 = new HashMap<>();
		expectedPattern2.put("a1", new ArrayList<>());
		expectedPattern2.get("a1").add(events[0]);

		expectedPattern2.put("a[]", new ArrayList<>());
		expectedPattern2.get("a[]").add(events[1]);
		expectedPattern2.get("a[]").add(events[2]);
		expectedPattern2.get("a[]").add(events[3]);
		expectedPattern2.get("a[]").add(events[4]);

		expectedPattern2.put("b", new ArrayList<>());
		expectedPattern2.get("b").add(events[5]);

		Map<String, List<Event>> expectedPattern3 = new HashMap<>();
		expectedPattern3.put("a1", new ArrayList<>());
		expectedPattern3.get("a1").add(events[0]);

		expectedPattern3.put("a[]", new ArrayList<>());
		expectedPattern3.get("a[]").add(events[1]);
		expectedPattern3.get("a[]").add(events[2]);
		expectedPattern3.get("a[]").add(events[3]);
		expectedPattern3.get("a[]").add(events[4]);
		expectedPattern3.get("a[]").add(events[5]);
		expectedPattern3.get("a[]").add(events[6]);

		expectedPattern3.put("b", new ArrayList<>());
		expectedPattern3.get("b").add(events[7]);

		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {
			NodeId a10 = sharedBufferAccessor.put("a1", eventIds[0], null, DeweyNumber.fromString("1"));
			NodeId aLoop0 = sharedBufferAccessor.put("a[]", eventIds[1], a10, DeweyNumber.fromString("1.0"));
			NodeId a11 = sharedBufferAccessor.put("a1", eventIds[2], null, DeweyNumber.fromString("2"));
			NodeId aLoop1 = sharedBufferAccessor.put("a[]", eventIds[2], aLoop0, DeweyNumber.fromString("1.0"));
			NodeId aLoop2 = sharedBufferAccessor.put("a[]", eventIds[3], aLoop1, DeweyNumber.fromString("1.0"));
			NodeId aSecondLoop0 = sharedBufferAccessor.put("a[]", eventIds[3], a11, DeweyNumber.fromString("2.0"));
			NodeId aLoop3 = sharedBufferAccessor.put("a[]", eventIds[4], aLoop2, DeweyNumber.fromString("1.0"));
			NodeId b0 = sharedBufferAccessor.put("b", eventIds[5], aLoop3, DeweyNumber.fromString("1.0.0"));
			NodeId aLoop4 = sharedBufferAccessor.put("a[]", eventIds[5], aLoop3, DeweyNumber.fromString("1.1"));
			NodeId b1 = sharedBufferAccessor.put("b", eventIds[5], aSecondLoop0, DeweyNumber.fromString("2.0.0"));
			NodeId aLoop5 = sharedBufferAccessor.put("a[]", eventIds[6], aLoop4, DeweyNumber.fromString("1.1"));
			NodeId b3 = sharedBufferAccessor.put("b", eventIds[7], aLoop5, DeweyNumber.fromString("1.1.0"));

			List<Map<String, List<EventId>>> patterns3 = sharedBufferAccessor.extractPatterns(b3,
				DeweyNumber.fromString("1.1.0"));
			assertEquals(1L, patterns3.size());
			assertEquals(expectedPattern3, sharedBufferAccessor.materializeMatch(patterns3.get(0)));
			sharedBufferAccessor.releaseNode(b3);

			List<Map<String, List<EventId>>> patterns4 = sharedBufferAccessor.extractPatterns(b3,
				DeweyNumber.fromString("1.1.0"));
			assertEquals(0L, patterns4.size());
			assertTrue(patterns4.isEmpty());

			List<Map<String, List<EventId>>> patterns1 = sharedBufferAccessor.extractPatterns(b1,
				DeweyNumber.fromString("2.0.0"));
			assertEquals(1L, patterns1.size());
			assertEquals(expectedPattern1, sharedBufferAccessor.materializeMatch(patterns1.get(0)));

			List<Map<String, List<EventId>>> patterns2 = sharedBufferAccessor.extractPatterns(b0,
				DeweyNumber.fromString("1.0.0"));
			assertEquals(1L, patterns2.size());
			assertEquals(expectedPattern2, sharedBufferAccessor.materializeMatch(patterns2.get(0)));
			sharedBufferAccessor.releaseNode(b1);
			sharedBufferAccessor.releaseNode(b0);

			for (EventId eventId : eventIds) {
				sharedBufferAccessor.releaseEvent(eventId);
			}
		}

		assertTrue(sharedBuffer.isEmpty());
	}

	@Test
	public void testClearingSharedBufferWithMultipleEdgesBetweenEntries() throws Exception {
		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		EventId[] eventIds = new EventId[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
			eventIds[i] = sharedBuffer.registerEvent(events[i], timestamp);
		}

		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {
			NodeId start = sharedBufferAccessor.put("start", eventIds[1], null, DeweyNumber.fromString("1"));
			NodeId b0 = sharedBufferAccessor.put("branching", eventIds[2], start, DeweyNumber.fromString("1.0"));
			NodeId b1 = sharedBufferAccessor.put("branching", eventIds[3], start, DeweyNumber.fromString("1.1"));
			NodeId b00 = sharedBufferAccessor.put("branching", eventIds[3], b0, DeweyNumber.fromString("1.0.0"));
			sharedBufferAccessor.put("branching", eventIds[4], b00, DeweyNumber.fromString("1.0.0.0"));
			NodeId b10 = sharedBufferAccessor.put("branching", eventIds[4], b1, DeweyNumber.fromString("1.1.0"));

			//simulate IGNORE (next event can point to events[2])
			sharedBufferAccessor.lockNode(b0);

			sharedBufferAccessor.releaseNode(b10);

			for (EventId eventId : eventIds) {
				sharedBufferAccessor.releaseEvent(eventId);
			}
		}

		//There should be still events[1] and events[2] in the buffer
		assertFalse(sharedBuffer.isEmpty());
	}

	@Test
	public void testSharedBufferExtractOrder() throws Exception {
		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		int numberEvents = 5;
		Event[] events = new Event[numberEvents];
		EventId[] eventIds = new EventId[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
			eventIds[i] = sharedBuffer.registerEvent(events[i], timestamp);
		}

		Map<String, List<Event>> expectedResult = new LinkedHashMap<>();
		expectedResult.put("a", new ArrayList<>());
		expectedResult.get("a").add(events[0]);
		expectedResult.put("b", new ArrayList<>());
		expectedResult.get("b").add(events[1]);
		expectedResult.put("aa", new ArrayList<>());
		expectedResult.get("aa").add(events[2]);
		expectedResult.put("bb", new ArrayList<>());
		expectedResult.get("bb").add(events[3]);
		expectedResult.put("c", new ArrayList<>());
		expectedResult.get("c").add(events[4]);

		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {
			NodeId a = sharedBufferAccessor.put("a", eventIds[0], null, DeweyNumber.fromString("1"));
			NodeId b = sharedBufferAccessor.put("b", eventIds[1], a, DeweyNumber.fromString("1.0"));
			NodeId aa = sharedBufferAccessor.put("aa", eventIds[2], b, DeweyNumber.fromString("1.0.0"));
			NodeId bb = sharedBufferAccessor.put("bb", eventIds[3], aa, DeweyNumber.fromString("1.0.0.0"));
			NodeId c = sharedBufferAccessor.put("c", eventIds[4], bb, DeweyNumber.fromString("1.0.0.0.0"));

			Map<String, List<Event>> patternsResult = sharedBufferAccessor.materializeMatch(sharedBufferAccessor.extractPatterns(c,
				DeweyNumber.fromString("1.0.0.0.0")).get(0));

			List<String> expectedOrder = new ArrayList<>();
			expectedOrder.add("a");
			expectedOrder.add("b");
			expectedOrder.add("aa");
			expectedOrder.add("bb");
			expectedOrder.add("c");

			for (EventId eventId : eventIds) {
				sharedBufferAccessor.releaseEvent(eventId);
			}
			List<String> resultOrder = new ArrayList<>(patternsResult.keySet());
			assertEquals(expectedOrder, resultOrder);
		}

	}

	@Test
	public void testSharedBufferCountersClearing() throws Exception {
		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		int numberEvents = 4;
		Event[] events = new Event[numberEvents];

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
			sharedBuffer.registerEvent(events[i], i);
		}

		sharedBuffer.advanceTime(3);

		Iterator<Map.Entry<Long, Integer>> counters = sharedBuffer.getEventCounters();
		Map.Entry<Long, Integer> entry = counters.next();
		assertEquals(3, entry.getKey().longValue());
		assertEquals(1, entry.getValue().intValue());
		assertFalse(counters.hasNext());
	}

	@Test
	public void testSharedBufferAccessor() throws Exception {
		TestSharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		EventId[] eventIds = new EventId[numberEvents];
		final long timestamp = 1L;

		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {
			for (int i = 0; i < numberEvents; i++) {
				events[i] = new Event(i + 1, "e" + (i + 1), i);
				eventIds[i] = sharedBufferAccessor.registerEvent(events[i], timestamp);
			}
			assertEquals(8, sharedBuffer.getEventsBufferCacheSize());
			assertEquals(0, sharedBuffer.getSharedBufferNodeCacheSize());

			NodeId start = sharedBufferAccessor.put("start", eventIds[1], null, DeweyNumber.fromString("1"));
			NodeId b0 = sharedBufferAccessor.put("branching", eventIds[2], start, DeweyNumber.fromString("1.0"));
			NodeId b1 = sharedBufferAccessor.put("branching", eventIds[3], start, DeweyNumber.fromString("1.1"));
			NodeId b00 = sharedBufferAccessor.put("branching", eventIds[3], b0, DeweyNumber.fromString("1.0.0"));
			sharedBufferAccessor.put("branching", eventIds[4], b00, DeweyNumber.fromString("1.0.0.0"));

			assertEquals(4, sharedBuffer.getSharedBufferNodeCacheSize());
			assertEquals(0, sharedBuffer.getSharedBufferNodeSize());

			sharedBufferAccessor.lockNode(b0);

			for (EventId eventId : eventIds) {
				sharedBufferAccessor.releaseEvent(eventId);
			}
		}
		assertEquals(0, sharedBuffer.getEventsBufferCacheSize());
		assertEquals(4, sharedBuffer.getEventsBufferSize());
		assertEquals(0, sharedBuffer.getSharedBufferNodeCacheSize());
		assertEquals(4, sharedBuffer.getSharedBufferNodeSize());
	}

	/**
	 * Test releasing a node which has a long path to the terminal node (the node without an out-going edge).
	 * @throws Exception if creating the shared buffer accessor fails.
	 */
	@Test
	public void testReleaseNodesWithLongPath() throws Exception {
		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());

		final int numberEvents = 100000;
		Event[] events = new Event[numberEvents];
		EventId[] eventIds = new EventId[numberEvents];
		NodeId[] nodeIds = new NodeId[numberEvents];

		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
			eventIds[i] = sharedBuffer.registerEvent(events[i], timestamp);
		}

		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {

			for (int i = 0; i < numberEvents; i++) {
				NodeId prevId = i == 0 ? null : nodeIds[i - 1];
				nodeIds[i] = sharedBufferAccessor.put("n" + i, eventIds[i], prevId, DeweyNumber.fromString("1.0"));
			}

			NodeId lastNode = nodeIds[numberEvents - 1];
			sharedBufferAccessor.releaseNode(lastNode);

			for (int i = 0; i < numberEvents; i++) {
				sharedBufferAccessor.releaseEvent(eventIds[i]);
			}
		}

		assertTrue(sharedBuffer.isEmpty());
	}

}
