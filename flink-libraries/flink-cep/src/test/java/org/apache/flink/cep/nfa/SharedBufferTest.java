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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cep.Event;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
	public void testSharedBuffer() {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>();
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
		}

		Map<String, List<Event>> expectedPattern1 = new HashMap<>();
		expectedPattern1.put("a1", new ArrayList<Event>());
		expectedPattern1.get("a1").add(events[2]);

		expectedPattern1.put("a[]", new ArrayList<Event>());
		expectedPattern1.get("a[]").add(events[3]);

		expectedPattern1.put("b", new ArrayList<Event>());
		expectedPattern1.get("b").add(events[5]);

		Map<String, List<Event>> expectedPattern2 = new HashMap<>();
		expectedPattern2.put("a1", new ArrayList<Event>());
		expectedPattern2.get("a1").add(events[0]);

		expectedPattern2.put("a[]", new ArrayList<Event>());
		expectedPattern2.get("a[]").add(events[1]);
		expectedPattern2.get("a[]").add(events[2]);
		expectedPattern2.get("a[]").add(events[3]);
		expectedPattern2.get("a[]").add(events[4]);

		expectedPattern2.put("b", new ArrayList<Event>());
		expectedPattern2.get("b").add(events[5]);

		Map<String, List<Event>> expectedPattern3 = new HashMap<>();
		expectedPattern3.put("a1", new ArrayList<Event>());
		expectedPattern3.get("a1").add(events[0]);

		expectedPattern3.put("a[]", new ArrayList<Event>());
		expectedPattern3.get("a[]").add(events[1]);
		expectedPattern3.get("a[]").add(events[2]);
		expectedPattern3.get("a[]").add(events[3]);
		expectedPattern3.get("a[]").add(events[4]);
		expectedPattern3.get("a[]").add(events[5]);
		expectedPattern3.get("a[]").add(events[6]);

		expectedPattern3.put("b", new ArrayList<Event>());
		expectedPattern3.get("b").add(events[7]);

		sharedBuffer.put("a1", events[0], timestamp, null, null, 0, 0, DeweyNumber.fromString("1"));
		sharedBuffer.put("a[]", events[1], timestamp, "a1", events[0], timestamp, 0, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a1", events[2], timestamp, null, null, 0, 0, DeweyNumber.fromString("2"));
		sharedBuffer.put("a[]", events[2], timestamp, "a[]", events[1], timestamp, 1, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a[]", events[2], timestamp, 2, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a1", events[2], timestamp, 0, DeweyNumber.fromString("2.0"));
		sharedBuffer.put("a[]", events[4], timestamp, "a[]", events[3], timestamp, 3, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[4], timestamp, 4, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("a[]", events[5], timestamp, "a[]", events[4], timestamp, 4, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[3], timestamp, 1, DeweyNumber.fromString("2.0.0"));
		sharedBuffer.put("a[]", events[6], timestamp, "a[]", events[5], timestamp, 5, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[7], timestamp, "a[]", events[6], timestamp, 6, DeweyNumber.fromString("1.1.0"));

		Collection<Map<String, List<Event>>> patterns3 = sharedBuffer.extractPatterns("b", events[7], timestamp, 7, DeweyNumber.fromString("1.1.0"));
		sharedBuffer.release("b", events[7], timestamp, 7);
		Collection<Map<String, List<Event>>> patterns4 = sharedBuffer.extractPatterns("b", events[7], timestamp, 7, DeweyNumber.fromString("1.1.0"));

		Collection<Map<String, List<Event>>> patterns1 = sharedBuffer.extractPatterns("b", events[5], timestamp, 2, DeweyNumber.fromString("2.0.0"));
		Collection<Map<String, List<Event>>> patterns2 = sharedBuffer.extractPatterns("b", events[5], timestamp, 5, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.release("b", events[5], timestamp, 2);
		sharedBuffer.release("b", events[5], timestamp, 5);

		assertEquals(1L, patterns3.size());
		assertEquals(0L, patterns4.size());
		assertEquals(1L, patterns1.size());
		assertEquals(1L, patterns2.size());

		assertTrue(sharedBuffer.isEmpty());
		assertTrue(patterns4.isEmpty());
		assertEquals(Collections.singletonList(expectedPattern1), patterns1);
		assertEquals(Collections.singletonList(expectedPattern2), patterns2);
		assertEquals(Collections.singletonList(expectedPattern3), patterns3);
	}

	@Test
	public void testSharedBufferSerialization() throws IOException, ClassNotFoundException {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>();
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
		}

		sharedBuffer.put("a1", events[0], timestamp, null, null, 0, 0, DeweyNumber.fromString("1"));
		sharedBuffer.put("a[]", events[1], timestamp, "a1", events[0], timestamp, 0, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a1", events[2], timestamp, null, null, 0, 0, DeweyNumber.fromString("2"));
		sharedBuffer.put("a[]", events[2], timestamp, "a[]", events[1], timestamp, 1, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a[]", events[2], timestamp, 2, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a1", events[2], timestamp, 0, DeweyNumber.fromString("2.0"));
		sharedBuffer.put("a[]", events[4], timestamp, "a[]", events[3], timestamp, 3, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[4], timestamp, 4, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("a[]", events[5], timestamp, "a[]", events[4], timestamp, 4, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[3], timestamp, 1, DeweyNumber.fromString("2.0.0"));
		sharedBuffer.put("a[]", events[6], timestamp, "a[]", events[5], timestamp, 5, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[7], timestamp, "a[]", events[6], timestamp, 6, DeweyNumber.fromString("1.1.0"));

		SharedBuffer.SharedBufferSerializer serializer = new SharedBuffer.SharedBufferSerializer(
				StringSerializer.INSTANCE, Event.createTypeSerializer());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		serializer.serialize(sharedBuffer, new DataOutputViewStreamWrapper(baos));

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		SharedBuffer<String, Event> copy = serializer.duplicate().deserialize(new DataInputViewStreamWrapper(bais));

		assertEquals(sharedBuffer, copy);
	}

	@Test
	public void testClearingSharedBufferWithMultipleEdgesBetweenEntries() {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>();
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
		}

		sharedBuffer.put("start", events[1], timestamp, DeweyNumber.fromString("1"));
		sharedBuffer.put("branching", events[2], timestamp, "start", events[1], timestamp, 0, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("branching", events[3], timestamp, "start", events[1], timestamp, 0, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("branching", events[3], timestamp, "branching", events[2], timestamp, 1, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("branching", events[4], timestamp, "branching", events[3], timestamp, 2, DeweyNumber.fromString("1.0.0.0"));
		sharedBuffer.put("branching", events[4], timestamp, "branching", events[3], timestamp, 2, DeweyNumber.fromString("1.1.0"));

		//simulate IGNORE (next event can point to events[2])
		sharedBuffer.lock("branching", events[2], timestamp, 1);

		sharedBuffer.release("branching", events[4], timestamp, 3);

		//There should be still events[1] and events[2] in the buffer
		assertFalse(sharedBuffer.isEmpty());
	}

	@Test
	public void testSharedBufferExtractOrder() {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>();
		int numberEvents = 10;
		Event[] events = new Event[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
		}

		Map<String, List<Event>> expectedResult = new LinkedHashMap<>();
		expectedResult.put("a", new ArrayList<>());
		expectedResult.get("a").add(events[1]);
		expectedResult.put("b", new ArrayList<>());
		expectedResult.get("b").add(events[2]);
		expectedResult.put("aa", new ArrayList<>());
		expectedResult.get("aa").add(events[3]);
		expectedResult.put("bb", new ArrayList<>());
		expectedResult.get("bb").add(events[4]);
		expectedResult.put("c", new ArrayList<>());
		expectedResult.get("c").add(events[5]);

		sharedBuffer.put("a", events[1], timestamp, DeweyNumber.fromString("1"));
		sharedBuffer.put("b", events[2], timestamp, "a", events[1], timestamp, 0, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("aa", events[3], timestamp, "b", events[2], timestamp, 1, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("bb", events[4], timestamp, "aa", events[3], timestamp, 2, DeweyNumber.fromString("1.0.0.0"));
		sharedBuffer.put("c", events[5], timestamp, "bb", events[4], timestamp, 3, DeweyNumber.fromString("1.0.0.0.0"));

		Collection<Map<String, List<Event>>> patternsResult = sharedBuffer.extractPatterns("c", events[5], timestamp, 4, DeweyNumber.fromString("1.0.0.0.0"));

		List<String> expectedOrder = new ArrayList<>();
		expectedOrder.add("a");
		expectedOrder.add("b");
		expectedOrder.add("aa");
		expectedOrder.add("bb");
		expectedOrder.add("c");

		List<String> resultOrder = new ArrayList<>();
		for (String key: patternsResult.iterator().next().keySet()){
			resultOrder.add(key);
		}
		assertEquals(expectedOrder, resultOrder);
	}
}
