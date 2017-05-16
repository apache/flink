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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.flink.cep.Event;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SharedBufferTest extends TestLogger {

	@Test
	public void testSharedBuffer() {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>(Event.createTypeSerializer());
		int numberEvents = 8;
		Event[] events = new Event[numberEvents];
		final long timestamp = 1L;

		for (int i = 0; i < numberEvents; i++) {
			events[i] = new Event(i + 1, "e" + (i + 1), i);
		}

		ListMultimap<String, Event> expectedPattern1 = ArrayListMultimap.create();
		expectedPattern1.put("a1", events[2]);
		expectedPattern1.put("a[]", events[3]);
		expectedPattern1.put("b", events[5]);

		ListMultimap<String, Event> expectedPattern2 = ArrayListMultimap.create();
		expectedPattern2.put("a1", events[0]);
		expectedPattern2.put("a[]", events[1]);
		expectedPattern2.put("a[]", events[2]);
		expectedPattern2.put("a[]", events[3]);
		expectedPattern2.put("a[]", events[4]);
		expectedPattern2.put("b", events[5]);

		ListMultimap<String, Event> expectedPattern3 = ArrayListMultimap.create();
		expectedPattern3.put("a1", events[0]);
		expectedPattern3.put("a[]", events[1]);
		expectedPattern3.put("a[]", events[2]);
		expectedPattern3.put("a[]", events[3]);
		expectedPattern3.put("a[]", events[4]);
		expectedPattern3.put("a[]", events[5]);
		expectedPattern3.put("a[]", events[6]);
		expectedPattern3.put("b", events[7]);

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

		Collection<ListMultimap<String, Event>> patterns3 = sharedBuffer.extractPatterns("b", events[7], timestamp, 7, DeweyNumber.fromString("1.1.0"));
		sharedBuffer.release("b", events[7], timestamp, 7);
		Collection<ListMultimap<String, Event>> patterns4 = sharedBuffer.extractPatterns("b", events[7], timestamp, 7, DeweyNumber.fromString("1.1.0"));

		Collection<ListMultimap<String, Event>> patterns1 = sharedBuffer.extractPatterns("b", events[5], timestamp, 2, DeweyNumber.fromString("2.0.0"));
		Collection<ListMultimap<String, Event>> patterns2 = sharedBuffer.extractPatterns("b", events[5], timestamp, 5, DeweyNumber.fromString("1.0.0"));
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
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>(Event.createTypeSerializer());
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

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(sharedBuffer);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);

		SharedBuffer<String, Event> copy = (SharedBuffer<String, Event>)ois.readObject();

		assertEquals(sharedBuffer, copy);
	}

	@Test
	public void testClearingSharedBufferWithMultipleEdgesBetweenEntries() {
		SharedBuffer<String, Event> sharedBuffer = new SharedBuffer<>(Event.createTypeSerializer());
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
}
