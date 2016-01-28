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

import com.google.common.collect.LinkedHashMultimap;
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

		LinkedHashMultimap<String, Event> expectedPattern1 = LinkedHashMultimap.create();
		expectedPattern1.put("a1", events[2]);
		expectedPattern1.put("a[]", events[3]);
		expectedPattern1.put("b", events[5]);

		LinkedHashMultimap<String, Event> expectedPattern2 = LinkedHashMultimap.create();
		expectedPattern2.put("a1", events[0]);
		expectedPattern2.put("a[]", events[1]);
		expectedPattern2.put("a[]", events[2]);
		expectedPattern2.put("a[]", events[3]);
		expectedPattern2.put("a[]", events[4]);
		expectedPattern2.put("b", events[5]);

		LinkedHashMultimap<String, Event> expectedPattern3 = LinkedHashMultimap.create();
		expectedPattern3.put("a1", events[0]);
		expectedPattern3.put("a[]", events[1]);
		expectedPattern3.put("a[]", events[2]);
		expectedPattern3.put("a[]", events[3]);
		expectedPattern3.put("a[]", events[4]);
		expectedPattern3.put("a[]", events[5]);
		expectedPattern3.put("a[]", events[6]);
		expectedPattern3.put("b", events[7]);

		sharedBuffer.put("a1", events[0], timestamp, null, null, 0, DeweyNumber.fromString("1"));
		sharedBuffer.put("a[]", events[1], timestamp, "a1", events[0], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a1", events[2], timestamp, null, null, 0, DeweyNumber.fromString("2"));
		sharedBuffer.put("a[]", events[2], timestamp, "a[]", events[1], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a[]", events[2], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a1", events[2], timestamp, DeweyNumber.fromString("2.0"));
		sharedBuffer.put("a[]", events[4], timestamp, "a[]", events[3], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[5], timestamp, "a[]", events[4], timestamp, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[3], timestamp, DeweyNumber.fromString("2.0.0"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[4], timestamp, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("a[]", events[6], timestamp, "a[]", events[5], timestamp, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[7], timestamp, "a[]", events[6], timestamp, DeweyNumber.fromString("1.1.0"));

		Collection<LinkedHashMultimap<String, Event>> patterns3 = sharedBuffer.extractPatterns("b", events[7], timestamp, DeweyNumber.fromString("1.1.0"));
		sharedBuffer.remove("b", events[7], timestamp);
		Collection<LinkedHashMultimap<String, Event>> patterns4 = sharedBuffer.extractPatterns("b", events[7], timestamp, DeweyNumber.fromString("1.1.0"));
		Collection<LinkedHashMultimap<String, Event>> patterns1 = sharedBuffer.extractPatterns("b", events[5], timestamp, DeweyNumber.fromString("2.0.0"));
		Collection<LinkedHashMultimap<String, Event>> patterns2 = sharedBuffer.extractPatterns("b", events[5], timestamp, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.remove("b", events[5], timestamp);

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

		sharedBuffer.put("a1", events[0], timestamp, null, null, 0, DeweyNumber.fromString("1"));
		sharedBuffer.put("a[]", events[1], timestamp, "a1", events[0], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a1", events[2], timestamp, null, null, 0, DeweyNumber.fromString("2"));
		sharedBuffer.put("a[]", events[2], timestamp, "a[]", events[1], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a[]", events[2], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[3], timestamp, "a1", events[2], timestamp, DeweyNumber.fromString("2.0"));
		sharedBuffer.put("a[]", events[4], timestamp, "a[]", events[3], timestamp, DeweyNumber.fromString("1.0"));
		sharedBuffer.put("a[]", events[5], timestamp, "a[]", events[4], timestamp, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[3], timestamp, DeweyNumber.fromString("2.0.0"));
		sharedBuffer.put("b", events[5], timestamp, "a[]", events[4], timestamp, DeweyNumber.fromString("1.0.0"));
		sharedBuffer.put("a[]", events[6], timestamp, "a[]", events[5], timestamp, DeweyNumber.fromString("1.1"));
		sharedBuffer.put("b", events[7], timestamp, "a[]", events[6], timestamp, DeweyNumber.fromString("1.1.0"));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(sharedBuffer);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);

		SharedBuffer<String, Event> copy = (SharedBuffer<String, Event>)ois.readObject();

		assertEquals(sharedBuffer, copy);
	}
}
