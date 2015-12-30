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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class EventSerializerTest {

	@Test
	public void testSerializeDeserializeEvent() {
		try {
			AbstractEvent[] events = {
					EndOfPartitionEvent.INSTANCE,
					EndOfSuperstepEvent.INSTANCE,
					new CheckpointBarrier(1678L, 4623784L),
					new TestTaskEvent(Math.random(), 12361231273L)
			};
			
			for (AbstractEvent evt : events) {
				ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(evt);
				assertTrue(serializedEvent.hasRemaining());

				AbstractEvent deserialized = 
						EventSerializer.fromSerializedEvent(serializedEvent, getClass().getClassLoader());
				assertNotNull(deserialized);
				assertEquals(evt, deserialized);
			}
			
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
