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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.junit.Test;

public class EventSerializerTest {

	@Test
	public void testCheckpointBarrierSerialization() throws Exception {
		long id = Integer.MAX_VALUE + 123123L;
		long timestamp = Integer.MAX_VALUE + 1228L;

		CheckpointOptions checkpoint = CheckpointOptions.forFullCheckpoint();
		testCheckpointBarrierSerialization(id, timestamp, checkpoint);

		CheckpointOptions savepoint = CheckpointOptions.forSavepoint("1289031838919123");
		testCheckpointBarrierSerialization(id, timestamp, savepoint);
	}

	private void testCheckpointBarrierSerialization(long id, long timestamp, CheckpointOptions options) throws IOException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, options);
		ByteBuffer serialized = EventSerializer.toSerializedEvent(barrier);
		CheckpointBarrier deserialized = (CheckpointBarrier) EventSerializer.fromSerializedEvent(serialized, cl);
		assertFalse(serialized.hasRemaining());

		assertEquals(id, deserialized.getId());
		assertEquals(timestamp, deserialized.getTimestamp());
		assertEquals(options.getCheckpointType(), deserialized.getCheckpointOptions().getCheckpointType());
		assertEquals(options.getTargetLocation(), deserialized.getCheckpointOptions().getTargetLocation());
	}

	@Test
	public void testSerializeDeserializeEvent() throws Exception {
		AbstractEvent[] events = {
				EndOfPartitionEvent.INSTANCE,
				EndOfSuperstepEvent.INSTANCE,
				new CheckpointBarrier(1678L, 4623784L, CheckpointOptions.forFullCheckpoint()),
				new TestTaskEvent(Math.random(), 12361231273L),
				new CancelCheckpointMarker(287087987329842L)
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

	/**
	 * Tests {@link EventSerializer#isEvent(Buffer, Class, ClassLoader)}
	 * whether it peaks into the buffer only, i.e. after the call, the buffer
	 * is still de-serializable.
	 *
	 * @throws Exception
	 */
	@Test
	public void testIsEventPeakOnly() throws Exception {
		final Buffer serializedEvent =
			EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
		try {
			final ClassLoader cl = getClass().getClassLoader();
			assertTrue(
				EventSerializer
					.isEvent(serializedEvent, EndOfPartitionEvent.class, cl));
			EndOfPartitionEvent event = (EndOfPartitionEvent) EventSerializer
				.fromBuffer(serializedEvent, cl);
			assertEquals(EndOfPartitionEvent.INSTANCE, event);
		} finally {
			serializedEvent.recycle();
		}
	}

	/**
	 * Tests {@link EventSerializer#isEvent(Buffer, Class, ClassLoader)} returns
	 * the correct answer for various encoded event buffers.
	 *
	 * @throws Exception
	 */
	@Test
	public void testIsEvent() throws Exception {
		AbstractEvent[] events = {
			EndOfPartitionEvent.INSTANCE,
			EndOfSuperstepEvent.INSTANCE,
			new CheckpointBarrier(1678L, 4623784L, CheckpointOptions.forFullCheckpoint()),
			new TestTaskEvent(Math.random(), 12361231273L),
			new CancelCheckpointMarker(287087987329842L)
		};

		for (AbstractEvent evt : events) {
			for (AbstractEvent evt2 : events) {
				if (evt == evt2) {
					assertTrue(checkIsEvent(evt, evt2.getClass()));
				} else {
					assertFalse(checkIsEvent(evt, evt2.getClass()));
				}
			}
		}
	}

	/**
	 * Returns the result of
	 * {@link EventSerializer#isEvent(Buffer, Class, ClassLoader)} on a buffer
	 * that encodes the given <tt>event</tt>.
	 *
	 * @param event the event to encode
	 * @param eventClass the event class to check against
	 *
	 * @return whether {@link EventSerializer#isEvent(ByteBuffer, Class, ClassLoader)}
	 * 		thinks the encoded buffer matches the class
	 * @throws IOException
	 */
	private boolean checkIsEvent(
			AbstractEvent event, 
			Class<? extends AbstractEvent> eventClass) throws IOException {

		final Buffer serializedEvent = EventSerializer.toBuffer(event);
		try {
			final ClassLoader cl = getClass().getClassLoader();
			return EventSerializer.isEvent(serializedEvent, eventClass, cl);
		} finally {
			serializedEvent.recycle();
		}
	}
}
