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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link EventSerializer}.
 */
public class EventSerializerTest {

	private final AbstractEvent[] events = {
		EndOfPartitionEvent.INSTANCE,
		EndOfSuperstepEvent.INSTANCE,
		new CheckpointBarrier(1678L, 4623784L, CheckpointOptions.forCheckpointWithDefaultLocation()),
		new TestTaskEvent(Math.random(), 12361231273L),
		new CancelCheckpointMarker(287087987329842L)
	};

	@Test
	public void testSerializeDeserializeEvent() throws Exception {
		for (AbstractEvent evt : events) {
			ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(evt);
			assertTrue(serializedEvent.hasRemaining());

			AbstractEvent deserialized =
					EventSerializer.fromSerializedEvent(serializedEvent, getClass().getClassLoader());
			assertNotNull(deserialized);
			assertEquals(evt, deserialized);
		}
	}

	@Test
	public void testToBufferConsumer() throws IOException {
		for (AbstractEvent evt : events) {
			BufferConsumer bufferConsumer = EventSerializer.toBufferConsumer(evt, false);

			assertFalse(bufferConsumer.isBuffer());
			assertTrue(bufferConsumer.isFinished());
			assertTrue(bufferConsumer.isDataAvailable());
			assertFalse(bufferConsumer.isRecycled());

			if (evt instanceof CheckpointBarrier) {
				assertTrue(bufferConsumer.build().getDataType().isBlockingUpstream());
			} else {
				assertEquals(Buffer.DataType.EVENT_BUFFER, bufferConsumer.build().getDataType());
			}
		}
	}

	@Test
	public void testToBuffer() throws IOException {
		for (AbstractEvent evt : events) {
			Buffer buffer = EventSerializer.toBuffer(evt, false);

			assertFalse(buffer.isBuffer());
			assertTrue(buffer.readableBytes() > 0);
			assertFalse(buffer.isRecycled());

			if (evt instanceof CheckpointBarrier) {
				assertTrue(buffer.getDataType().isBlockingUpstream());
			} else {
				assertEquals(Buffer.DataType.EVENT_BUFFER, buffer.getDataType());
			}
		}
	}

	/**
	 * Tests {@link EventSerializer#isEvent(Buffer, Class)}
	 * whether it peaks into the buffer only, i.e. after the call, the buffer
	 * is still de-serializable.
	 */
	@Test
	public void testIsEventPeakOnly() throws Exception {
		final Buffer serializedEvent = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
		try {
			final ClassLoader cl = getClass().getClassLoader();
			assertTrue(
				EventSerializer.isEvent(serializedEvent, EndOfPartitionEvent.class));
			EndOfPartitionEvent event = (EndOfPartitionEvent) EventSerializer
				.fromBuffer(serializedEvent, cl);
			assertEquals(EndOfPartitionEvent.INSTANCE, event);
		} finally {
			serializedEvent.recycleBuffer();
		}
	}

	/**
	 * Tests {@link EventSerializer#isEvent(Buffer, Class)} returns
	 * the correct answer for various encoded event buffers.
	 */
	@Test
	public void testIsEvent() throws Exception {
		Class[] expectedClasses = Arrays.stream(events)
			.map(AbstractEvent::getClass)
			.toArray(Class[]::new);

		for (AbstractEvent evt : events) {
			for (Class<?> expectedClass: expectedClasses) {
				if (expectedClass.equals(TestTaskEvent.class)) {
					try {
						checkIsEvent(evt, expectedClass);
						fail("This should fail");
					}
					catch (UnsupportedOperationException ex) {
						// expected
					}
				}
				else if (evt.getClass().equals(expectedClass)) {
					assertTrue(checkIsEvent(evt, expectedClass));
				} else {
					assertFalse(checkIsEvent(evt, expectedClass));
				}
			}
		}
	}

	/**
	 * Returns the result of
	 * {@link EventSerializer#isEvent(Buffer, Class)} on a buffer
	 * that encodes the given <tt>event</tt>.
	 *
	 * @param event the event to encode
	 * @param eventClass the event class to check against
	 *
	 * @return whether {@link EventSerializer#isEvent(Buffer, Class)}
	 * 		thinks the encoded buffer matches the class
	 */
	private boolean checkIsEvent(
			AbstractEvent event,
			Class<?> eventClass) throws IOException {

		final boolean unprioritizedIsEvent = isEvent(event, eventClass, false);
		final boolean prioritizedIsEvent = isEvent(event, eventClass, true);
		assertEquals(unprioritizedIsEvent, prioritizedIsEvent);
		return unprioritizedIsEvent;
	}

	private boolean isEvent(AbstractEvent event, Class<?> eventClass, boolean hasPriority) throws IOException {
		final Buffer serializedEvent = EventSerializer.toBuffer(event, hasPriority);
		try {
			return EventSerializer.isEvent(serializedEvent, eventClass);
		} finally {
			serializedEvent.recycleBuffer();
		}
	}
}
