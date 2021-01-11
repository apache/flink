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
import org.apache.flink.runtime.io.network.api.*;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for the {@link EventSerializer}. */
public class EventSerializerTest {

    private final AbstractEvent[] events = {
        EndOfPartitionEvent.INSTANCE,
        EndOfSuperstepEvent.INSTANCE,
        new CheckpointBarrier(
                1678L, 4623784L, CheckpointOptions.forCheckpointWithDefaultLocation()),
        new TestTaskEvent(Math.random(), 12361231273L),
        new CancelCheckpointMarker(287087987329842L),
        new EventAnnouncement(
                new CheckpointBarrier(
                        42L,
                        1337L,
                        CheckpointOptions.alignedWithTimeout(
                                CheckpointStorageLocationReference.getDefault(), 10)),
                44)
    };

    @Test
    public void testSerializeDeserializeEvent() throws Exception {
        for (AbstractEvent evt : events) {
            ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(evt);
            assertTrue(serializedEvent.hasRemaining());

            AbstractEvent deserialized =
                    EventSerializer.fromSerializedEvent(
                            serializedEvent, getClass().getClassLoader());
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
}
