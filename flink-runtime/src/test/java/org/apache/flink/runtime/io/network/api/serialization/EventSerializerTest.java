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

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link EventSerializer}. */
class EventSerializerTest {

    private final AbstractEvent[] events = {
        EndOfPartitionEvent.INSTANCE,
        EndOfSuperstepEvent.INSTANCE,
        new EndOfData(StopMode.DRAIN),
        new EndOfData(StopMode.NO_DRAIN),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        CheckpointType.FULL_CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.suspend(SavepointFormatType.CANONICAL),
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.terminate(SavepointFormatType.CANONICAL),
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.savepoint(SavepointFormatType.NATIVE),
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.suspend(SavepointFormatType.NATIVE),
                        CheckpointStorageLocationReference.getDefault())),
        new CheckpointBarrier(
                1678L,
                4623784L,
                new CheckpointOptions(
                        SavepointType.terminate(SavepointFormatType.NATIVE),
                        CheckpointStorageLocationReference.getDefault())),
        new TestTaskEvent(Math.random(), 12361231273L),
        new CancelCheckpointMarker(287087987329842L),
        new EventAnnouncement(
                new CheckpointBarrier(
                        42L,
                        1337L,
                        CheckpointOptions.alignedWithTimeout(
                                CheckpointType.CHECKPOINT,
                                CheckpointStorageLocationReference.getDefault(),
                                10)),
                44),
        new SubtaskConnectionDescriptor(23, 42),
        EndOfSegmentEvent.INSTANCE,
        new RecoveryMetadata(3)
    };

    @Test
    void testSerializeDeserializeEvent() throws Exception {
        for (AbstractEvent evt : events) {
            ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(evt);
            assertThat(serializedEvent.hasRemaining()).isTrue();

            AbstractEvent deserialized =
                    EventSerializer.fromSerializedEvent(
                            serializedEvent, getClass().getClassLoader());
            assertThat(deserialized).isNotNull().isEqualTo(evt);
        }
    }

    @Test
    void testToBufferConsumer() throws IOException {
        for (AbstractEvent evt : events) {
            BufferConsumer bufferConsumer = EventSerializer.toBufferConsumer(evt, false);

            assertThat(bufferConsumer.isBuffer()).isFalse();
            assertThat(bufferConsumer.isFinished()).isTrue();
            assertThat(bufferConsumer.isDataAvailable()).isTrue();
            assertThat(bufferConsumer.isRecycled()).isFalse();

            if (evt instanceof CheckpointBarrier) {
                assertThat(bufferConsumer.build().getDataType().isBlockingUpstream()).isTrue();
            } else if (evt instanceof EndOfData) {
                assertThat(bufferConsumer.build().getDataType())
                        .isEqualTo(Buffer.DataType.END_OF_DATA);
            } else if (evt instanceof EndOfPartitionEvent) {
                assertThat(bufferConsumer.build().getDataType())
                        .isEqualTo(Buffer.DataType.END_OF_PARTITION);
            } else {
                assertThat(bufferConsumer.build().getDataType())
                        .isEqualTo(Buffer.DataType.EVENT_BUFFER);
            }
        }
    }

    @Test
    void testToBuffer() throws IOException {
        for (AbstractEvent evt : events) {
            Buffer buffer = EventSerializer.toBuffer(evt, false);

            assertThat(buffer.isBuffer()).isFalse();
            assertThat(buffer.readableBytes()).isGreaterThan(0);
            assertThat(buffer.isRecycled()).isFalse();

            if (evt instanceof CheckpointBarrier) {
                assertThat(buffer.getDataType().isBlockingUpstream()).isTrue();
            } else if (evt instanceof EndOfData) {
                assertThat(buffer.getDataType()).isEqualTo(Buffer.DataType.END_OF_DATA);
            } else if (evt instanceof EndOfPartitionEvent) {
                assertThat(buffer.getDataType()).isEqualTo(Buffer.DataType.END_OF_PARTITION);
            } else {
                assertThat(buffer.getDataType()).isEqualTo(Buffer.DataType.EVENT_BUFFER);
            }
        }
    }
}
