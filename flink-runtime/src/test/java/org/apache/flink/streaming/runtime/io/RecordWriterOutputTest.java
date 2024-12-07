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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions.AlignmentType;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RecordWriterOutput}. */
class RecordWriterOutputTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDisableUnalignedCheckpoint(boolean supportsUnalignedCheckpoints) throws IOException {
        Queue<Tuple2<AbstractEvent, Boolean>> queue = new LinkedList<>();

        RecordWriter<SerializationDelegate<StreamRecord<Long>>> task1 =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<Long>>>()
                        .build(
                                new MockResultPartitionWriter() {
                                    @Override
                                    public void broadcastEvent(
                                            AbstractEvent event, boolean isPriorityEvent) {
                                        queue.add(Tuple2.of(event, isPriorityEvent));
                                    }
                                });

        RecordWriterOutput<Long> writerOutput =
                new RecordWriterOutput<>(
                        task1, LongSerializer.INSTANCE, null, supportsUnalignedCheckpoints);

        // Test unalignedBarrier
        CheckpointBarrier unalignedBarrier =
                new CheckpointBarrier(
                        0,
                        1L,
                        CheckpointOptions.unaligned(
                                CheckpointType.CHECKPOINT,
                                CheckpointStorageLocationReference.getDefault()));

        writerOutput.broadcastEvent(unalignedBarrier, true);
        assertAlignmentTypeAndIsPriorityEvent(
                queue.poll(),
                supportsUnalignedCheckpoints
                        ? AlignmentType.UNALIGNED
                        : AlignmentType.FORCED_ALIGNED,
                supportsUnalignedCheckpoints);

        // Test alignedTimeoutBarrier
        CheckpointBarrier alignedTimeoutBarrier =
                new CheckpointBarrier(
                        0,
                        1L,
                        CheckpointOptions.alignedWithTimeout(
                                CheckpointType.CHECKPOINT,
                                CheckpointStorageLocationReference.getDefault(),
                                1000));

        writerOutput.broadcastEvent(alignedTimeoutBarrier, false);
        assertAlignmentTypeAndIsPriorityEvent(
                queue.poll(),
                supportsUnalignedCheckpoints ? AlignmentType.ALIGNED : AlignmentType.FORCED_ALIGNED,
                false);
    }

    private void assertAlignmentTypeAndIsPriorityEvent(
            Tuple2<AbstractEvent, Boolean> unalignedResult,
            AlignmentType expectedAlignmentType,
            boolean isPriorityEvent) {
        assertThat(unalignedResult).isNotNull();

        assertThat(unalignedResult.f0).isInstanceOf(CheckpointBarrier.class);
        assertThat(((CheckpointBarrier) unalignedResult.f0).getCheckpointOptions().getAlignment())
                .isSameAs(expectedAlignmentType);
        assertThat(unalignedResult.f1).isEqualTo(isPriorityEvent);
    }
}
