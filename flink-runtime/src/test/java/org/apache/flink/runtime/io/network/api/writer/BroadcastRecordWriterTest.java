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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.io.network.api.writer.RecordWriterTest.createResultPartition;
import static org.apache.flink.runtime.io.network.api.writer.RecordWriterTest.verifyDeserializationResults;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link BroadcastRecordWriter}. */
class BroadcastRecordWriterTest {

    /**
     * Tests the number of requested buffers and results are correct in the case of switching modes
     * between {@link BroadcastRecordWriter#broadcastEmit(IOReadableWritable)} and {@link
     * BroadcastRecordWriter#randomEmit(IOReadableWritable)}.
     */
    @Test
    void testBroadcastMixedRandomEmitRecord(@TempDir Path tempPath) throws Exception {
        final int numberOfChannels = 8;
        final int numberOfRecords = 8;
        final int bufferSize = 32;

        final ResultPartition partition = createResultPartition(bufferSize, numberOfChannels);
        final BroadcastRecordWriter<SerializationTestType> writer =
                new BroadcastRecordWriter<>(partition, -1, "test");
        final RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempPath.toString()});

        // generate the configured number of int values as global record set
        final Iterable<SerializationTestType> records =
                Util.randomRecords(numberOfRecords, SerializationTestTypeFactory.INT);
        // restore the corresponding record set for every input channel
        final Map<Integer, ArrayDeque<SerializationTestType>> serializedRecords = new HashMap<>();
        for (int i = 0; i < numberOfChannels; i++) {
            serializedRecords.put(i, new ArrayDeque<>());
        }

        // every record in global set would both emit into one random channel and broadcast to all
        // the channels
        int index = 0;
        for (SerializationTestType record : records) {
            int randomChannel = index++ % numberOfChannels;
            writer.emit(record, randomChannel);
            serializedRecords.get(randomChannel).add(record);

            writer.broadcastEmit(record);
            for (int i = 0; i < numberOfChannels; i++) {
                serializedRecords.get(i).add(record);
            }
        }

        final int numberOfCreatedBuffers =
                partition.getBufferPool().bestEffortGetNumOfUsedBuffers();
        // verify the expected number of requested buffers, and it would always request a new buffer
        // while random emitting
        assertThat(2 * numberOfRecords).isEqualTo(numberOfCreatedBuffers);

        for (int i = 0; i < numberOfChannels; i++) {
            // every channel would queue the number of above crated buffers
            assertThat(partition.getNumberOfQueuedBuffers(i)).isEqualTo(numberOfRecords + 1);

            final int excessRandomRecords = i < numberOfRecords % numberOfChannels ? 1 : 0;
            final int numberOfRandomRecords =
                    numberOfRecords / numberOfChannels + excessRandomRecords;
            final int numberOfTotalRecords = numberOfRecords + numberOfRandomRecords;
            // verify the data correctness in every channel queue
            verifyDeserializationResults(
                    partition.createSubpartitionView(i, new NoOpBufferAvailablityListener()),
                    deserializer,
                    serializedRecords.get(i),
                    numberOfRecords + 1,
                    numberOfTotalRecords);
        }
    }

    /**
     * FLINK-17780: Tests that a shared buffer(or memory segment) of a buffer builder is only freed
     * when all consumers are closed.
     */
    @Test
    void testRandomEmitAndBufferRecycling() throws Exception {
        int recordSize = 8;
        int numberOfChannels = 2;

        ResultPartition partition = createResultPartition(2 * recordSize, numberOfChannels);
        BufferPool bufferPool = partition.getBufferPool();
        BroadcastRecordWriter<SerializationTestType> writer =
                new BroadcastRecordWriter<>(partition, -1, "test");

        // force materialization of both buffers for easier availability tests
        List<Buffer> buffers =
                Arrays.asList(bufferPool.requestBuffer(), bufferPool.requestBuffer());
        buffers.forEach(Buffer::recycleBuffer);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(3);

        // fill first buffer
        writer.broadcastEmit(new IntType(1));
        writer.broadcastEmit(new IntType(2));
        assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // simulate consumption of first buffer consumer; this should not free buffers
        assertThat(partition.getNumberOfQueuedBuffers(0)).isOne();
        ResultSubpartitionView view0 =
                partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
        closeConsumer(view0, 2 * recordSize);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // use second buffer
        writer.emit(new IntType(3), 0);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isOne();
        // fully free first buffer
        assertThat(partition.getNumberOfQueuedBuffers(1)).isOne();
        ResultSubpartitionView view1 =
                partition.createSubpartitionView(1, new NoOpBufferAvailablityListener());
        closeConsumer(view1, 2 * recordSize);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);
    }

    public void closeConsumer(ResultSubpartitionView view, int expectedSize) throws IOException {
        Buffer buffer = view.getNextBuffer().buffer();
        assertThat(buffer.getSize()).isEqualTo(expectedSize);
        buffer.recycleBuffer();
    }
}
