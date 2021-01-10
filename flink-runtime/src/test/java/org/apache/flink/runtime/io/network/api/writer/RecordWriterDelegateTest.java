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

import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link SingleRecordWriter} and {@link MultipleRecordWriters}. */
public class RecordWriterDelegateTest extends TestLogger {

    private static final int recordSize = 8;

    private static final int numberOfBuffers = 10;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool globalPool;

    @Before
    public void setup() {
        assertEquals("Illegal memory segment size,", 0, memorySegmentSize % recordSize);
        globalPool = new NetworkBufferPool(numberOfBuffers, memorySegmentSize);
    }

    @After
    public void teardown() {
        globalPool.destroyAllBufferPools();
        globalPool.destroy();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSingleRecordWriterAvailability() throws Exception {
        final RecordWriter recordWriter = createRecordWriter(globalPool);
        final RecordWriterDelegate writerDelegate = new SingleRecordWriter(recordWriter);

        assertEquals(recordWriter, writerDelegate.getRecordWriter(0));
        verifyAvailability(writerDelegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleRecordWritersAvailability() throws Exception {
        // setup
        final int numRecordWriters = 2;
        final List<RecordWriter> recordWriters = new ArrayList<>(numRecordWriters);

        for (int i = 0; i < numRecordWriters; i++) {
            recordWriters.add(createRecordWriter(globalPool));
        }

        RecordWriterDelegate writerDelegate = new MultipleRecordWriters(recordWriters);
        for (int i = 0; i < numRecordWriters; i++) {
            assertEquals(recordWriters.get(i), writerDelegate.getRecordWriter(i));
        }

        verifyAvailability(writerDelegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSingleRecordWriterBroadcastEvent() throws Exception {
        // setup
        final ResultPartition partition =
                RecordWriterTest.createResultPartition(memorySegmentSize, 2);
        final RecordWriter recordWriter = new RecordWriterBuilder<>().build(partition);
        final RecordWriterDelegate writerDelegate = new SingleRecordWriter(recordWriter);

        verifyBroadcastEvent(writerDelegate, Collections.singletonList(partition));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleRecordWritersBroadcastEvent() throws Exception {
        // setup
        final int numRecordWriters = 2;
        final List<RecordWriter> recordWriters = new ArrayList<>(numRecordWriters);
        final List<ResultPartition> partitions = new ArrayList<>(numRecordWriters);

        for (int i = 0; i < numRecordWriters; i++) {
            final ResultPartition partition =
                    RecordWriterTest.createResultPartition(memorySegmentSize, 2);
            partitions.add(partition);
            recordWriters.add(new RecordWriterBuilder<>().build(partition));
        }
        final RecordWriterDelegate writerDelegate = new MultipleRecordWriters(recordWriters);

        verifyBroadcastEvent(writerDelegate, partitions);
    }

    private RecordWriter createRecordWriter(NetworkBufferPool globalPool) throws Exception {
        final BufferPool localPool = globalPool.createBufferPool(1, 1, 1, Integer.MAX_VALUE);
        final ResultPartitionWriter partition =
                new ResultPartitionBuilder().setBufferPoolFactory(() -> localPool).build();
        partition.setup();

        return new RecordWriterBuilder().build(partition);
    }

    private void verifyAvailability(RecordWriterDelegate writerDelegate) throws Exception {
        // writer is available at the beginning
        assertTrue(writerDelegate.isAvailable());
        assertTrue(writerDelegate.getAvailableFuture().isDone());

        // request one buffer from the local pool to make it unavailable
        RecordWriter recordWriter = writerDelegate.getRecordWriter(0);
        for (int i = 0; i < memorySegmentSize / recordSize; ++i) {
            recordWriter.emit(new IntValue(i));
        }
        assertFalse(writerDelegate.isAvailable());
        CompletableFuture future = writerDelegate.getAvailableFuture();
        assertFalse(future.isDone());

        // recycle the buffer to make the local pool available again
        ResultSubpartitionView readView =
                recordWriter
                        .getTargetPartition()
                        .createSubpartitionView(0, new NoOpBufferAvailablityListener());
        Buffer buffer = readView.getNextBuffer().buffer();

        buffer.recycleBuffer();
        assertTrue(future.isDone());
        assertTrue(writerDelegate.isAvailable());
        assertTrue(writerDelegate.getAvailableFuture().isDone());
    }

    private void verifyBroadcastEvent(
            RecordWriterDelegate writerDelegate, List<ResultPartition> partitions)
            throws Exception {

        final CancelCheckpointMarker message = new CancelCheckpointMarker(1);
        writerDelegate.broadcastEvent(message);

        // verify the added messages in all the queues
        for (ResultPartition partition : partitions) {
            for (int i = 0; i < partition.getNumberOfSubpartitions(); i++) {
                assertEquals(1, partition.getNumberOfQueuedBuffers(i));

                ResultSubpartitionView view =
                        partition.createSubpartitionView(i, new NoOpBufferAvailablityListener());
                BufferOrEvent boe = RecordWriterTest.parseBuffer(view.getNextBuffer().buffer(), i);
                assertTrue(boe.isEvent());
                assertEquals(message, boe.getEvent());
            }
        }
    }
}
