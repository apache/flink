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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PipelinedSubpartition}.
 *
 * @see PipelinedSubpartitionWithReadViewTest
 */
public class PipelinedSubpartitionTest extends SubpartitionTestBase {

    /** Executor service for concurrent produce/consume tests. */
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    @AfterClass
    public static void shutdownExecutorService() throws Exception {
        executorService.shutdownNow();
    }

    @Override
    PipelinedSubpartition createSubpartition() throws Exception {
        return createPipelinedSubpartition();
    }

    @Override
    ResultSubpartition createFailingWritesSubpartition() throws Exception {
        // the tests relating to this are currently not supported by the PipelinedSubpartition
        Assume.assumeTrue(false);
        return null;
    }

    @Test
    public void testIllegalReadViewRequest() throws Exception {
        final PipelinedSubpartition subpartition = createSubpartition();

        // Successful request
        assertNotNull(subpartition.createReadView(new NoOpBufferAvailablityListener()));

        try {
            subpartition.createReadView(new NoOpBufferAvailablityListener());

            fail("Did not throw expected exception after duplicate notifyNonEmpty view request.");
        } catch (IllegalStateException expected) {
        }
    }

    /** Verifies that the isReleased() check of the view checks the parent subpartition. */
    @Test
    public void testIsReleasedChecksParent() {
        PipelinedSubpartition subpartition = mock(PipelinedSubpartition.class);

        PipelinedSubpartitionView reader =
                new PipelinedSubpartitionView(subpartition, mock(BufferAvailabilityListener.class));

        assertFalse(reader.isReleased());
        verify(subpartition, times(1)).isReleased();

        when(subpartition.isReleased()).thenReturn(true);
        assertTrue(reader.isReleased());
        verify(subpartition, times(2)).isReleased();
    }

    @Test
    public void testConcurrentFastProduceAndFastConsume() throws Exception {
        testProduceConsume(false, false);
    }

    @Test
    public void testConcurrentFastProduceAndSlowConsume() throws Exception {
        testProduceConsume(false, true);
    }

    @Test
    public void testConcurrentSlowProduceAndFastConsume() throws Exception {
        testProduceConsume(true, false);
    }

    @Test
    public void testConcurrentSlowProduceAndSlowConsume() throws Exception {
        testProduceConsume(true, true);
    }

    private void testProduceConsume(boolean isSlowProducer, boolean isSlowConsumer)
            throws Exception {
        // Config
        final int producerNumberOfBuffersToProduce = 128;
        final int bufferSize = 32 * 1024;

        // Producer behaviour
        final TestProducerSource producerSource =
                new TestProducerSource() {

                    private int numberOfBuffers;

                    @Override
                    public BufferAndChannel getNextBuffer() throws Exception {
                        if (numberOfBuffers == producerNumberOfBuffersToProduce) {
                            return null;
                        }

                        MemorySegment segment =
                                MemorySegmentFactory.allocateUnpooledSegment(bufferSize);

                        int next = numberOfBuffers * (bufferSize / Integer.BYTES);

                        for (int i = 0; i < bufferSize; i += 4) {
                            segment.putInt(i, next);
                            next++;
                        }

                        numberOfBuffers++;

                        return new BufferAndChannel(segment.getArray(), 0);
                    }
                };

        // Consumer behaviour
        final TestConsumerCallback consumerCallback =
                new TestConsumerCallback() {

                    private int numberOfBuffers;

                    @Override
                    public void onBuffer(Buffer buffer) {
                        final MemorySegment segment = buffer.getMemorySegment();
                        assertEquals(segment.size(), buffer.getSize());

                        int expected = numberOfBuffers * (segment.size() / 4);

                        for (int i = 0; i < segment.size(); i += 4) {
                            assertEquals(expected, segment.getInt(i));

                            expected++;
                        }

                        numberOfBuffers++;

                        buffer.recycleBuffer();
                    }

                    @Override
                    public void onEvent(AbstractEvent event) {
                        // Nothing to do in this test
                    }
                };

        final PipelinedSubpartition subpartition = createSubpartition();

        TestSubpartitionProducer producer =
                new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource);
        TestSubpartitionConsumer consumer =
                new TestSubpartitionConsumer(isSlowConsumer, consumerCallback);
        final PipelinedSubpartitionView view = subpartition.createReadView(consumer);
        consumer.setSubpartitionView(view);

        CompletableFuture<Boolean> producerResult =
                CompletableFuture.supplyAsync(
                        CheckedSupplier.unchecked(producer::call), executorService);
        CompletableFuture<Boolean> consumerResult =
                CompletableFuture.supplyAsync(
                        CheckedSupplier.unchecked(consumer::call), executorService);

        FutureUtils.waitForAll(Arrays.asList(producerResult, consumerResult))
                .get(60_000L, TimeUnit.MILLISECONDS);
    }

    /** Tests cleanup of {@link PipelinedSubpartition#release()} with no read view attached. */
    @Test
    public void testCleanupReleasedPartitionNoView() throws Exception {
        testCleanupReleasedPartition(false);
    }

    /** Tests cleanup of {@link PipelinedSubpartition#release()} with a read view attached. */
    @Test
    public void testCleanupReleasedPartitionWithView() throws Exception {
        testCleanupReleasedPartition(true);
    }

    /**
     * Tests cleanup of {@link PipelinedSubpartition#release()}.
     *
     * @param createView whether the partition should have a view attached to it (<tt>true</tt>) or
     *     not (<tt>false</tt>)
     */
    private void testCleanupReleasedPartition(boolean createView) throws Exception {
        PipelinedSubpartition partition = createSubpartition();

        BufferConsumer buffer1 = createFilledFinishedBufferConsumer(4096);
        BufferConsumer buffer2 = createFilledFinishedBufferConsumer(4096);
        boolean buffer1Recycled;
        boolean buffer2Recycled;
        try {
            partition.add(buffer1);
            partition.add(buffer2);
            // create the read view first
            ResultSubpartitionView view = null;
            if (createView) {
                view = partition.createReadView(new NoOpBufferAvailablityListener());
            }

            partition.release();

            assertTrue(partition.isReleased());
            if (createView) {
                assertTrue(view.isReleased());
            }
            assertTrue(buffer1.isRecycled());
        } finally {
            buffer1Recycled = buffer1.isRecycled();
            if (!buffer1Recycled) {
                buffer1.close();
            }
            buffer2Recycled = buffer2.isRecycled();
            if (!buffer2Recycled) {
                buffer2.close();
            }
        }
        if (!buffer1Recycled) {
            Assert.fail("buffer 1 not recycled");
        }
        if (!buffer2Recycled) {
            Assert.fail("buffer 2 not recycled");
        }
        assertEquals(2, partition.getTotalNumberOfBuffers());
        assertEquals(0, partition.getTotalNumberOfBytes()); // buffer data is never consumed
    }

    @Test
    public void testReleaseParent() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        verifyViewReleasedAfterParentRelease(partition);
    }

    private void verifyViewReleasedAfterParentRelease(ResultSubpartition partition)
            throws Exception {
        // Add a bufferConsumer
        BufferConsumer bufferConsumer =
                createFilledFinishedBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
        partition.add(bufferConsumer);
        partition.finish();

        // Create the view
        BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);
        ResultSubpartitionView view = partition.createReadView(listener);

        // The added bufferConsumer and end-of-partition event
        assertNotNull(view.getNextBuffer());
        assertNotNull(view.getNextBuffer());

        // Release the parent
        assertFalse(view.isReleased());
        partition.release();

        // Verify that parent release is reflected at partition view
        assertTrue(view.isReleased());
    }

    public static PipelinedSubpartition createPipelinedSubpartition() {
        final ResultPartition parent = PartitionTestUtils.createPartition();

        return new PipelinedSubpartition(0, parent);
    }
}
