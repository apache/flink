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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    @ClassRule
    public static final TestExecutorResource<ExecutorService> EXECUTOR_RESOURCE =
            new TestExecutorResource<>(() -> Executors.newCachedThreadPool());

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
                        CheckedSupplier.unchecked(producer::call), EXECUTOR_RESOURCE.getExecutor());
        CompletableFuture<Boolean> consumerResult =
                CompletableFuture.supplyAsync(
                        CheckedSupplier.unchecked(consumer::call), EXECUTOR_RESOURCE.getExecutor());

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
            assertEquals(2, partition.getNumberOfQueuedBuffers());

            // create the read view first
            ResultSubpartitionView view = null;
            if (createView) {
                view = partition.createReadView(new NoOpBufferAvailablityListener());
            }

            partition.release();
            assertEquals(0, partition.getNumberOfQueuedBuffers());

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
        assertEquals(2, partition.getTotalNumberOfBuffersUnsafe());
        assertEquals(0, partition.getTotalNumberOfBytesUnsafe()); // buffer data is never consumed
    }

    @Test
    public void testReleaseParent() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        verifyViewReleasedAfterParentRelease(partition);
    }

    @Test
    public void testNumberOfQueueBuffers() throws Exception {
        final PipelinedSubpartition subpartition = createSubpartition();

        subpartition.add(createFilledFinishedBufferConsumer(4096));
        assertEquals(1, subpartition.getNumberOfQueuedBuffers());

        subpartition.add(createFilledFinishedBufferConsumer(4096));
        assertEquals(2, subpartition.getNumberOfQueuedBuffers());

        subpartition.getNextBuffer();

        assertEquals(1, subpartition.getNumberOfQueuedBuffers());
    }

    @Test
    public void testNewBufferSize() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertEquals(Integer.MAX_VALUE, subpartition.add(createFilledFinishedBufferConsumer(4)));

        // when: Changing buffer size.
        subpartition.bufferSize(42);

        // then: Changes successfully applied.
        assertEquals(42, subpartition.add(createFilledFinishedBufferConsumer(4)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeNewBufferSize() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertEquals(Integer.MAX_VALUE, subpartition.add(createFilledFinishedBufferConsumer(4)));

        // when: Changing buffer size to the negative value.
        subpartition.bufferSize(-1);
    }

    @Test
    public void testNegativeBufferSizeAsSignOfAddingFail() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertEquals(Integer.MAX_VALUE, subpartition.add(createFilledFinishedBufferConsumer(4)));

        // when: Finishing the subpartition which make following adding impossible.
        subpartition.finish();

        // then: -1 should be return because the add operation fails.
        assertEquals(-1, subpartition.add(createFilledFinishedBufferConsumer(4)));
    }

    @Test
    public void testProducerFailedException() {
        PipelinedSubpartition subpartition =
                new FailurePipelinedSubpartition(0, 2, PartitionTestUtils.createPartition());

        ResultSubpartitionView view =
                subpartition.createReadView(new NoOpBufferAvailablityListener());

        assertNotNull(view.getFailureCause());
        assertTrue(view.getFailureCause() instanceof CancelTaskException);
    }

    @Test
    public void testConsumeTimeoutableCheckpointBarrierQuickly() throws Exception {
        PipelinedSubpartition subpartition = createSubpartition();
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);
        assertSubpartitionChannelStateFuturesAndQueuedBuffers(subpartition, null, true, 0, false);

        // test without data buffer
        testConsumeQuicklyWithNDataBuffers(0, subpartition, 5L);

        // test with data buffer
        testConsumeQuicklyWithNDataBuffers(1, subpartition, 6L);
        testConsumeQuicklyWithNDataBuffers(2, subpartition, 7L);
    }

    private void testConsumeQuicklyWithNDataBuffers(
            int numberOfDataBuffers, PipelinedSubpartition subpartition, long checkpointId)
            throws Exception {
        // add data buffers and barrier
        for (int i = 0; i < numberOfDataBuffers; i++) {
            subpartition.add(createFilledFinishedBufferConsumer(4096));
        }
        subpartition.add(getTimeoutableBarrierBuffer(checkpointId));
        assertEquals(checkpointId, subpartition.getChannelStateCheckpointId());
        CompletableFuture<List<Buffer>> channelStateFuture = subpartition.getChannelStateFuture();
        assertSubpartitionChannelStateFuturesAndQueuedBuffers(
                subpartition, channelStateFuture, false, numberOfDataBuffers + 1, false);

        // poll data buffers first
        for (int i = 0; i < numberOfDataBuffers; i++) {
            pollBufferAndCheckType(subpartition, Buffer.DataType.DATA_BUFFER);
        }
        pollBufferAndCheckType(
                subpartition, Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER);

        assertSubpartitionChannelStateFuturesAndQueuedBuffers(
                subpartition, channelStateFuture, true, 0, true);
        assertTrue(channelStateFuture.get().isEmpty());
        subpartition.resumeConsumption();
    }

    @Test
    public void testTimeoutAlignedToUnalignedBarrier() throws Exception {
        PipelinedSubpartition subpartition = createSubpartition();
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);
        assertSubpartitionChannelStateFuturesAndQueuedBuffers(subpartition, null, true, 0, false);

        // test without data buffer
        testTimeoutWithNDataBuffers(0, subpartition, 7L);

        // test with data buffer
        testTimeoutWithNDataBuffers(1, subpartition, 8L);
    }

    private void testTimeoutWithNDataBuffers(
            int numberOfDataBuffers, PipelinedSubpartition subpartition, long checkpointId)
            throws Exception {
        // put data buffers and barrier
        List<Buffer> expectedBuffers = new ArrayList<>();
        for (int i = 0; i < numberOfDataBuffers; i++) {
            BufferConsumer bufferConsumer = createFilledFinishedBufferConsumer(4096);
            subpartition.add(bufferConsumer);
            expectedBuffers.add(bufferConsumer.copy().build());
        }
        subpartition.add(getTimeoutableBarrierBuffer(checkpointId));

        assertEquals(checkpointId, subpartition.getChannelStateCheckpointId());
        CompletableFuture<List<Buffer>> channelStateFuture = subpartition.getChannelStateFuture();
        assertSubpartitionChannelStateFuturesAndQueuedBuffers(
                subpartition, channelStateFuture, false, numberOfDataBuffers + 1, false);

        subpartition.alignedBarrierTimeout(checkpointId);
        assertSubpartitionChannelStateFuturesAndQueuedBuffers(
                subpartition, channelStateFuture, true, numberOfDataBuffers + 1, true);

        pollBufferAndCheckType(subpartition, Buffer.DataType.PRIORITIZED_EVENT_BUFFER);
        for (int i = 0; i < numberOfDataBuffers; i++) {
            pollBufferAndCheckType(subpartition, Buffer.DataType.DATA_BUFFER);
        }

        assertEquals(expectedBuffers, channelStateFuture.get());
    }

    private void pollBufferAndCheckType(
            PipelinedSubpartition subpartition, Buffer.DataType dataType) {
        ResultSubpartition.BufferAndBacklog barrierBuffer = subpartition.pollBuffer();
        assertNotNull(barrierBuffer);
        assertEquals(dataType, barrierBuffer.buffer().getDataType());
    }

    @Test
    public void testConcurrentTimeoutableCheckpointBarrier() throws Exception {
        PipelinedSubpartition subpartition = createSubpartition();
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);

        subpartition.add(getTimeoutableBarrierBuffer(10L));
        assertEquals(10L, subpartition.getChannelStateCheckpointId());
        CompletableFuture<List<Buffer>> checkpointFuture10 = subpartition.getChannelStateFuture();
        assertNotNull(checkpointFuture10);

        try {
            // It should fail due to currently does not support concurrent unaligned checkpoints.
            subpartition.add(getTimeoutableBarrierBuffer(11L));
            checkpointFuture10.get();
            fail("Should fail with an IllegalStateException.");
        } catch (Throwable e) {
            ExceptionUtils.assertThrowable(e, IllegalStateException.class);
        }
    }

    private BufferConsumer getTimeoutableBarrierBuffer(long checkpointId) throws IOException {
        CheckpointOptions checkpointOptions =
                CheckpointOptions.alignedWithTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault(),
                        1000);
        return EventSerializer.toBufferConsumer(
                new CheckpointBarrier(checkpointId, System.currentTimeMillis(), checkpointOptions),
                false);
    }

    private void assertSubpartitionChannelStateFuturesAndQueuedBuffers(
            PipelinedSubpartition subpartition,
            CompletableFuture<List<Buffer>> channelStateFuture,
            boolean channelStateFutureIsNull,
            long numberOfQueuedBuffers,
            boolean expectedFutureIsDone) {
        assertEquals(channelStateFutureIsNull, subpartition.getChannelStateFuture() == null);
        assertEquals(numberOfQueuedBuffers, subpartition.getNumberOfQueuedBuffers());
        if (channelStateFuture != null) {
            assertEquals(expectedFutureIsDone, channelStateFuture.isDone());
        }
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

        return new PipelinedSubpartition(0, 2, parent);
    }

    public static PipelinedSubpartition createPipelinedSubpartition(ResultPartition parent) {
        return new PipelinedSubpartition(0, 2, parent);
    }

    private static class FailurePipelinedSubpartition extends PipelinedSubpartition {

        FailurePipelinedSubpartition(
                int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
            super(index, receiverExclusiveBuffersPerChannel, parent);
        }

        @Override
        Throwable getFailureCause() {
            return new RuntimeException("Expected test exception");
        }
    }
}
