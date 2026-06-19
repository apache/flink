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
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PipelinedSubpartition}.
 *
 * @see PipelinedSubpartitionWithReadViewTest
 */
@ExtendWith(NoOpTestExtension.class)
public class PipelinedSubpartitionTest extends SubpartitionTestBase {

    /** Executor service for concurrent produce/consume tests. */
    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(Executors::newCachedThreadPool);

    @Override
    PipelinedSubpartition createSubpartition() throws Exception {
        return createPipelinedSubpartition();
    }

    @Override
    ResultSubpartition createFailingWritesSubpartition() throws Exception {
        // the tests relating to this are currently not supported by the PipelinedSubpartition
        assumeThat(false).isTrue();
        return null;
    }

    @TestTemplate
    void testIllegalReadViewRequest() throws Exception {
        final PipelinedSubpartition subpartition = createSubpartition();

        // Successful request
        assertThat(subpartition.createReadView(new NoOpBufferAvailablityListener())).isNotNull();

        assertThatThrownBy(() -> subpartition.createReadView(new NoOpBufferAvailablityListener()))
                .withFailMessage(
                        "Did not throw expected exception after duplicate notifyNonEmpty view request.")
                .isInstanceOf(IllegalStateException.class);
    }

    /** Verifies that the isReleased() check of the view checks the parent subpartition. */
    @TestTemplate
    void testIsReleasedChecksParent() {
        PipelinedSubpartition subpartition = mock(PipelinedSubpartition.class);

        PipelinedSubpartitionView reader =
                new PipelinedSubpartitionView(subpartition, mock(BufferAvailabilityListener.class));

        assertThat(reader.isReleased()).isFalse();
        verify(subpartition, times(1)).isReleased();

        when(subpartition.isReleased()).thenReturn(true);
        assertThat(reader.isReleased()).isTrue();
        verify(subpartition, times(2)).isReleased();
    }

    @TestTemplate
    void testConcurrentFastProduceAndFastConsume() throws Exception {
        testProduceConsume(false, false);
    }

    @TestTemplate
    void testConcurrentFastProduceAndSlowConsume() throws Exception {
        testProduceConsume(false, true);
    }

    @TestTemplate
    void testConcurrentSlowProduceAndFastConsume() throws Exception {
        testProduceConsume(true, false);
    }

    @TestTemplate
    void testConcurrentSlowProduceAndSlowConsume() throws Exception {
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
                        assertThat(buffer.getSize()).isEqualTo(segment.size());

                        int expected = numberOfBuffers * (segment.size() / 4);

                        for (int i = 0; i < segment.size(); i += 4) {
                            assertThat(segment.getInt(i)).isEqualTo(expected);

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
                        CheckedSupplier.unchecked(producer::call),
                        EXECUTOR_EXTENSION.getExecutor());
        CompletableFuture<Boolean> consumerResult =
                CompletableFuture.supplyAsync(
                        CheckedSupplier.unchecked(consumer::call),
                        EXECUTOR_EXTENSION.getExecutor());

        FutureUtils.waitForAll(Arrays.asList(producerResult, consumerResult))
                .get(60_000L, TimeUnit.MILLISECONDS);
    }

    /** Tests cleanup of {@link PipelinedSubpartition#release()} with no read view attached. */
    @TestTemplate
    void testCleanupReleasedPartitionNoView() throws Exception {
        testCleanupReleasedPartition(false);
    }

    /** Tests cleanup of {@link PipelinedSubpartition#release()} with a read view attached. */
    @TestTemplate
    void testCleanupReleasedPartitionWithView() throws Exception {
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
            assertThat(partition.getNumberOfQueuedBuffers()).isEqualTo(2);

            // create the read view first
            ResultSubpartitionView view = null;
            if (createView) {
                view = partition.createReadView(new NoOpBufferAvailablityListener());
            }

            partition.release();
            assertThat(partition.getNumberOfQueuedBuffers()).isZero();

            assertThat(partition.isReleased()).isTrue();
            if (createView) {
                assertThat(view.isReleased()).isTrue();
            }
            assertThat(buffer1.isRecycled()).isTrue();
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

        assertThat(buffer1Recycled).withFailMessage("buffer 1 not recycled").isTrue();
        assertThat(buffer2Recycled).withFailMessage("buffer 2 not recycled").isTrue();
        assertThat(partition.getTotalNumberOfBuffersUnsafe()).isEqualTo(2);
        assertThat(partition.getTotalNumberOfBytesUnsafe())
                .isZero(); // buffer data is never consumed
    }

    @TestTemplate
    void testReleaseParent() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        verifyViewReleasedAfterParentRelease(partition);
    }

    @TestTemplate
    void testNumberOfQueueBuffers() throws Exception {
        final PipelinedSubpartition subpartition = createSubpartition();

        subpartition.add(createFilledFinishedBufferConsumer(4096));
        assertThat(subpartition.getNumberOfQueuedBuffers()).isOne();

        subpartition.add(createFilledFinishedBufferConsumer(4096));
        assertThat(subpartition.getNumberOfQueuedBuffers()).isEqualTo(2);

        subpartition.getNextBuffer();

        assertThat(subpartition.getNumberOfQueuedBuffers()).isOne();
    }

    @TestTemplate
    public void testStartingBufferSize() throws Exception {
        int startingBufferSize = 1234;
        final PipelinedSubpartition subpartition = createPipelinedSubpartition(startingBufferSize);
        assertEquals(startingBufferSize, subpartition.add(createFilledFinishedBufferConsumer(4)));
    }

    @TestTemplate
    void testNewBufferSize() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertThat(subpartition.add(createFilledFinishedBufferConsumer(4)))
                .isEqualTo(Integer.MAX_VALUE);

        // when: Changing buffer size.
        subpartition.bufferSize(42);

        // then: Changes successfully applied.
        assertThat(subpartition.add(createFilledFinishedBufferConsumer(4))).isEqualTo(42);
    }

    @TestTemplate
    void testNegativeNewBufferSize() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertThat(subpartition.add(createFilledFinishedBufferConsumer(4)))
                .isEqualTo(Integer.MAX_VALUE);

        // when: Changing buffer size to the negative value.
        assertThatThrownBy(() -> subpartition.bufferSize(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testNegativeBufferSizeAsSignOfAddingFail() throws Exception {
        // given: Buffer size equal to integer max value by default.
        final PipelinedSubpartition subpartition = createSubpartition();
        assertThat(subpartition.add(createFilledFinishedBufferConsumer(4)))
                .isEqualTo(Integer.MAX_VALUE);

        // when: Finishing the subpartition which make following adding impossible.
        subpartition.finish();

        // then: -1 should be return because the add operation fails.
        assertThat(subpartition.add(createFilledFinishedBufferConsumer(4))).isEqualTo(-1);
    }

    @TestTemplate
    void testProducerFailedException() {
        PipelinedSubpartition subpartition =
                new FailurePipelinedSubpartition(0, 2, PartitionTestUtils.createPartition());

        ResultSubpartitionView view =
                subpartition.createReadView(new NoOpBufferAvailablityListener());

        assertThat(view.getFailureCause()).isNotNull().isInstanceOf(CancelTaskException.class);
    }

    @TestTemplate
    void testConsumeTimeoutableCheckpointBarrierQuickly() throws Exception {
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
        assertThat(subpartition.getChannelStateCheckpointId()).isEqualTo(checkpointId);
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
        assertThatFuture(channelStateFuture).eventuallySucceeds().asList().isEmpty();
        subpartition.resumeConsumption();
    }

    @TestTemplate
    void testTimeoutAlignedToUnalignedBarrier() throws Exception {
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

        assertThat(subpartition.getChannelStateCheckpointId()).isEqualTo(checkpointId);
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

        assertThatFuture(channelStateFuture).eventuallySucceeds().isEqualTo(expectedBuffers);
    }

    private void pollBufferAndCheckType(
            PipelinedSubpartition subpartition, Buffer.DataType dataType) {
        ResultSubpartition.BufferAndBacklog barrierBuffer = subpartition.pollBuffer();
        assertThat(barrierBuffer).isNotNull();
        assertThat(barrierBuffer.buffer().getDataType()).isEqualTo(dataType);
    }

    @TestTemplate
    void testConcurrentTimeoutableCheckpointBarrier() throws Exception {
        PipelinedSubpartition subpartition = createSubpartition();
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);

        subpartition.add(getTimeoutableBarrierBuffer(10L));
        assertThat(subpartition.getChannelStateCheckpointId()).isEqualTo(10L);
        CompletableFuture<List<Buffer>> checkpointFuture10 = subpartition.getChannelStateFuture();
        assertThat(checkpointFuture10).isNotNull();

        // It should fail due to currently does not support concurrent unaligned checkpoints.
        subpartition.add(getTimeoutableBarrierBuffer(11L));
        assertThatThrownBy(checkpointFuture10::get)
                .hasCauseInstanceOf(IllegalStateException.class)
                .isInstanceOf(ExecutionException.class);
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
        assertThat(subpartition.getChannelStateFuture() == null)
                .isEqualTo(channelStateFutureIsNull);
        assertThat(subpartition.getNumberOfQueuedBuffers()).isEqualTo(numberOfQueuedBuffers);
        if (channelStateFuture != null) {
            assertThat(channelStateFuture.isDone()).isEqualTo(expectedFutureIsDone);
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
        assertThat(view.getNextBuffer()).isNotNull();
        assertThat(view.getNextBuffer()).isNotNull();

        // Release the parent
        assertThat(view.isReleased()).isFalse();
        partition.release();

        // Verify that parent release is reflected at partition view
        assertThat(view.isReleased()).isTrue();
    }

    public static PipelinedSubpartition createPipelinedSubpartition(int startingBufferSize) {
        final ResultPartition parent = PartitionTestUtils.createPartition();

        return new PipelinedSubpartition(0, 2, startingBufferSize, parent);
    }

    public static PipelinedSubpartition createPipelinedSubpartition() {
        final ResultPartition parent = PartitionTestUtils.createPartition();

        return new PipelinedSubpartition(0, 2, Integer.MAX_VALUE, parent);
    }

    public static PipelinedSubpartition createPipelinedSubpartition(ResultPartition parent) {
        return new PipelinedSubpartition(0, 2, Integer.MAX_VALUE, parent);
    }

    private static class FailurePipelinedSubpartition extends PipelinedSubpartition {

        FailurePipelinedSubpartition(
                int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
            super(index, receiverExclusiveBuffersPerChannel, Integer.MAX_VALUE, parent);
        }

        @Override
        Throwable getFailureCause() {
            return new RuntimeException("Expected test exception");
        }
    }
}
