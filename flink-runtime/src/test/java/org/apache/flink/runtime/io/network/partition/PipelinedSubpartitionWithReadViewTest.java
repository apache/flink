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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledUnfinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional tests for {@link PipelinedSubpartition} which require an availability listener and a
 * read view.
 *
 * @see PipelinedSubpartitionTest
 */
@ExtendWith(ParameterizedTestExtension.class)
class PipelinedSubpartitionWithReadViewTest {

    ResultPartition resultPartition;
    PipelinedSubpartition subpartition;
    AwaitableBufferAvailablityListener availablityListener;
    PipelinedSubpartitionView readView;

    @Parameter private boolean compressionEnabled;

    @Parameters(name = "compressionEnabled = {0}")
    private static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    void before() throws IOException {
        setup(ResultPartitionType.PIPELINED);
        subpartition = new PipelinedSubpartition(0, 2, Integer.MAX_VALUE, resultPartition);
        availablityListener = new AwaitableBufferAvailablityListener();
        readView = subpartition.createReadView(availablityListener);
    }

    @AfterEach
    void tearDown() {
        readView.releaseAllResources();
        subpartition.release();
    }

    @TestTemplate
    void testAddTwoNonFinishedBuffer() throws IOException {
        subpartition.add(createBufferBuilder().createBufferConsumer());
        subpartition.add(createBufferBuilder().createBufferConsumer());
        assertThatThrownBy(readView::getNextBuffer).isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void testRelease() {
        readView.releaseAllResources();
        resultPartition.close();
        assertThat(resultPartition.getPartitionManager().getUnreleasedPartitions())
                .doesNotContain(resultPartition.getPartitionId());
    }

    @TestTemplate
    void testAddEmptyNonFinishedBuffer() throws IOException {
        assertThat(availablityListener.getNumNotifications()).isZero();

        BufferBuilder bufferBuilder = createBufferBuilder();
        subpartition.add(bufferBuilder.createBufferConsumer());

        assertThat(availablityListener.getNumNotifications()).isZero();
        assertThat(readView.getNextBuffer()).isNull();

        bufferBuilder.finish();
        bufferBuilder = createBufferBuilder();
        subpartition.add(bufferBuilder.createBufferConsumer());

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isOne();
        // notification from finishing previous buffer.
        assertThat(availablityListener.getNumNotifications()).isOne();

        assertThat(readView.getNextBuffer()).isNull();
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
    }

    @TestTemplate
    void testAddNonEmptyNotFinishedBuffer() throws Exception {
        assertThat(availablityListener.getNumNotifications()).isZero();

        subpartition.add(createFilledUnfinishedBufferConsumer(1024));

        // note that since the buffer builder is not finished, there is still a retained instance!
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
        assertNextBuffer(readView, 1024, false, 0, false, false);
    }

    /**
     * Normally moreAvailable flag from InputChannel should ignore non finished BufferConsumers,
     * otherwise we would busy loop on the unfinished BufferConsumers.
     */
    @TestTemplate
    void testUnfinishedBufferBehindFinished() throws Exception {
        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isOne();
        assertThat(availablityListener.getNumNotifications()).isGreaterThan(0L);
        assertNextBuffer(readView, 1025, false, 0, false, true);
        // not notified, but we could still access the unfinished buffer
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    /**
     * After flush call unfinished BufferConsumers should be reported as available, otherwise we
     * might not flush some of the data.
     */
    @TestTemplate
    void testFlushWithUnfinishedBufferBehindFinished() throws Exception {
        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished
        long oldNumNotifications = availablityListener.getNumNotifications();

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isOne();

        subpartition.flush();
        // buffer queue is > 1, should already be notified, no further notification necessary
        assertThat(oldNumNotifications).isGreaterThan(0L);
        assertThat(availablityListener.getNumNotifications()).isEqualTo(oldNumNotifications);

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isEqualTo(2);
        assertNextBuffer(readView, 1025, true, 1, false, true);
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    /**
     * A flush call with a buffer size of 1 should always notify consumers (unless already flushed).
     */
    @TestTemplate
    void testFlushWithUnfinishedBufferBehindFinished2() throws Exception {
        // no buffers -> no notification or any other effects
        subpartition.flush();
        assertThat(availablityListener.getNumNotifications()).isZero();

        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isOne();
        assertNextBuffer(readView, 1025, false, 0, false, true);

        long oldNumNotifications = availablityListener.getNumNotifications();
        subpartition.flush();
        // buffer queue is 1 again -> need to flush
        assertThat(availablityListener.getNumNotifications()).isEqualTo(oldNumNotifications + 1);
        subpartition.flush();
        // calling again should not flush again
        assertThat(availablityListener.getNumNotifications()).isEqualTo(oldNumNotifications + 1);

        assertThat(subpartition.getBuffersInBacklogUnsafe()).isOne();
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    @TestTemplate
    void testMultipleEmptyBuffers() throws Exception {
        assertThat(availablityListener.getNumNotifications()).isZero();

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertThat(availablityListener.getNumNotifications()).isZero();

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertThat(availablityListener.getNumNotifications()).isOne();

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isEqualTo(2);

        subpartition.add(createFilledFinishedBufferConsumer(1024));
        assertThat(availablityListener.getNumNotifications()).isOne();

        assertNextBuffer(readView, 1024, false, 0, false, true);
    }

    @TestTemplate
    void testEmptyFlush() {
        subpartition.flush();
        assertThat(availablityListener.getNumNotifications()).isZero();
    }

    @TestTemplate
    void testBasicPipelinedProduceConsumeLogic() throws Exception {
        // Empty => should return null
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();
        assertNoNextBuffer(readView);
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable())
                .isFalse(); // also after getNextBuffer()
        assertThat(availablityListener.getNumNotifications()).isZero();

        // Add data to the queue...
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();

        assertThat(subpartition.getTotalNumberOfBuffersUnsafe()).isOne();
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isZero(); // only updated when getting the buffer

        assertThat(availablityListener.getNumNotifications()).isZero();

        // ...and one available result
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(BUFFER_SIZE); // only updated when getting the buffer
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
        assertNoNextBuffer(readView);
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        // Add data to the queue...
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();

        assertThat(subpartition.getTotalNumberOfBuffersUnsafe()).isEqualTo(2);
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(BUFFER_SIZE); // only updated when getting the buffer
        assertThat(availablityListener.getNumNotifications()).isZero();

        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(2 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();
        assertNoNextBuffer(readView);
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        // some tests with events

        // fill with: buffer, event, and buffer
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();
        subpartition.add(createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.EVENT_BUFFER));
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertThat(readView.getAvailabilityAndBacklog(false).isAvailable()).isFalse();

        assertThat(subpartition.getTotalNumberOfBuffersUnsafe()).isEqualTo(5);
        assertThat(subpartition.getBuffersInBacklogUnsafe())
                .isOne(); // two buffers (events don't count)
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(2 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(availablityListener.getNumNotifications()).isOne();

        // the first buffer
        assertNextBuffer(readView, BUFFER_SIZE, true, 0, true, true);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(3 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        // the event
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(4 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        // the remaining buffer
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(5 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        // nothing more
        assertNoNextBuffer(readView);
        assertThat(subpartition.getBuffersInBacklogUnsafe()).isZero();

        assertThat(subpartition.getTotalNumberOfBuffersUnsafe()).isEqualTo(5);
        assertThat(subpartition.getTotalNumberOfBytesUnsafe())
                .isEqualTo(5 * BUFFER_SIZE); // only updated when getting the buffer
        assertThat(availablityListener.getNumNotifications()).isOne();
    }

    @TestTemplate
    void testBarrierOvertaking() throws Exception {
        final RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        subpartition.setChannelStateWriter(channelStateWriter);

        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertThat(availablityListener.getNumNotifications()).isZero();
        assertThat(availablityListener.getNumPriorityEvents()).isZero();

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(availablityListener.getNumPriorityEvents()).isZero();

        BufferConsumer eventBuffer =
                EventSerializer.toBufferConsumer(EndOfSuperstepEvent.INSTANCE, false);
        subpartition.add(eventBuffer);
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(availablityListener.getNumPriorityEvents()).isZero();

        subpartition.add(createFilledFinishedBufferConsumer(4));
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(availablityListener.getNumPriorityEvents()).isZero();

        CheckpointOptions options =
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(new byte[] {0, 1, 2}));
        channelStateWriter.start(0, options);
        BufferConsumer barrierBuffer =
                EventSerializer.toBufferConsumer(new CheckpointBarrier(0, 0, options), true);
        subpartition.add(barrierBuffer);
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(availablityListener.getNumPriorityEvents()).isOne();

        final List<Buffer> inflight =
                channelStateWriter.getAddedOutput().get(subpartition.getSubpartitionInfo());
        assertThat(inflight.stream().map(Buffer::getSize).collect(Collectors.toList()))
                .containsExactly(1, 2, 4);
        inflight.forEach(Buffer::recycleBuffer);

        assertNextEvent(
                readView,
                barrierBuffer.getWrittenBytes(),
                CheckpointBarrier.class,
                true,
                2,
                false,
                true);
        assertNextBuffer(readView, 1, true, 1, false, true);
        assertNextBuffer(readView, 2, true, 0, true, true);
        assertNextEvent(
                readView,
                eventBuffer.getWrittenBytes(),
                EndOfSuperstepEvent.class,
                false,
                0,
                false,
                true);
        assertNextBuffer(readView, 4, false, 0, false, true);
        assertNoNextBuffer(readView);
    }

    @TestTemplate
    void testAvailabilityAfterPriority() throws Exception {
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);

        CheckpointOptions options =
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        new CheckpointStorageLocationReference(new byte[] {0, 1, 2}));
        BufferConsumer barrierBuffer =
                EventSerializer.toBufferConsumer(new CheckpointBarrier(0, 0, options), true);
        subpartition.add(barrierBuffer);
        assertThat(availablityListener.getNumNotifications()).isOne();
        assertThat(availablityListener.getNumPriorityEvents()).isOne();

        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertThat(availablityListener.getNumNotifications()).isEqualTo(2);
        assertThat(availablityListener.getNumPriorityEvents()).isOne();

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertThat(availablityListener.getNumNotifications()).isEqualTo(2);
        assertThat(availablityListener.getNumPriorityEvents()).isOne();

        assertNextEvent(
                readView,
                barrierBuffer.getWrittenBytes(),
                CheckpointBarrier.class,
                true,
                1,
                false,
                true);
        assertNextBuffer(readView, 1, false, 0, false, true);
        assertNextBuffer(readView, 2, false, 0, false, true);
        assertNoNextBuffer(readView);
    }

    @TestTemplate
    void testBacklogConsistentWithNumberOfConsumableBuffers() throws Exception {
        testBacklogConsistentWithNumberOfConsumableBuffers(false, false);
    }

    @TestTemplate
    void testBacklogConsistentWithConsumableBuffersForFlushedPartition() throws Exception {
        testBacklogConsistentWithNumberOfConsumableBuffers(true, false);
    }

    @TestTemplate
    void testBacklogConsistentWithConsumableBuffersForFinishedPartition() throws Exception {
        testBacklogConsistentWithNumberOfConsumableBuffers(false, true);
    }

    private void testBacklogConsistentWithNumberOfConsumableBuffers(
            boolean isFlushRequested, boolean isFinished) throws Exception {
        final int numberOfAddedBuffers = 5;

        for (int i = 1; i <= numberOfAddedBuffers; i++) {
            if (i < numberOfAddedBuffers || isFinished) {
                subpartition.add(createFilledFinishedBufferConsumer(1024));
            } else {
                subpartition.add(createFilledUnfinishedBufferConsumer(1024));
            }
        }

        if (isFlushRequested) {
            subpartition.flush();
        }

        if (isFinished) {
            subpartition.finish();
        }

        final int backlog = subpartition.getBuffersInBacklogUnsafe();

        int numberOfConsumableBuffers = 0;
        try (final CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            while (readView.getAvailabilityAndBacklog(true).isAvailable()) {
                ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
                assertThat(bufferAndBacklog).isNotNull();

                if (bufferAndBacklog.buffer().isBuffer()) {
                    ++numberOfConsumableBuffers;
                }

                closeableRegistry.registerCloseable(bufferAndBacklog.buffer()::recycleBuffer);
            }

            assertThat(backlog).isEqualTo(numberOfConsumableBuffers);
        }
    }

    @TestTemplate
    void testResumeBlockedSubpartitionWithEvents() throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // add an event after subpartition blocked
        subpartition.add(createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.EVENT_BUFFER));
        // no data available notification after adding an event
        checkNumNotificationsAndAvailability(1);

        // Resumption will make the subpartition available.
        resumeConsumptionAndCheckAvailability(0, true);
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
    }

    @TestTemplate
    void testResumeBlockedSubpartitionWithUnfinishedBufferFlushed()
            throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // add a buffer and flush the subpartition
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        subpartition.flush();
        // no data available notification after adding a buffer and flushing the subpartition
        checkNumNotificationsAndAvailability(1);

        // Resumption will make the subpartition available.
        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, true);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
    }

    @TestTemplate
    void testResumeBlockedSubpartitionWithUnfinishedBufferNotFlushed()
            throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // add a buffer but not flush the subpartition.
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        // no data available notification after adding a buffer.
        checkNumNotificationsAndAvailability(1);

        // Resumption will not make the subpartition available since the data is not flushed before.
        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, false);
    }

    @TestTemplate
    void testResumeBlockedSubpartitionWithFinishedBuffers()
            throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // add two buffers to the subpartition
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        // no data available notification after adding the second buffer
        checkNumNotificationsAndAvailability(1);

        // Resumption will make the subpartition available.
        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, true);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
    }

    @TestTemplate
    void testResumeBlockedEmptySubpartition() throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // Resumption will not make the subpartition available since it is empty.
        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, false);
        assertNoNextBuffer(readView);
    }

    // ------------------------------------------------------------------------

    private void blockSubpartitionByCheckpoint(int numNotifications)
            throws IOException, InterruptedException {
        subpartition.add(
                createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.ALIGNED_CHECKPOINT_BARRIER));

        assertThat(availablityListener.getNumNotifications()).isEqualTo(numNotifications);
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
    }

    private void checkNumNotificationsAndAvailability(int numNotifications)
            throws IOException, InterruptedException {
        assertThat(availablityListener.getNumNotifications()).isEqualTo(numNotifications);

        // view not available and no buffer can be read
        assertThat(readView.getAvailabilityAndBacklog(true).isAvailable()).isFalse();
        assertNoNextBuffer(readView);
    }

    private void resumeConsumptionAndCheckAvailability(int availableCredit, boolean dataAvailable) {
        readView.resumeConsumption();

        assertThat(readView.getAvailabilityAndBacklog(availableCredit > 0).isAvailable())
                .isEqualTo(dataAvailable);
    }

    static void assertNextBuffer(
            ResultSubpartitionView readView,
            int expectedReadableBufferSize,
            boolean expectedIsDataAvailable,
            int expectedBuffersInBacklog,
            boolean expectedIsEventAvailable,
            boolean expectedRecycledAfterRecycle)
            throws IOException, InterruptedException {
        assertNextBufferOrEvent(
                readView,
                expectedReadableBufferSize,
                true,
                null,
                expectedIsDataAvailable,
                expectedBuffersInBacklog,
                expectedIsEventAvailable,
                expectedRecycledAfterRecycle);
    }

    static void assertNextEvent(
            ResultSubpartitionView readView,
            int expectedReadableBufferSize,
            Class<? extends AbstractEvent> expectedEventClass,
            boolean expectedIsDataAvailable,
            int expectedBuffersInBacklog,
            boolean expectedIsEventAvailable,
            boolean expectedRecycledAfterRecycle)
            throws IOException, InterruptedException {
        assertNextBufferOrEvent(
                readView,
                expectedReadableBufferSize,
                false,
                expectedEventClass,
                expectedIsDataAvailable,
                expectedBuffersInBacklog,
                expectedIsEventAvailable,
                expectedRecycledAfterRecycle);
    }

    private static void assertNextBufferOrEvent(
            ResultSubpartitionView readView,
            int expectedReadableBufferSize,
            boolean expectedIsBuffer,
            @Nullable Class<? extends AbstractEvent> expectedEventClass,
            boolean expectedIsDataAvailable,
            int expectedBuffersInBacklog,
            boolean expectedIsEventAvailable,
            boolean expectedRecycledAfterRecycle)
            throws IOException, InterruptedException {
        checkArgument(expectedEventClass == null || !expectedIsBuffer);

        ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
        assertThat(bufferAndBacklog).isNotNull();
        try {
            assertThat(bufferAndBacklog.buffer().readableBytes())
                    .as("buffer size")
                    .isEqualTo(expectedReadableBufferSize);
            assertThat(bufferAndBacklog.buffer().isBuffer())
                    .as("buffer or event")
                    .isEqualTo(expectedIsBuffer);
            if (expectedEventClass != null) {
                assertThat(
                                EventSerializer.fromBuffer(
                                        bufferAndBacklog.buffer(),
                                        ClassLoader.getSystemClassLoader()))
                        .isInstanceOf(expectedEventClass);
            }
            assertThat(bufferAndBacklog.isDataAvailable())
                    .as("data available")
                    .isEqualTo(expectedIsDataAvailable);
            assertThat(readView.getAvailabilityAndBacklog(true).isAvailable())
                    .as("data available")
                    .isEqualTo(expectedIsDataAvailable);
            assertThat(bufferAndBacklog.buffersInBacklog())
                    .as("backlog")
                    .isEqualTo(expectedBuffersInBacklog);
            assertThat(bufferAndBacklog.isEventAvailable())
                    .as("event available")
                    .isEqualTo(expectedIsEventAvailable);
            assertThat(readView.getAvailabilityAndBacklog(false).isAvailable())
                    .as("event available")
                    .isEqualTo(expectedIsEventAvailable);

            assertThat(bufferAndBacklog.buffer().isRecycled()).as("not recycled").isFalse();
        } finally {
            bufferAndBacklog.buffer().recycleBuffer();
        }
        assertThat(bufferAndBacklog.buffer().isRecycled())
                .as("recycled")
                .isEqualTo(expectedRecycledAfterRecycle);
    }

    static void assertNoNextBuffer(ResultSubpartitionView readView)
            throws IOException, InterruptedException {
        assertThat(readView.getNextBuffer()).isNull();
    }

    void setup(ResultPartitionType resultPartitionType) throws IOException {
        resultPartition =
                PartitionTestUtils.createPartition(
                        resultPartitionType,
                        NoOpFileChannelManager.INSTANCE,
                        compressionEnabled,
                        BUFFER_SIZE);
        resultPartition.setup();
    }
}
