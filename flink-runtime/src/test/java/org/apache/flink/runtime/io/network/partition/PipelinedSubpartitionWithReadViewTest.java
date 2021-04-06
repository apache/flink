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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Additional tests for {@link PipelinedSubpartition} which require an availability listener and a
 * read view.
 *
 * @see PipelinedSubpartitionTest
 */
@RunWith(Parameterized.class)
public class PipelinedSubpartitionWithReadViewTest {

    ResultPartition resultPartition;
    PipelinedSubpartition subpartition;
    AwaitableBufferAvailablityListener availablityListener;
    PipelinedSubpartitionView readView;

    @Parameterized.Parameter public boolean compressionEnabled;

    @Parameterized.Parameters(name = "compressionEnabled = {0}")
    public static Boolean[] parameters() {
        return new Boolean[] {false, true};
    }

    @Before
    public void before() throws IOException {
        setup(ResultPartitionType.PIPELINED);
        subpartition = new PipelinedSubpartition(0, resultPartition);
        availablityListener = new AwaitableBufferAvailablityListener();
        readView = subpartition.createReadView(availablityListener);
    }

    @After
    public void tearDown() {
        readView.releaseAllResources();
        subpartition.release();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddTwoNonFinishedBuffer() throws IOException {
        subpartition.add(createBufferBuilder().createBufferConsumer());
        subpartition.add(createBufferBuilder().createBufferConsumer());
        assertNull(readView.getNextBuffer());
    }

    @Test
    public void testRelease() {
        readView.releaseAllResources();
        assertFalse(
                resultPartition
                        .getPartitionManager()
                        .getUnreleasedPartitions()
                        .contains(resultPartition.getPartitionId()));
    }

    @Test
    public void testAddEmptyNonFinishedBuffer() throws IOException {
        assertEquals(0, availablityListener.getNumNotifications());

        BufferBuilder bufferBuilder = createBufferBuilder();
        subpartition.add(bufferBuilder.createBufferConsumer());

        assertEquals(0, availablityListener.getNumNotifications());
        assertNull(readView.getNextBuffer());

        bufferBuilder.finish();
        bufferBuilder = createBufferBuilder();
        subpartition.add(bufferBuilder.createBufferConsumer());

        assertEquals(1, subpartition.getBuffersInBacklog());
        assertEquals(
                1,
                availablityListener
                        .getNumNotifications()); // notification from finishing previous buffer.
        assertNull(readView.getNextBuffer());
        assertEquals(0, subpartition.getBuffersInBacklog());
    }

    @Test
    public void testAddNonEmptyNotFinishedBuffer() throws Exception {
        assertEquals(0, availablityListener.getNumNotifications());

        subpartition.add(createFilledUnfinishedBufferConsumer(1024));

        // note that since the buffer builder is not finished, there is still a retained instance!
        assertEquals(0, subpartition.getBuffersInBacklog());
        assertNextBuffer(readView, 1024, false, 0, false, false);
    }

    /**
     * Normally moreAvailable flag from InputChannel should ignore non finished BufferConsumers,
     * otherwise we would busy loop on the unfinished BufferConsumers.
     */
    @Test
    public void testUnfinishedBufferBehindFinished() throws Exception {
        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

        assertEquals(1, subpartition.getBuffersInBacklog());
        assertThat(availablityListener.getNumNotifications(), greaterThan(0L));
        assertNextBuffer(readView, 1025, false, 0, false, true);
        // not notified, but we could still access the unfinished buffer
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    /**
     * After flush call unfinished BufferConsumers should be reported as available, otherwise we
     * might not flush some of the data.
     */
    @Test
    public void testFlushWithUnfinishedBufferBehindFinished() throws Exception {
        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished
        long oldNumNotifications = availablityListener.getNumNotifications();

        assertEquals(1, subpartition.getBuffersInBacklog());

        subpartition.flush();
        // buffer queue is > 1, should already be notified, no further notification necessary
        assertThat(oldNumNotifications, greaterThan(0L));
        assertEquals(oldNumNotifications, availablityListener.getNumNotifications());

        assertEquals(2, subpartition.getBuffersInBacklog());
        assertNextBuffer(readView, 1025, true, 1, false, true);
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    /**
     * A flush call with a buffer size of 1 should always notify consumers (unless already flushed).
     */
    @Test
    public void testFlushWithUnfinishedBufferBehindFinished2() throws Exception {
        // no buffers -> no notification or any other effects
        subpartition.flush();
        assertEquals(0, availablityListener.getNumNotifications());

        subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
        subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

        assertEquals(1, subpartition.getBuffersInBacklog());
        assertNextBuffer(readView, 1025, false, 0, false, true);

        long oldNumNotifications = availablityListener.getNumNotifications();
        subpartition.flush();
        // buffer queue is 1 again -> need to flush
        assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());
        subpartition.flush();
        // calling again should not flush again
        assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());

        assertEquals(1, subpartition.getBuffersInBacklog());
        assertNextBuffer(readView, 1024, false, 0, false, false);
        assertNoNextBuffer(readView);
    }

    @Test
    public void testMultipleEmptyBuffers() throws Exception {
        assertEquals(0, availablityListener.getNumNotifications());

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertEquals(0, availablityListener.getNumNotifications());

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertEquals(1, availablityListener.getNumNotifications());

        subpartition.add(createFilledFinishedBufferConsumer(0));
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(2, subpartition.getBuffersInBacklog());

        subpartition.add(createFilledFinishedBufferConsumer(1024));
        assertEquals(1, availablityListener.getNumNotifications());

        assertNextBuffer(readView, 1024, false, 0, false, true);
    }

    @Test
    public void testEmptyFlush() {
        subpartition.flush();
        assertEquals(0, availablityListener.getNumNotifications());
    }

    @Test
    public void testBasicPipelinedProduceConsumeLogic() throws Exception {
        // Empty => should return null
        assertFalse(readView.isAvailable(0));
        assertNoNextBuffer(readView);
        assertFalse(readView.isAvailable(0)); // also after getNextBuffer()
        assertEquals(0, availablityListener.getNumNotifications());

        // Add data to the queue...
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertFalse(readView.isAvailable(0));

        assertEquals(1, subpartition.getTotalNumberOfBuffers());
        assertEquals(0, subpartition.getBuffersInBacklog());
        assertEquals(
                0, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer

        assertEquals(0, availablityListener.getNumNotifications());

        // ...and one available result
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertEquals(
                BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, subpartition.getBuffersInBacklog());
        assertNoNextBuffer(readView);
        assertEquals(0, subpartition.getBuffersInBacklog());

        // Add data to the queue...
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertFalse(readView.isAvailable(0));

        assertEquals(2, subpartition.getTotalNumberOfBuffers());
        assertEquals(0, subpartition.getBuffersInBacklog());
        assertEquals(
                BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, availablityListener.getNumNotifications());

        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertEquals(
                2 * BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, subpartition.getBuffersInBacklog());
        assertNoNextBuffer(readView);
        assertEquals(0, subpartition.getBuffersInBacklog());

        // some tests with events

        // fill with: buffer, event, and buffer
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertFalse(readView.isAvailable(0));
        subpartition.add(createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.EVENT_BUFFER));
        assertFalse(readView.isAvailable(0));
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        assertFalse(readView.isAvailable(0));

        assertEquals(5, subpartition.getTotalNumberOfBuffers());
        assertEquals(1, subpartition.getBuffersInBacklog()); // two buffers (events don't count)
        assertEquals(
                2 * BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(1, availablityListener.getNumNotifications());

        // the first buffer
        assertNextBuffer(readView, BUFFER_SIZE, true, 0, true, true);
        assertEquals(
                3 * BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, subpartition.getBuffersInBacklog());

        // the event
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
        assertEquals(
                4 * BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, subpartition.getBuffersInBacklog());

        // the remaining buffer
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertEquals(
                5 * BUFFER_SIZE,
                subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
        assertEquals(0, subpartition.getBuffersInBacklog());

        // nothing more
        assertNoNextBuffer(readView);
        assertEquals(0, subpartition.getBuffersInBacklog());

        assertEquals(5, subpartition.getTotalNumberOfBuffers());
        assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes());
        assertEquals(1, availablityListener.getNumNotifications());
    }

    @Test
    public void testBarrierOvertaking() throws Exception {
        final RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        subpartition.setChannelStateWriter(channelStateWriter);

        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertEquals(0, availablityListener.getNumNotifications());
        assertEquals(0, availablityListener.getNumPriorityEvents());

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(0, availablityListener.getNumPriorityEvents());

        BufferConsumer eventBuffer =
                EventSerializer.toBufferConsumer(EndOfSuperstepEvent.INSTANCE, false);
        subpartition.add(eventBuffer);
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(0, availablityListener.getNumPriorityEvents());

        subpartition.add(createFilledFinishedBufferConsumer(4));
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(0, availablityListener.getNumPriorityEvents());

        CheckpointOptions options =
                CheckpointOptions.unaligned(
                        new CheckpointStorageLocationReference(new byte[] {0, 1, 2}));
        channelStateWriter.start(0, options);
        BufferConsumer barrierBuffer =
                EventSerializer.toBufferConsumer(new CheckpointBarrier(0, 0, options), true);
        subpartition.add(barrierBuffer);
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(1, availablityListener.getNumPriorityEvents());

        final List<Buffer> inflight =
                channelStateWriter.getAddedOutput().get(subpartition.getSubpartitionInfo());
        assertEquals(
                Arrays.asList(1, 2, 4),
                inflight.stream().map(Buffer::getSize).collect(Collectors.toList()));
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

    @Test
    public void testAvailabilityAfterPriority() throws Exception {
        subpartition.setChannelStateWriter(ChannelStateWriter.NO_OP);

        CheckpointOptions options =
                CheckpointOptions.unaligned(
                        new CheckpointStorageLocationReference(new byte[] {0, 1, 2}));
        BufferConsumer barrierBuffer =
                EventSerializer.toBufferConsumer(new CheckpointBarrier(0, 0, options), true);
        subpartition.add(barrierBuffer);
        assertEquals(1, availablityListener.getNumNotifications());
        assertEquals(1, availablityListener.getNumPriorityEvents());

        subpartition.add(createFilledFinishedBufferConsumer(1));
        assertEquals(2, availablityListener.getNumNotifications());
        assertEquals(1, availablityListener.getNumPriorityEvents());

        subpartition.add(createFilledFinishedBufferConsumer(2));
        assertEquals(2, availablityListener.getNumNotifications());
        assertEquals(1, availablityListener.getNumPriorityEvents());

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

    @Test
    public void testBacklogConsistentWithNumberOfConsumableBuffers() throws Exception {
        testBacklogConsistentWithNumberOfConsumableBuffers(false, false);
    }

    @Test
    public void testBacklogConsistentWithConsumableBuffersForFlushedPartition() throws Exception {
        testBacklogConsistentWithNumberOfConsumableBuffers(true, false);
    }

    @Test
    public void testBacklogConsistentWithConsumableBuffersForFinishedPartition() throws Exception {
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

        final int backlog = subpartition.getBuffersInBacklog();

        int numberOfConsumableBuffers = 0;
        try (final CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            while (readView.isAvailable(Integer.MAX_VALUE)) {
                ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
                assertNotNull(bufferAndBacklog);

                if (bufferAndBacklog.buffer().isBuffer()) {
                    ++numberOfConsumableBuffers;
                }

                closeableRegistry.registerCloseable(bufferAndBacklog.buffer()::recycleBuffer);
            }

            assertThat(backlog, is(numberOfConsumableBuffers));
        }
    }

    @Test
    public void testBlockedByCheckpointAndResumeConsumption()
            throws IOException, InterruptedException {
        blockSubpartitionByCheckpoint(1);

        // add an event after subpartition blocked
        subpartition.add(createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.EVENT_BUFFER));
        // no data available notification after adding an event
        checkNumNotificationsAndAvailability(1);

        resumeConsumptionAndCheckAvailability(0, true);
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);

        blockSubpartitionByCheckpoint(2);

        // add a buffer and flush the subpartition
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        subpartition.flush();
        // no data available notification after adding a buffer and flushing the subpartition
        checkNumNotificationsAndAvailability(2);

        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, false);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);

        blockSubpartitionByCheckpoint(3);

        // add two buffers to the subpartition
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        // no data available notification after adding the second buffer
        checkNumNotificationsAndAvailability(3);

        resumeConsumptionAndCheckAvailability(Integer.MAX_VALUE, true);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
        assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
    }

    // ------------------------------------------------------------------------

    private void blockSubpartitionByCheckpoint(int numNotifications)
            throws IOException, InterruptedException {
        subpartition.add(
                createEventBufferConsumer(BUFFER_SIZE, Buffer.DataType.ALIGNED_CHECKPOINT_BARRIER));

        assertEquals(numNotifications, availablityListener.getNumNotifications());
        assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
    }

    private void checkNumNotificationsAndAvailability(int numNotifications)
            throws IOException, InterruptedException {
        assertEquals(numNotifications, availablityListener.getNumNotifications());

        // view not available and no buffer can be read
        assertFalse(readView.isAvailable(Integer.MAX_VALUE));
        assertNoNextBuffer(readView);
    }

    private void resumeConsumptionAndCheckAvailability(int availableCredit, boolean dataAvailable) {
        readView.resumeConsumption();

        assertEquals(dataAvailable, readView.isAvailable(availableCredit));
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
        assertNotNull(bufferAndBacklog);
        try {
            assertEquals(
                    "buffer size",
                    expectedReadableBufferSize,
                    bufferAndBacklog.buffer().readableBytes());
            assertEquals("buffer or event", expectedIsBuffer, bufferAndBacklog.buffer().isBuffer());
            if (expectedEventClass != null) {
                Assert.assertThat(
                        EventSerializer.fromBuffer(
                                bufferAndBacklog.buffer(), ClassLoader.getSystemClassLoader()),
                        instanceOf(expectedEventClass));
            }
            assertEquals(
                    "data available", expectedIsDataAvailable, bufferAndBacklog.isDataAvailable());
            assertEquals(
                    "data available",
                    expectedIsDataAvailable,
                    readView.isAvailable(Integer.MAX_VALUE));
            assertEquals("backlog", expectedBuffersInBacklog, bufferAndBacklog.buffersInBacklog());
            assertEquals(
                    "event available",
                    expectedIsEventAvailable,
                    bufferAndBacklog.isEventAvailable());
            assertEquals("event available", expectedIsEventAvailable, readView.isAvailable(0));

            assertFalse("not recycled", bufferAndBacklog.buffer().isRecycled());
        } finally {
            bufferAndBacklog.buffer().recycleBuffer();
        }
        assertEquals(
                "recycled", expectedRecycledAfterRecycle, bufferAndBacklog.buffer().isRecycled());
    }

    static void assertNoNextBuffer(ResultSubpartitionView readView)
            throws IOException, InterruptedException {
        assertNull(readView.getNextBuffer());
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
