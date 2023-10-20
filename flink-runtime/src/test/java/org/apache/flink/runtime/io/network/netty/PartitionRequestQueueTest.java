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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionTest;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.TestingResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionRequestQueue}. */
class PartitionRequestQueueTest {

    private static final int BUFFER_SIZE = 1024 * 1024;

    private static FileChannelManager fileChannelManager;

    @BeforeAll
    static void setUp(@TempDir File temporaryFolder) {
        fileChannelManager =
                new FileChannelManagerImpl(
                        new String[] {temporaryFolder.getAbsolutePath()}, "testing");
    }

    @AfterAll
    static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    /** Test that PartitionNotFound message will be sent to downstream in notifying timeout. */
    @Test
    public void testNotifyReaderPartitionTimeout() throws Exception {
        PartitionRequestQueue queue = new PartitionRequestQueue();
        EmbeddedChannel channel = new EmbeddedChannel(queue);
        ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
        ResultPartitionID resultPartitionId = new ResultPartitionID();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(new InputChannelID(0, 0), 10, queue);
        reader.requestSubpartitionViewOrRegisterListener(
                resultPartitionManager, resultPartitionId, new ResultSubpartitionIndexSet(0));

        assertThat(
                        resultPartitionManager
                                .getListenerManagers()
                                .get(resultPartitionId)
                                .getPartitionRequestListeners())
                .hasSize(1);

        reader.notifyPartitionRequestTimeout(
                resultPartitionManager
                        .getListenerManagers()
                        .get(resultPartitionId)
                        .getPartitionRequestListeners()
                        .iterator()
                        .next());

        channel.runPendingTasks();

        Object read = channel.readOutbound();
        assertThat(read)
                .isNotNull()
                .isInstanceOf(NettyMessage.ErrorResponse.class)
                .isInstanceOfSatisfying(
                        NettyMessage.ErrorResponse.class,
                        r -> assertThat(r.cause).isInstanceOf(PartitionNotFoundException.class));
    }

    /**
     * In case of enqueuing an empty reader and a reader that actually has some buffers when channel
     * is not writable, on channelWritability change event should result in reading all of the
     * messages.
     */
    @Test
    void testNotifyReaderNonEmptyOnEmptyReaders() throws Exception {
        final int buffersToWrite = 5;
        PartitionRequestQueue queue = new PartitionRequestQueue();
        EmbeddedChannel channel = new EmbeddedChannel(queue);

        CreditBasedSequenceNumberingViewReader reader1 =
                new CreditBasedSequenceNumberingViewReader(new InputChannelID(0, 0), 10, queue);
        CreditBasedSequenceNumberingViewReader reader2 =
                new CreditBasedSequenceNumberingViewReader(new InputChannelID(1, 1), 10, queue);

        ResultSubpartitionView view1 = new EmptyAlwaysAvailableResultSubpartitionView();
        reader1.notifySubpartitionsCreated(
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view1)
                        .build(),
                new ResultSubpartitionIndexSet(0));
        reader1.notifyDataAvailable(view1);
        assertThat(reader1.getAvailabilityAndBacklog().isAvailable()).isTrue();
        assertThat(reader1.isRegisteredAsAvailable()).isFalse();

        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        assertThat(channel.isWritable()).isFalse();

        reader1.notifyDataAvailable(view1);
        channel.runPendingTasks();

        ResultSubpartitionView view2 = new DefaultBufferResultSubpartitionView(buffersToWrite);
        reader2.notifyDataAvailable(view2);
        reader2.notifySubpartitionsCreated(
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view2)
                        .build(),
                new ResultSubpartitionIndexSet(0));
        assertThat(reader2.getAvailabilityAndBacklog().isAvailable()).isTrue();
        assertThat(reader2.isRegisteredAsAvailable()).isFalse();

        reader2.notifyDataAvailable(view2);

        // changing a channel writability should result in draining both reader1 and reader2
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
        channel.runPendingTasks();
        assertThat(channel.outboundMessages()).hasSize(buffersToWrite);
    }

    /** Tests {@link PartitionRequestQueue} buffer writing with default buffers. */
    @Test
    void testDefaultBufferWriting() throws Exception {
        testBufferWriting(new DefaultBufferResultSubpartitionView(1));
    }

    /** Tests {@link PartitionRequestQueue} buffer writing with read-only buffers. */
    @Test
    void testReadOnlyBufferWriting() throws Exception {
        testBufferWriting(new ReadOnlyBufferResultSubpartitionView(1));
    }

    private void testBufferWriting(ResultSubpartitionView view) throws IOException {
        // setup
        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        final InputChannelID receiverId = new InputChannelID();
        final PartitionRequestQueue queue = new PartitionRequestQueue();
        final CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, Integer.MAX_VALUE, queue);
        final EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));

        // notify about buffer availability and encode one buffer
        reader.notifyDataAvailable(view);

        channel.runPendingTasks();

        Object read = channel.readOutbound();
        assertThat(read).isNotNull();
        if (read instanceof NettyMessage.ErrorResponse) {
            ((NettyMessage.ErrorResponse) read).cause.printStackTrace();
        }
        assertThat(read).isInstanceOf(NettyMessage.BufferResponse.class);
        read = channel.readOutbound();
        assertThat(read).isNull();
    }

    private static class DefaultBufferResultSubpartitionView extends NoOpResultSubpartitionView {
        /** Number of buffer in the backlog to report with every {@link #getNextBuffer()} call. */
        private final AtomicInteger buffersInBacklog;

        private DefaultBufferResultSubpartitionView(int buffersInBacklog) {
            this.buffersInBacklog = new AtomicInteger(buffersInBacklog);
        }

        @Nullable
        @Override
        public BufferAndBacklog getNextBuffer() {
            int buffers = buffersInBacklog.decrementAndGet();
            return new BufferAndBacklog(
                    TestBufferFactory.createBuffer(10),
                    buffers,
                    buffers > 0 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.NONE,
                    0);
        }

        @Override
        public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
            int numBuffers = buffersInBacklog.get();
            return new AvailabilityWithBacklog(isCreditAvailable && numBuffers > 0, numBuffers);
        }
    }

    private static class ReadOnlyBufferResultSubpartitionView
            extends DefaultBufferResultSubpartitionView {
        private ReadOnlyBufferResultSubpartitionView(int buffersInBacklog) {
            super(buffersInBacklog);
        }

        @Nullable
        @Override
        public BufferAndBacklog getNextBuffer() {
            BufferAndBacklog nextBuffer = super.getNextBuffer();
            return new BufferAndBacklog(
                    nextBuffer.buffer().readOnlySlice(),
                    nextBuffer.buffersInBacklog(),
                    nextBuffer.getNextDataType(),
                    0);
        }
    }

    private static class EmptyAlwaysAvailableResultSubpartitionView
            extends NoOpResultSubpartitionView {
        @Override
        public boolean isReleased() {
            return false;
        }

        @Override
        public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
            return new AvailabilityWithBacklog(true, 0);
        }
    }

    /**
     * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
     * verifying the reader would be enqueued in the pipeline if the next sending buffer is an event
     * even though it has no available credits.
     */
    @Test
    void testEnqueueReaderByNotifyingEventBuffer() throws Exception {
        // setup
        final ResultSubpartitionView view = new NextIsEventResultSubpartitionView();

        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        final InputChannelID receiverId = new InputChannelID();
        final PartitionRequestQueue queue = new PartitionRequestQueue();
        final CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
        final EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));

        // block the channel so that we see an intermediate state in the test
        ByteBuf channelBlockingBuffer = blockChannel(channel);
        assertThat((Object) channel.readOutbound()).isNull();

        // Notify an available event buffer to trigger enqueue the reader
        reader.notifyDataAvailable(view);

        channel.runPendingTasks();

        // The reader is enqueued in the pipeline because the next buffer is an event, even though
        // no credits are available
        assertThat(queue.getAvailableReaders()).contains(reader); // contains only (this) one!
        assertThat(reader.getNumCreditsAvailable()).isZero();

        // Flush the buffer to make the channel writable again and see the final results
        channel.flush();
        assertThat((ByteBuf) channel.readOutbound()).isSameAs(channelBlockingBuffer);

        assertThat(queue.getAvailableReaders()).isEmpty();
        assertThat(reader.getNumCreditsAvailable()).isZero();
        assertThat((Object) channel.readOutbound()).isNull();
    }

    private static class NextIsEventResultSubpartitionView extends NoOpResultSubpartitionView {
        @Override
        public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
            return new AvailabilityWithBacklog(true, 0);
        }
    }

    /**
     * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
     * verifying the reader would be enqueued in the pipeline iff it has both available credits and
     * buffers.
     */
    @Test
    void testEnqueueReaderByNotifyingBufferAndCredit() throws Exception {
        // setup
        final ResultSubpartitionView view = new DefaultBufferResultSubpartitionView(10);

        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        final InputChannelID receiverId = new InputChannelID();
        final PartitionRequestQueue queue = new PartitionRequestQueue();
        final CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 2, queue);
        final EmbeddedChannel channel = new EmbeddedChannel(queue);
        reader.addCredit(-2);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));
        queue.notifyReaderCreated(reader);

        // block the channel so that we see an intermediate state in the test
        ByteBuf channelBlockingBuffer = blockChannel(channel);
        assertThat((Object) channel.readOutbound()).isNull();

        // Notify available buffers to trigger enqueue the reader
        final int notifyNumBuffers = 5;
        for (int i = 0; i < notifyNumBuffers; i++) {
            reader.notifyDataAvailable(view);
        }

        channel.runPendingTasks();

        // the reader is not enqueued in the pipeline because no credits are available
        // -> it should still have the same number of pending buffers
        assertThat(queue.getAvailableReaders()).isEmpty();
        assertThat(reader.hasBuffersAvailable().isAvailable()).isTrue();
        assertThat(reader.isRegisteredAsAvailable()).isFalse();
        assertThat(reader.getNumCreditsAvailable()).isZero();

        // Notify available credits to trigger enqueue the reader again
        final int notifyNumCredits = 3;
        for (int i = 1; i <= notifyNumCredits; i++) {
            queue.addCreditOrResumeConsumption(receiverId, viewReader -> viewReader.addCredit(1));

            // the reader is enqueued in the pipeline because it has both available buffers and
            // credits
            // since the channel is blocked though, we will not process anything and only enqueue
            // the
            // reader once
            assertThat(reader.isRegisteredAsAvailable()).isTrue();
            assertThat(queue.getAvailableReaders()).contains(reader); // contains only (this) one!
            assertThat(reader.getNumCreditsAvailable()).isEqualTo(i);
            assertThat(reader.hasBuffersAvailable().isAvailable()).isTrue();
        }

        // Flush the buffer to make the channel writable again and see the final results
        channel.flush();
        assertThat((ByteBuf) channel.readOutbound()).isSameAs(channelBlockingBuffer);

        assertThat(queue.getAvailableReaders()).isEmpty();
        assertThat(reader.getNumCreditsAvailable()).isZero();
        assertThat(reader.hasBuffersAvailable().isAvailable()).isTrue();
        assertThat(reader.isRegisteredAsAvailable()).isFalse();
        for (int i = 1; i <= notifyNumCredits; i++) {
            assertThat((Object) channel.readOutbound())
                    .isInstanceOf(NettyMessage.BufferResponse.class);
        }
        assertThat((Object) channel.readOutbound()).isNull();
    }

    /**
     * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
     * verifying the reader would be enqueued in the pipeline after resuming data consumption if
     * there are credit and data available.
     */
    @Test
    void testEnqueueReaderByResumingConsumption() throws Exception {
        PipelinedSubpartition subpartition =
                PipelinedSubpartitionTest.createPipelinedSubpartition();
        Buffer.DataType dataType1 = Buffer.DataType.ALIGNED_CHECKPOINT_BARRIER;
        Buffer.DataType dataType2 = Buffer.DataType.DATA_BUFFER;
        subpartition.add(createEventBufferConsumer(4096, dataType1));
        subpartition.add(createEventBufferConsumer(4096, dataType2));

        BufferAvailabilityListener bufferAvailabilityListener = new NoOpBufferAvailablityListener();
        PipelinedSubpartitionView view = subpartition.createReadView(bufferAvailabilityListener);
        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        InputChannelID receiverId = new InputChannelID();
        PartitionRequestQueue queue = new PartitionRequestQueue();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 2, queue);
        EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));
        queue.notifyReaderCreated(reader);
        assertThat(reader.getAvailabilityAndBacklog().isAvailable()).isTrue();

        reader.notifyDataAvailable(view);
        channel.runPendingTasks();
        assertThat(reader.getAvailabilityAndBacklog().isAvailable()).isFalse();
        assertThat(subpartition.unsynchronizedGetNumberOfQueuedBuffers()).isOne();

        queue.addCreditOrResumeConsumption(
                receiverId, NetworkSequenceViewReader::resumeConsumption);
        assertThat(reader.getAvailabilityAndBacklog().isAvailable()).isFalse();
        assertThat(subpartition.unsynchronizedGetNumberOfQueuedBuffers()).isZero();

        Object data1 = channel.readOutbound();
        assertThat(((NettyMessage.BufferResponse) data1).buffer.getDataType()).isEqualTo(dataType1);
        Object data2 = channel.readOutbound();
        assertThat(((NettyMessage.BufferResponse) data2).buffer.getDataType()).isEqualTo(dataType2);
    }

    @Test
    void testAnnounceBacklog() throws Exception {
        PipelinedSubpartition subpartition =
                PipelinedSubpartitionTest.createPipelinedSubpartition();
        subpartition.add(createEventBufferConsumer(4096, Buffer.DataType.DATA_BUFFER));
        subpartition.add(createEventBufferConsumer(4096, Buffer.DataType.DATA_BUFFER));

        PipelinedSubpartitionView view =
                subpartition.createReadView(new NoOpBufferAvailablityListener());
        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        PartitionRequestQueue queue = new PartitionRequestQueue();
        InputChannelID receiverId = new InputChannelID();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
        EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));
        queue.notifyReaderCreated(reader);

        reader.notifyDataAvailable(view);
        channel.runPendingTasks();
        Object data = channel.readOutbound();
        assertThat(data).isInstanceOf(NettyMessage.BacklogAnnouncement.class);
        NettyMessage.BacklogAnnouncement announcement = (NettyMessage.BacklogAnnouncement) data;
        assertThat(announcement.receiverId).isEqualTo(receiverId);
        assertThat(announcement.backlog).isEqualTo(subpartition.getBuffersInBacklogUnsafe());

        subpartition.release();
        reader.notifyDataAvailable(view);
        channel.runPendingTasks();
        assertThat((Object) channel.readOutbound()).isNotNull();
    }

    @Test
    void testCancelPartitionRequestForUnavailableView() throws Exception {
        testCancelPartitionRequest(false);
    }

    @Test
    void testCancelPartitionRequestForAvailableView() throws Exception {
        testCancelPartitionRequest(true);
    }

    private void testCancelPartitionRequest(boolean isAvailableView) throws Exception {
        // setup
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createFinishedPartitionWithFilledData(partitionManager);
        final InputChannelID receiverId = new InputChannelID();
        final PartitionRequestQueue queue = new PartitionRequestQueue();
        final CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 2, queue);
        final EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));
        // add this reader into allReaders queue
        queue.notifyReaderCreated(reader);

        // block the channel so that we see an intermediate state in the test
        blockChannel(channel);

        // add credit to make this reader available for adding into availableReaders queue
        if (isAvailableView) {
            queue.addCreditOrResumeConsumption(receiverId, viewReader -> viewReader.addCredit(1));
            assertThat(queue.getAvailableReaders()).contains(reader);
        }

        // cancel this subpartition view
        queue.cancel(receiverId);
        channel.runPendingTasks();

        assertThat(queue.getAvailableReaders()).doesNotContain(reader);

        // the reader view should be released (the partition is not, though, blocking partitions
        // support multiple successive readers for recovery and caching)
        assertThat(reader.isReleased()).isTrue();

        // cleanup
        partition.release();
        channel.close();
    }

    @Test
    void testNotifyNewBufferSize() throws Exception {
        // given: Result partition and the reader for subpartition 0.
        ResultPartition parent = createResultPartition();

        BufferAvailabilityListener bufferAvailabilityListener = new NoOpBufferAvailablityListener();
        ResultSubpartitionView view =
                parent.createSubpartitionView(
                        new ResultSubpartitionIndexSet(0), bufferAvailabilityListener);
        ResultPartition partition =
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        InputChannelID receiverId = new InputChannelID();
        PartitionRequestQueue queue = new PartitionRequestQueue();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(receiverId, 2, queue);
        EmbeddedChannel channel = new EmbeddedChannel(queue);

        reader.notifySubpartitionsCreated(partition, new ResultSubpartitionIndexSet(0));
        queue.notifyReaderCreated(reader);

        // when: New buffer size received.
        queue.notifyNewBufferSize(receiverId, 65);

        // and: New records emit.
        parent.emitRecord(ByteBuffer.allocate(128), 0);
        parent.emitRecord(ByteBuffer.allocate(10), 0);
        parent.emitRecord(ByteBuffer.allocate(60), 0);

        reader.notifyDataAvailable(view);
        channel.runPendingTasks();

        // then: Buffers of received size will be in outbound channel.
        Object data1 = channel.readOutbound();
        // The size can not be less than the first record in buffer.
        assertThat(((NettyMessage.BufferResponse) data1).buffer.getSize()).isEqualTo(128);
        Object data2 = channel.readOutbound();
        // The size should shrink up to notified buffer size.
        assertThat(((NettyMessage.BufferResponse) data2).buffer.getSize()).isEqualTo(65);
    }

    private static ResultPartition createResultPartition() throws IOException {
        NettyShuffleEnvironment network =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(10)
                        .setBufferSize(BUFFER_SIZE)
                        .build();
        ResultPartition resultPartition =
                createPartition(
                        network, NoOpFileChannelManager.INSTANCE, ResultPartitionType.PIPELINED, 2);
        resultPartition.setup();
        return resultPartition;
    }

    private static ResultPartition createFinishedPartitionWithFilledData(
            ResultPartitionManager partitionManager) throws Exception {
        NettyShuffleEnvironment environment =
                new NettyShuffleEnvironmentBuilder()
                        .setResultPartitionManager(partitionManager)
                        .build();
        ResultPartition partition =
                createPartition(environment, fileChannelManager, ResultPartitionType.BLOCKING, 1);

        partition.setup();
        partition.emitRecord(ByteBuffer.allocate(BUFFER_SIZE), 0);
        partition.finish();

        return partition;
    }

    /**
     * Blocks the given channel by adding a buffer that is bigger than the high watermark.
     *
     * <p>The channel may be unblocked with:
     *
     * <pre>
     * channel.flush();
     * assertSame(channelBlockingBuffer, channel.readOutbound());
     * </pre>
     *
     * @param channel the channel to block
     * @return the created blocking buffer
     */
    static ByteBuf blockChannel(EmbeddedChannel channel) {
        final int highWaterMark = channel.config().getWriteBufferHighWaterMark();
        // Set the writer index to the high water mark to ensure that all bytes are written
        // to the wire although the buffer is "empty".
        ByteBuf channelBlockingBuffer = Unpooled.buffer(highWaterMark).writerIndex(highWaterMark);
        channel.write(channelBlockingBuffer);
        assertThat(channel.isWritable()).isFalse();

        return channelBlockingBuffer;
    }
}
