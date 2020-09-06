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

import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionTest;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PartitionRequestQueue}.
 */
public class PartitionRequestQueueTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final int BUFFER_SIZE = 1024 * 1024;

	private static FileChannelManager fileChannelManager;

	@BeforeClass
	public static void setUp() throws Exception {
		fileChannelManager = new FileChannelManagerImpl(
			new String[] {TEMPORARY_FOLDER.newFolder().getAbsolutePath()}, "testing");
	}

	@AfterClass
	public static void shutdown() throws Exception {
		fileChannelManager.close();
	}

	/**
	 * In case of enqueuing an empty reader and a reader that actually has some buffers when channel is not writable,
	 * on channelWritability change event should result in reading all of the messages.
	 */
	@Test
	public void testNotifyReaderNonEmptyOnEmptyReaders() throws Exception {
		final int buffersToWrite = 5;
		PartitionRequestQueue queue = new PartitionRequestQueue();
		EmbeddedChannel channel = new EmbeddedChannel(queue);

		CreditBasedSequenceNumberingViewReader reader1 = new CreditBasedSequenceNumberingViewReader(new InputChannelID(0, 0), 10, queue);
		CreditBasedSequenceNumberingViewReader reader2 = new CreditBasedSequenceNumberingViewReader(new InputChannelID(1, 1), 10, queue);

		reader1.requestSubpartitionView((partitionId, index, availabilityListener) -> new EmptyAlwaysAvailableResultSubpartitionView(), new ResultPartitionID(), 0);
		reader1.notifyDataAvailable();
		assertTrue(reader1.isAvailable());
		assertFalse(reader1.isRegisteredAsAvailable());

		channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
		assertFalse(channel.isWritable());

		reader1.notifyDataAvailable();
		channel.runPendingTasks();

		reader2.notifyDataAvailable();
		reader2.requestSubpartitionView((partitionId, index, availabilityListener) -> new DefaultBufferResultSubpartitionView(buffersToWrite), new ResultPartitionID(), 0);
		assertTrue(reader2.isAvailable());
		assertFalse(reader2.isRegisteredAsAvailable());

		reader2.notifyDataAvailable();

		// changing a channel writability should result in draining both reader1 and reader2
		channel.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
		channel.runPendingTasks();
		assertEquals(buffersToWrite, channel.outboundMessages().size());
	}

	@Test
	public void testProducerFailedException() throws Exception {
		PartitionRequestQueue queue = new PartitionRequestQueue();

		ResultSubpartitionView view = new ReleasedResultSubpartitionView();

		ResultPartitionProvider partitionProvider =
			(partitionId, index, availabilityListener) -> view;

		EmbeddedChannel ch = new EmbeddedChannel(queue);

		CreditBasedSequenceNumberingViewReader seqView = new CreditBasedSequenceNumberingViewReader(new InputChannelID(), 2, queue);
		seqView.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);
		// Add available buffer to trigger enqueue the erroneous view
		seqView.notifyDataAvailable();

		ch.runPendingTasks();

		// Read the enqueued msg
		Object msg = ch.readOutbound();

		assertEquals(msg.getClass(), NettyMessage.ErrorResponse.class);

		NettyMessage.ErrorResponse err = (NettyMessage.ErrorResponse) msg;
		assertTrue(err.cause instanceof CancelTaskException);
	}

	/**
	 * Tests {@link PartitionRequestQueue} buffer writing with default buffers.
	 */
	@Test
	public void testDefaultBufferWriting() throws Exception {
		testBufferWriting(new DefaultBufferResultSubpartitionView(1));
	}

	/**
	 * Tests {@link PartitionRequestQueue} buffer writing with read-only buffers.
	 */
	@Test
	public void testReadOnlyBufferWriting() throws Exception {
		testBufferWriting(new ReadOnlyBufferResultSubpartitionView(1));
	}

	private void testBufferWriting(ResultSubpartitionView view) throws IOException {
		// setup
		ResultPartitionProvider partitionProvider =
			(partitionId, index, availabilityListener) -> view;

		final InputChannelID receiverId = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final CreditBasedSequenceNumberingViewReader reader = new CreditBasedSequenceNumberingViewReader(
			receiverId,
			Integer.MAX_VALUE,
			queue);
		final EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);

		// notify about buffer availability and encode one buffer
		reader.notifyDataAvailable();

		channel.runPendingTasks();

		Object read = channel.readOutbound();
		assertNotNull(read);
		if (read instanceof NettyMessage.ErrorResponse) {
			((NettyMessage.ErrorResponse) read).cause.printStackTrace();
		}
		assertThat(read, instanceOf(NettyMessage.BufferResponse.class));
		read = channel.readOutbound();
		assertNull(read);
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
				buffers > 0,
				buffers,
				false);
		}

		@Override
		public boolean isAvailable(int numCreditsAvailable) {
			if (numCreditsAvailable > 0) {
				return buffersInBacklog.get() > 0;
			}

			return false;
		}
	}

	private static class ReadOnlyBufferResultSubpartitionView extends DefaultBufferResultSubpartitionView {
		private ReadOnlyBufferResultSubpartitionView(int buffersInBacklog) {
			super(buffersInBacklog);
		}

		@Nullable
		@Override
		public BufferAndBacklog getNextBuffer() {
			BufferAndBacklog nextBuffer = super.getNextBuffer();
			return new BufferAndBacklog(
				nextBuffer.buffer().readOnlySlice(),
				nextBuffer.isDataAvailable(),
				nextBuffer.buffersInBacklog(),
				nextBuffer.isEventAvailable());
		}
	}

	private static class EmptyAlwaysAvailableResultSubpartitionView extends NoOpResultSubpartitionView {
		@Override
		public boolean isReleased() {
			return false;
		}

		@Override
		public boolean isAvailable(int numCreditsAvailable) {
			return true;
		}
	}

	private static class ReleasedResultSubpartitionView extends EmptyAlwaysAvailableResultSubpartitionView {
		@Override
		public boolean isReleased() {
			return true;
		}

		@Override
		public Throwable getFailureCause() {
			return new RuntimeException("Expected test exception");
		}
	}

	/**
	 * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
	 * verifying the reader would be enqueued in the pipeline if the next sending buffer is an event
	 * even though it has no available credits.
	 */
	@Test
	public void testEnqueueReaderByNotifyingEventBuffer() throws Exception {
		// setup
		final ResultSubpartitionView view = new NextIsEventResultSubpartitionView();

		ResultPartitionProvider partitionProvider =
			(partitionId, index, availabilityListener) -> view;

		final InputChannelID receiverId = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final CreditBasedSequenceNumberingViewReader reader = new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
		final EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);

		// block the channel so that we see an intermediate state in the test
		ByteBuf channelBlockingBuffer = blockChannel(channel);
		assertNull(channel.readOutbound());

		// Notify an available event buffer to trigger enqueue the reader
		reader.notifyDataAvailable();

		channel.runPendingTasks();

		// The reader is enqueued in the pipeline because the next buffer is an event, even though no credits are available
		assertThat(queue.getAvailableReaders(), contains(reader)); // contains only (this) one!
		assertEquals(0, reader.getNumCreditsAvailable());

		// Flush the buffer to make the channel writable again and see the final results
		channel.flush();
		assertSame(channelBlockingBuffer, channel.readOutbound());

		assertEquals(0, queue.getAvailableReaders().size());
		assertEquals(0, reader.getNumCreditsAvailable());
		assertNull(channel.readOutbound());
	}

	private static class NextIsEventResultSubpartitionView extends NoOpResultSubpartitionView {
		@Override
		public boolean isAvailable(int numCreditsAvailable) {
			return true;
		}
	}

	/**
	 * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
	 * verifying the reader would be enqueued in the pipeline iff it has both available credits and buffers.
	 */
	@Test
	public void testEnqueueReaderByNotifyingBufferAndCredit() throws Exception {
		// setup
		final ResultSubpartitionView view = new DefaultBufferResultSubpartitionView(10);

		ResultPartitionProvider partitionProvider =
			(partitionId, index, availabilityListener) -> view;

		final InputChannelID receiverId = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final CreditBasedSequenceNumberingViewReader reader = new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
		final EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);
		queue.notifyReaderCreated(reader);

		// block the channel so that we see an intermediate state in the test
		ByteBuf channelBlockingBuffer = blockChannel(channel);
		assertNull(channel.readOutbound());

		// Notify available buffers to trigger enqueue the reader
		final int notifyNumBuffers = 5;
		for (int i = 0; i < notifyNumBuffers; i++) {
			reader.notifyDataAvailable();
		}

		channel.runPendingTasks();

		// the reader is not enqueued in the pipeline because no credits are available
		// -> it should still have the same number of pending buffers
		assertEquals(0, queue.getAvailableReaders().size());
		assertTrue(reader.hasBuffersAvailable());
		assertFalse(reader.isRegisteredAsAvailable());
		assertEquals(0, reader.getNumCreditsAvailable());

		// Notify available credits to trigger enqueue the reader again
		final int notifyNumCredits = 3;
		for (int i = 1; i <= notifyNumCredits; i++) {
			queue.addCreditOrResumeConsumption(receiverId, viewReader -> viewReader.addCredit(1));

			// the reader is enqueued in the pipeline because it has both available buffers and credits
			// since the channel is blocked though, we will not process anything and only enqueue the
			// reader once
			assertTrue(reader.isRegisteredAsAvailable());
			assertThat(queue.getAvailableReaders(), contains(reader)); // contains only (this) one!
			assertEquals(i, reader.getNumCreditsAvailable());
			assertTrue(reader.hasBuffersAvailable());
		}

		// Flush the buffer to make the channel writable again and see the final results
		channel.flush();
		assertSame(channelBlockingBuffer, channel.readOutbound());

		assertEquals(0, queue.getAvailableReaders().size());
		assertEquals(0, reader.getNumCreditsAvailable());
		assertTrue(reader.hasBuffersAvailable());
		assertFalse(reader.isRegisteredAsAvailable());
		for (int i = 1; i <= notifyNumCredits; i++) {
			assertThat(channel.readOutbound(), instanceOf(NettyMessage.BufferResponse.class));
		}
		assertNull(channel.readOutbound());
	}

	/**
	 * Tests {@link PartitionRequestQueue#enqueueAvailableReader(NetworkSequenceViewReader)},
	 * verifying the reader would be enqueued in the pipeline after resuming data consumption if there
	 * are credit and data available.
	 */
	@Test
	public void testEnqueueReaderByResumingConsumption() throws Exception {
		PipelinedSubpartition subpartition = PipelinedSubpartitionTest.createPipelinedSubpartition();
		Buffer.DataType dataType1 = Buffer.DataType.ALIGNED_EXACTLY_ONCE_CHECKPOINT_BARRIER;
		Buffer.DataType dataType2 = Buffer.DataType.DATA_BUFFER;
		subpartition.add(createEventBufferConsumer(4096, dataType1));
		subpartition.add(createEventBufferConsumer(4096, dataType2));

		BufferAvailabilityListener bufferAvailabilityListener = new NoOpBufferAvailablityListener();
		PipelinedSubpartitionView view = subpartition.createReadView(bufferAvailabilityListener);
		ResultPartitionProvider partitionProvider = (partitionId, index, availabilityListener) -> view;

		InputChannelID receiverId = new InputChannelID();
		PartitionRequestQueue queue = new PartitionRequestQueue();
		CreditBasedSequenceNumberingViewReader reader = new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
		EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);
		queue.notifyReaderCreated(reader);
		// we have adequate credits
		reader.addCredit(Integer.MAX_VALUE);
		assertTrue(reader.isAvailable());

		reader.notifyDataAvailable();
		channel.runPendingTasks();
		assertFalse(reader.isAvailable());
		assertEquals(1, subpartition.unsynchronizedGetNumberOfQueuedBuffers());

		queue.addCreditOrResumeConsumption(receiverId, NetworkSequenceViewReader::resumeConsumption);
		assertFalse(reader.isAvailable());
		assertEquals(0, subpartition.unsynchronizedGetNumberOfQueuedBuffers());

		Object data1 = channel.readOutbound();
		assertEquals(dataType1, ((NettyMessage.BufferResponse) data1).buffer.getDataType());
		Object data2 = channel.readOutbound();
		assertEquals(dataType2, ((NettyMessage.BufferResponse) data2).buffer.getDataType());
	}

	@Test
	public void testCancelPartitionRequestForUnavailableView() throws Exception {
		testCancelPartitionRequest(false);
	}

	@Test
	public void testCancelPartitionRequestForAvailableView() throws Exception {
		testCancelPartitionRequest(true);
	}

	private void testCancelPartitionRequest(boolean isAvailableView) throws Exception {
		// setup
		final ResultPartitionManager partitionManager = new ResultPartitionManager();
		final ResultPartition partition = createFinishedPartitionWithFilledData(partitionManager);
		final InputChannelID receiverId = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final CreditBasedSequenceNumberingViewReader reader = new CreditBasedSequenceNumberingViewReader(receiverId, 0, queue);
		final EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionManager, partition.getPartitionId(), 0);
		// add this reader into allReaders queue
		queue.notifyReaderCreated(reader);

		// block the channel so that we see an intermediate state in the test
		blockChannel(channel);

		// add credit to make this reader available for adding into availableReaders queue
		if (isAvailableView) {
			queue.addCreditOrResumeConsumption(receiverId, viewReader -> viewReader.addCredit(1));
			assertTrue(queue.getAvailableReaders().contains(reader));
		}

		// cancel this subpartition view
		queue.cancel(receiverId);
		channel.runPendingTasks();

		assertFalse(queue.getAvailableReaders().contains(reader));

		// the reader view should be released (the partition is not, though, blocking partitions
		// support multiple successive readers for recovery and caching)
		assertTrue(reader.isReleased());

		// cleanup
		partition.release();
		channel.close();
	}

	private static ResultPartition createFinishedPartitionWithFilledData(ResultPartitionManager partitionManager) throws Exception {
		final ResultPartition partition = new ResultPartitionBuilder()
			.setResultPartitionType(ResultPartitionType.BLOCKING)
			.setFileChannelManager(fileChannelManager)
			.setResultPartitionManager(partitionManager)
			.build();

		partitionManager.registerResultPartition(partition);
		PartitionTestUtils.writeBuffers(partition, 1, BUFFER_SIZE);

		return partition;
	}

	/**
	 * Blocks the given channel by adding a buffer that is bigger than the high watermark.
	 *
	 * <p>The channel may be unblocked with:
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
		assertFalse(channel.isWritable());

		return channelBlockingBuffer;
	}
}
