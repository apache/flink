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
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;

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
		seqView.notifyBuffersAvailable(1);

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
		testBufferWriting(new DefaultBufferResultSubpartitionView(2));
	}

	/**
	 * Tests {@link PartitionRequestQueue} buffer writing with read-only buffers.
	 */
	@Test
	public void testReadOnlyBufferWriting() throws Exception {
		testBufferWriting(new ReadOnlyBufferResultSubpartitionView(2));
	}

	private void testBufferWriting(ResultSubpartitionView view) throws IOException {
		// setup
		ResultPartitionProvider partitionProvider =
			(partitionId, index, availabilityListener) -> view;

		final InputChannelID receiverId = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final SequenceNumberingViewReader reader = new SequenceNumberingViewReader(receiverId, queue);
		final EmbeddedChannel channel = new EmbeddedChannel(queue);

		reader.requestSubpartitionView(partitionProvider, new ResultPartitionID(), 0);

		// notify about buffer availability and encode one buffer
		reader.notifyBuffersAvailable(1);

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
		final int buffersInBacklog;

		private DefaultBufferResultSubpartitionView(int buffersInBacklog) {
			this.buffersInBacklog = buffersInBacklog;
		}

		@Nullable
		@Override
		public BufferAndBacklog getNextBuffer() {
			return new BufferAndBacklog(
				TestBufferFactory.createBuffer(10),
				buffersInBacklog,
				false);
		}
	}

	private static class ReadOnlyBufferResultSubpartitionView extends NoOpResultSubpartitionView {
		/** Number of buffer in the backlog to report with every {@link #getNextBuffer()} call. */
		final int buffersInBacklog;

		private ReadOnlyBufferResultSubpartitionView(int buffersInBacklog) {
			this.buffersInBacklog = buffersInBacklog;
		}

		@Nullable
		@Override
		public BufferAndBacklog getNextBuffer() {
			return new BufferAndBacklog(
				TestBufferFactory.createBuffer(10).readOnlySlice(),
				buffersInBacklog,
				false);
		}
	}

	private static class ReleasedResultSubpartitionView extends NoOpResultSubpartitionView {
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
		reader.notifyBuffersAvailable(1);

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
		public boolean nextBufferIsEvent() {
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
		final ResultSubpartitionView view = new DefaultBufferResultSubpartitionView(2);

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
			reader.notifyBuffersAvailable(1);
		}

		channel.runPendingTasks();

		// the reader is not enqueued in the pipeline because no credits are available
		// -> it should still have the same number of pending buffers
		assertEquals(0, queue.getAvailableReaders().size());
		assertEquals(notifyNumBuffers, reader.getNumBuffersAvailable());
		assertFalse(reader.isRegisteredAsAvailable());
		assertEquals(0, reader.getNumCreditsAvailable());

		// Notify available credits to trigger enqueue the reader again
		final int notifyNumCredits = 3;
		for (int i = 1; i <= notifyNumCredits; i++) {
			queue.addCredit(receiverId, 1);

			// the reader is enqueued in the pipeline because it has both available buffers and credits
			// since the channel is blocked though, we will not process anything and only enqueue the
			// reader once
			assertTrue(reader.isRegisteredAsAvailable());
			assertThat(queue.getAvailableReaders(), contains(reader)); // contains only (this) one!
			assertEquals(i, reader.getNumCreditsAvailable());
			assertEquals(notifyNumBuffers, reader.getNumBuffersAvailable());
		}

		// Flush the buffer to make the channel writable again and see the final results
		channel.flush();
		assertSame(channelBlockingBuffer, channel.readOutbound());

		assertEquals(0, queue.getAvailableReaders().size());
		assertEquals(0, reader.getNumCreditsAvailable());
		assertEquals(notifyNumBuffers - notifyNumCredits, reader.getNumBuffersAvailable());
		assertFalse(reader.isRegisteredAsAvailable());
		for (int i = 1; i <= notifyNumCredits; i++) {
			assertThat(channel.readOutbound(), instanceOf(NettyMessage.BufferResponse.class));
		}
		assertNull(channel.readOutbound());
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

	private static class NoOpResultSubpartitionView implements ResultSubpartitionView {
		@Nullable
		public BufferAndBacklog getNextBuffer() {
			return null;
		}

		@Override
		public void notifyBuffersAvailable(long buffers) {
		}

		@Override
		public void releaseAllResources() {
		}

		@Override
		public void notifySubpartitionConsumed() {
		}

		@Override
		public boolean isReleased() {
			return true;
		}

		@Override
		public Throwable getFailureCause() {
			return null;
		}

		@Override
		public boolean nextBufferIsEvent() {
			return false;
		}
	}
}
