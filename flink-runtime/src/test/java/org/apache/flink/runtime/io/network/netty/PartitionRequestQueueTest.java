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
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PartitionRequestQueue}.
 */
public class PartitionRequestQueueTest {

	@Test
	public void testProducerFailedException() throws Exception {
		PartitionRequestQueue queue = new PartitionRequestQueue();

		ResultPartitionProvider partitionProvider = mock(ResultPartitionProvider.class);
		ResultPartitionID rpid = new ResultPartitionID();

		ResultSubpartitionView view = mock(ResultSubpartitionView.class);
		when(view.isReleased()).thenReturn(true);
		when(view.getFailureCause()).thenReturn(new RuntimeException("Expected test exception"));

		when(partitionProvider.createSubpartitionView(
			eq(rpid),
			eq(0),
			any(BufferAvailabilityListener.class))).thenReturn(view);

		EmbeddedChannel ch = new EmbeddedChannel(queue);

		SequenceNumberingViewReader seqView = new SequenceNumberingViewReader(new InputChannelID(), queue);
		seqView.requestSubpartitionView(partitionProvider, rpid, 0);

		// Enqueue the erroneous view
		queue.notifyReaderNonEmpty(seqView);
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
		public ResultSubpartition.BufferAndBacklog getNextBuffer() {
			return new ResultSubpartition.BufferAndBacklog(
				TestBufferFactory.createBuffer(10),
				buffersInBacklog);
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
		public ResultSubpartition.BufferAndBacklog getNextBuffer() {
			return new ResultSubpartition.BufferAndBacklog(
				TestBufferFactory.createBuffer(10).readOnlySlice(),
				buffersInBacklog);
		}
	}

	private static class NoOpResultSubpartitionView implements ResultSubpartitionView {
		@Nullable
		public ResultSubpartition.BufferAndBacklog getNextBuffer() {
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
	}
}
