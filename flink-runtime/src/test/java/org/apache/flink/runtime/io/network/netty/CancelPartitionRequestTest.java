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

import io.netty.channel.Channel;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CancelPartitionRequestTest {

	/**
	 * Verifies that requests for non-existing (failed/cancelled) input channels are properly
	 * cancelled. The receiver receives data, but there is no input channel to receive the data.
	 * This should cancel the request.
	 */
	@Test
	public void testCancelPartitionRequest() throws Exception {

		NettyServerAndClient serverAndClient = null;

		try {
			TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);

			ResultPartitionManager partitions = mock(ResultPartitionManager.class);

			ResultPartitionID pid = new ResultPartitionID();

			CountDownLatch sync = new CountDownLatch(1);

			ResultSubpartitionView view = spy(new InfiniteSubpartitionView(outboundBuffers, sync));

			// Return infinite subpartition
			when(partitions.createSubpartitionView(eq(pid), eq(0), any(BufferProvider.class)))
					.thenReturn(view);

			PartitionRequestProtocol protocol = new PartitionRequestProtocol(
					partitions, mock(TaskEventDispatcher.class), mock(NetworkBufferPool.class));

			serverAndClient = initServerAndClient(protocol);

			Channel ch = connect(serverAndClient);

			// Request for non-existing input channel => results in cancel request
			ch.writeAndFlush(new PartitionRequest(pid, 0, new InputChannelID())).await();

			// Wait for the notification
			if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
				fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
						" ms to be notified about cancelled partition.");
			}

			verify(view, times(1)).releaseAllResources();
			verify(view, times(0)).notifySubpartitionConsumed();
		}
		finally {
			shutdown(serverAndClient);
		}
	}

	@Test
	public void testDuplicateCancel() throws Exception {

		NettyServerAndClient serverAndClient = null;

		try {
			TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);

			ResultPartitionManager partitions = mock(ResultPartitionManager.class);

			ResultPartitionID pid = new ResultPartitionID();

			CountDownLatch sync = new CountDownLatch(1);

			ResultSubpartitionView view = spy(new InfiniteSubpartitionView(outboundBuffers, sync));

			// Return infinite subpartition
			when(partitions.createSubpartitionView(eq(pid), eq(0), any(BufferProvider.class)))
					.thenReturn(view);

			PartitionRequestProtocol protocol = new PartitionRequestProtocol(
					partitions, mock(TaskEventDispatcher.class), mock(NetworkBufferPool.class));

			serverAndClient = initServerAndClient(protocol);

			Channel ch = connect(serverAndClient);

			// Request for non-existing input channel => results in cancel request
			InputChannelID inputChannelId = new InputChannelID();

			ch.writeAndFlush(new PartitionRequest(pid, 0, inputChannelId)).await();

			// Wait for the notification
			if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
				fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
						" ms to be notified about cancelled partition.");
			}

			ch.writeAndFlush(new CancelPartitionRequest(inputChannelId)).await();

			ch.close();

			NettyTestUtil.awaitClose(ch);

			verify(view, times(1)).releaseAllResources();
			verify(view, times(0)).notifySubpartitionConsumed();
		}
		finally {
			shutdown(serverAndClient);
		}
	}

	// ---------------------------------------------------------------------------------------------

	static class InfiniteSubpartitionView implements ResultSubpartitionView {

		private final BufferProvider bufferProvider;

		private final CountDownLatch sync;

		public InfiniteSubpartitionView(BufferProvider bufferProvider, CountDownLatch sync) {
			this.bufferProvider = checkNotNull(bufferProvider);
			this.sync = checkNotNull(sync);
		}

		@Override
		public Buffer getNextBuffer() throws IOException, InterruptedException {
			return bufferProvider.requestBufferBlocking();
		}

		@Override
		public boolean registerListener(final NotificationListener listener) throws IOException {
			return false;
		}

		@Override
		public void releaseAllResources() throws IOException {
			sync.countDown();
		}

		@Override
		public void notifySubpartitionConsumed() throws IOException {
		}

		@Override
		public boolean isReleased() {
			return false;
		}

		@Override
		public Throwable getFailureCause() {
			return null;
		}
	}
}
