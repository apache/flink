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

import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.apache.flink.util.Preconditions.checkNotNull;
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

			final ResultSubpartitionView view = spy(new InfiniteSubpartitionView(outboundBuffers, sync));

			// Return infinite subpartition
			when(partitions.createSubpartitionView(eq(pid), eq(0), any(BufferAvailabilityListener.class)))
				.thenAnswer(new Answer<ResultSubpartitionView>() {
					@Override
					public ResultSubpartitionView answer(InvocationOnMock invocationOnMock) throws Throwable {
						BufferAvailabilityListener listener = (BufferAvailabilityListener) invocationOnMock.getArguments()[2];
						listener.notifyDataAvailable();
						return view;
					}
				});

			NettyProtocol protocol = new NettyProtocol(partitions, mock(TaskEventDispatcher.class));

			serverAndClient = initServerAndClient(protocol);

			Channel ch = connect(serverAndClient);

			// Request for non-existing input channel => results in cancel request
			ch.writeAndFlush(new PartitionRequest(pid, 0, new InputChannelID(), Integer.MAX_VALUE)).await();

			// Wait for the notification
			if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
				fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
						" ms to be notified about cancelled partition.");
			}

			verify(view, times(1)).releaseAllResources();
		}
		finally {
			shutdown(serverAndClient);
		}
	}

	@Test
	public void testDuplicateCancel() throws Exception {

		NettyServerAndClient serverAndClient = null;

		try {
			final TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);

			ResultPartitionManager partitions = mock(ResultPartitionManager.class);

			ResultPartitionID pid = new ResultPartitionID();

			final CountDownLatch sync = new CountDownLatch(1);

			final ResultSubpartitionView view = spy(new InfiniteSubpartitionView(outboundBuffers, sync));

			// Return infinite subpartition
			when(partitions.createSubpartitionView(eq(pid), eq(0), any(BufferAvailabilityListener.class)))
					.thenAnswer(new Answer<ResultSubpartitionView>() {
						@Override
						public ResultSubpartitionView answer(InvocationOnMock invocationOnMock) throws Throwable {
							BufferAvailabilityListener listener = (BufferAvailabilityListener) invocationOnMock.getArguments()[2];
							listener.notifyDataAvailable();
							return view;
						}
					});

			NettyProtocol protocol = new NettyProtocol(partitions, mock(TaskEventDispatcher.class));

			serverAndClient = initServerAndClient(protocol);

			Channel ch = connect(serverAndClient);

			// Request for non-existing input channel => results in cancel request
			InputChannelID inputChannelId = new InputChannelID();

			ch.writeAndFlush(new PartitionRequest(pid, 0, inputChannelId, Integer.MAX_VALUE)).await();

			// Wait for the notification
			if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
				fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
						" ms to be notified about cancelled partition.");
			}

			ch.writeAndFlush(new CancelPartitionRequest(inputChannelId)).await();

			ch.close();

			NettyTestUtil.awaitClose(ch);

			verify(view, times(1)).releaseAllResources();
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

		@Nullable
		@Override
		public BufferAndBacklog getNextBuffer() throws IOException {
			Buffer buffer = bufferProvider.requestBuffer();
			if (buffer != null) {
				buffer.setSize(buffer.getMaxCapacity()); // fake some data
				return new BufferAndBacklog(buffer, true, 0, false);
			} else {
				return null;
			}
		}

		@Override
		public void notifyDataAvailable() {
		}

		@Override
		public void releaseAllResources() throws IOException {
			sync.countDown();
		}

		@Override
		public boolean isReleased() {
			return false;
		}

		@Override
		public boolean nextBufferIsEvent() {
			return false;
		}

		@Override
		public boolean isAvailable() {
			return true;
		}

		@Override
		public int unsynchronizedGetNumberOfQueuedBuffers() {
			return 0;
		}

		@Override
		public Throwable getFailureCause() {
			return null;
		}
	}
}
