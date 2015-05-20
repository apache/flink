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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.CancelPartitionRequestTest.InfiniteSubpartitionView;
import org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.createConfig;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerTransportErrorHandlingTest {

	/**
	 * Verifies remote closes trigger the release of all resources.
	 */
	@Test
	public void testRemoteClose() throws Exception {
		final TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);

		final CountDownLatch sync = new CountDownLatch(1);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

		when(partitionManager
				.createSubpartitionView(any(ResultPartitionID.class), anyInt(), any(BufferProvider.class)))
				.thenReturn(new InfiniteSubpartitionView(outboundBuffers, sync));

		NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new PartitionRequestProtocol(
						partitionManager,
						mock(TaskEventDispatcher.class),
						mock(NetworkBufferPool.class)).getServerChannelHandlers();
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[] {
						new NettyMessageEncoder(),
						// Close on read
						new ChannelInboundHandlerAdapter() {
							@Override
							public void channelRead(ChannelHandlerContext ctx, Object msg)
									throws Exception {

								ctx.channel().close();
							}
						}
				};
			}
		};

		NettyServerAndClient serverAndClient = null;

		try {
			serverAndClient = initServerAndClient(protocol, createConfig());

			Channel ch = connect(serverAndClient);

			// Write something to trigger close by server
			ch.writeAndFlush(new PartitionRequest(new ResultPartitionID(), 0, new InputChannelID()));

			// Wait for the notification
			if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
				fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
						" ms to be notified about released partition.");
			}
		}
		finally {
			shutdown(serverAndClient);
		}
	}
}
