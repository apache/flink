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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.createConfig;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientTransportErrorHandlingTest {

	/**
	 * Verifies that failed client requests via {@link PartitionRequestClient} are correctly
	 * attributed to the respective {@link RemoteInputChannel}.
	 */
	@Test
	public void testExceptionOnWrite() throws Exception {

		NettyProtocol protocol = new NettyProtocol(
				mock(ResultPartitionProvider.class),
				mock(TaskEventDispatcher.class),
				true) {

			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		// We need a real server and client in this test, because Netty's EmbeddedChannel is
		// not failing the ChannelPromise of failed writes.
		NettyServerAndClient serverAndClient = initServerAndClient(protocol, createConfig());

		Channel ch = connect(serverAndClient);

		NetworkClientHandler handler = getClientHandler(ch);

		// Last outbound handler throws Exception after 1st write
		ch.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
			int writeNum = 0;

			@Override
			public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
					throws Exception {

				if (writeNum >= 1) {
					throw new RuntimeException("Expected test exception.");
				}

				writeNum++;
				ctx.write(msg, promise);
			}
		});

		PartitionRequestClient requestClient = new NettyPartitionRequestClient(
				ch, handler, mock(ConnectionID.class), mock(PartitionRequestClientFactory.class));

		// Create input channels
		RemoteInputChannel[] rich = new RemoteInputChannel[] {
				createRemoteInputChannel(), createRemoteInputChannel()};

		final CountDownLatch sync = new CountDownLatch(1);

		// Do this with explicit synchronization. Otherwise this is not robust against slow timings
		// of the callback (e.g. we cannot just verify that it was called once, because there is
		// a chance that we do this too early).
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				sync.countDown();
				return null;
			}
		}).when(rich[1]).onError(isA(LocalTransportException.class));

		// First request is successful
		requestClient.requestSubpartition(new ResultPartitionID(), 0, rich[0], 0);

		// Second request is *not* successful
		requestClient.requestSubpartition(new ResultPartitionID(), 0, rich[1], 0);

		// Wait for the notification and it could confirm all the request operations are done
		if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
			fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
					" ms to be notified about the channel error.");
		}

		// Only the second channel should be notified about the error
		verify(rich[0], times(0)).onError(any(LocalTransportException.class));

		shutdown(serverAndClient);
	}

	/**
	 * Verifies that {@link NettyMessage.ErrorResponse} messages are correctly wrapped in
	 * {@link RemoteTransportException} instances.
	 */
	@Test
	public void testWrappingOfRemoteErrorMessage() throws Exception {
		EmbeddedChannel ch = createEmbeddedChannel();

		NetworkClientHandler handler = getClientHandler(ch);

		// Create input channels
		RemoteInputChannel[] rich = new RemoteInputChannel[] {
				createRemoteInputChannel(), createRemoteInputChannel()};

		for (RemoteInputChannel r : rich) {
			when(r.getInputChannelId()).thenReturn(new InputChannelID());
			handler.addInputChannel(r);
		}

		// Error msg for channel[0]
		ch.pipeline().fireChannelRead(new NettyMessage.ErrorResponse(
				new RuntimeException("Expected test exception"),
				rich[0].getInputChannelId()));

		try {
			// Exception should not reach end of pipeline...
			ch.checkException();
		}
		catch (Exception e) {
			fail("The exception reached the end of the pipeline and "
					+ "was not handled correctly by the last handler.");
		}

		verify(rich[0], times(1)).onError(isA(RemoteTransportException.class));
		verify(rich[1], never()).onError(any(Throwable.class));

		// Fatal error for all channels
		ch.pipeline().fireChannelRead(new NettyMessage.ErrorResponse(
				new RuntimeException("Expected test exception")));

		try {
			// Exception should not reach end of pipeline...
			ch.checkException();
		}
		catch (Exception e) {
			fail("The exception reached the end of the pipeline and "
					+ "was not handled correctly by the last handler.");
		}

		verify(rich[0], times(2)).onError(isA(RemoteTransportException.class));
		verify(rich[1], times(1)).onError(isA(RemoteTransportException.class));
	}

	/**
	 * Verifies that unexpected remote closes are reported as an instance of
	 * {@link RemoteTransportException}.
	 */
	@Test
	public void testExceptionOnRemoteClose() throws Exception {

		NettyProtocol protocol = new NettyProtocol(
				mock(ResultPartitionProvider.class),
				mock(TaskEventDispatcher.class),
				true) {

			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[] {
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

		NettyServerAndClient serverAndClient = initServerAndClient(protocol, createConfig());

		Channel ch = connect(serverAndClient);

		NetworkClientHandler handler = getClientHandler(ch);

		// Create input channels
		RemoteInputChannel[] rich = new RemoteInputChannel[] {
				createRemoteInputChannel(), createRemoteInputChannel()};

		final CountDownLatch sync = new CountDownLatch(rich.length);

		Answer<Void> countDownLatch = new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				sync.countDown();
				return null;
			}
		};

		for (RemoteInputChannel r : rich) {
			doAnswer(countDownLatch).when(r).onError(any(Throwable.class));
			handler.addInputChannel(r);
		}

		// Write something to trigger close by server
		ch.writeAndFlush(Unpooled.buffer().writerIndex(16));

		// Wait for the notification
		if (!sync.await(TestingUtils.TESTING_DURATION().toMillis(), TimeUnit.MILLISECONDS)) {
			fail("Timed out after waiting for " + TestingUtils.TESTING_DURATION().toMillis() +
					" ms to be notified about remote connection close.");
		}

		// All the registered channels should be notified.
		for (RemoteInputChannel r : rich) {
			verify(r).onError(isA(RemoteTransportException.class));
		}

		shutdown(serverAndClient);
	}

	/**
	 * Verifies that fired Exceptions are handled correctly by the pipeline.
	 */
	@Test
	public void testExceptionCaught() throws Exception {
		EmbeddedChannel ch = createEmbeddedChannel();

		NetworkClientHandler handler = getClientHandler(ch);

		// Create input channels
		RemoteInputChannel[] rich = new RemoteInputChannel[] {
				createRemoteInputChannel(), createRemoteInputChannel()};

		for (RemoteInputChannel r : rich) {
			when(r.getInputChannelId()).thenReturn(new InputChannelID());
			handler.addInputChannel(r);
		}

		ch.pipeline().fireExceptionCaught(new Exception());

		try {
			// Exception should not reach end of pipeline...
			ch.checkException();
		}
		catch (Exception e) {
			fail("The exception reached the end of the pipeline and "
					+ "was not handled correctly by the last handler.");
		}

		// ...but all the registered channels should be notified.
		for (RemoteInputChannel r : rich) {
			verify(r).onError(isA(LocalTransportException.class));
		}
	}

	/**
	 * Verifies that "Connection reset by peer" Exceptions are special-cased and are reported as
	 * an instance of {@link RemoteTransportException}.
	 */
	@Test
	public void testConnectionResetByPeer() throws Throwable {
		EmbeddedChannel ch = createEmbeddedChannel();

		NetworkClientHandler handler = getClientHandler(ch);

		RemoteInputChannel rich = addInputChannel(handler);

		final Throwable[] error = new Throwable[1];

		// Verify the Exception
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				Throwable cause = (Throwable) invocation.getArguments()[0];

				try {
					assertEquals(RemoteTransportException.class, cause.getClass());
					assertNotEquals("Connection reset by peer", cause.getMessage());

					assertEquals(IOException.class, cause.getCause().getClass());
					assertEquals("Connection reset by peer", cause.getCause().getMessage());
				}
				catch (Throwable t) {
					error[0] = t;
				}

				return null;
			}
		}).when(rich).onError(any(Throwable.class));

		ch.pipeline().fireExceptionCaught(new IOException("Connection reset by peer"));

		assertNull(error[0]);
	}

	/**
	 * Verifies that the channel is closed if there is an error *during* error notification.
	 */
	@Test
	public void testChannelClosedOnExceptionDuringErrorNotification() throws Exception {
		EmbeddedChannel ch = createEmbeddedChannel();

		NetworkClientHandler handler = getClientHandler(ch);

		RemoteInputChannel rich = addInputChannel(handler);

		doThrow(new RuntimeException("Expected test exception"))
				.when(rich).onError(any(Throwable.class));

		ch.pipeline().fireExceptionCaught(new Exception());

		assertFalse(ch.isActive());
	}

	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------

	private EmbeddedChannel createEmbeddedChannel() {
		NettyProtocol protocol = new NettyProtocol(
				mock(ResultPartitionProvider.class),
				mock(TaskEventDispatcher.class),
				true);

		return new EmbeddedChannel(protocol.getClientChannelHandlers());
	}

	private RemoteInputChannel addInputChannel(NetworkClientHandler clientHandler)
		throws IOException {
		RemoteInputChannel rich = createRemoteInputChannel();
		clientHandler.addInputChannel(rich);

		return rich;
	}

	private NetworkClientHandler getClientHandler(Channel ch) {
		return ch.pipeline().get(NetworkClientHandler.class);
	}

	private RemoteInputChannel createRemoteInputChannel() {
		return when(mock(RemoteInputChannel.class)
				.getInputChannelId())
				.thenReturn(new InputChannelID()).getMock();
	}
}
