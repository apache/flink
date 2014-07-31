/**
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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.NetworkConnectionManager;
import org.apache.flink.runtime.io.network.RemoteReceiver;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Assert;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class OutboundConnectionQueueTest {

	private final static long RANDOM_SEED = 520346508276087l;

	private final Object lock = new Object();

	private Channel channel;

	private NetworkConnectionManager connectionManager;

	private RemoteReceiver receiver;

	private OutboundConnectionQueue queue;

	private TestControlHandler controller;

	private TestVerificationHandler verifier;

	private Throwable exception;

	private void initTest(boolean autoTriggerWrite) {
		controller = new TestControlHandler(autoTriggerWrite);
		verifier = new TestVerificationHandler();

		channel = Mockito.spy(new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
			@Override
			public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
				exception = cause;
				super.exceptionCaught(ctx, cause);
			}
		}));

		connectionManager = Mockito.mock(NetworkConnectionManager.class);

		receiver = Mockito.mock(RemoteReceiver.class);

		queue = new OutboundConnectionQueue(channel, receiver, connectionManager, 0);

		channel.pipeline().addFirst("Test Control Handler", controller);
		channel.pipeline().addFirst("Test Verification Handler", verifier);

		exception = null;

		// The testing pipeline looks as follows:
		// - Test Verification Handler [OUT]
		// - Test Control Handler [IN]
		// - Idle State Handler [IN/OUT] [added by OutboundConnectionQueue]
		// - Outbound queue (SUT) [IN] [added by OutboundConnectionQueue]
		// - Exception setter [IN] [EmbeddedChannel constructor]
	}

	/**
	 * Verifies that the channel is closed after an idle event, when
	 * there are no queued envelopes.
	 */
	public void testClose() throws Exception {
		initTest(false);

		JobID jid = new JobID();
		ChannelID cid = new ChannelID();

		Assert.assertTrue(queue.enqueue(new Envelope(1, jid, cid)));
		Assert.assertTrue(queue.enqueue(new Envelope(2, jid, cid)));
		Assert.assertTrue(queue.enqueue(new Envelope(3, jid, cid)));

		controller.triggerWrite();

		controller.fireIdle();

		verifier.waitForClose();

		verifier.verifyEnvelopeReceived(cid, 3);

		Mockito.verify(connectionManager, Mockito.times(1)).close(Mockito.any(RemoteReceiver.class));
	}

	/**
	 * Verifies that the channel is not closed while there are queued
	 * envelopes.
	 */
	public void testCloseWithQueuedEnvelopes() throws Exception {
		initTest(true);

		final JobID jid = new JobID();
		final ChannelID cid = new ChannelID();
		final CountDownLatch sync = verifier.waitForEnvelopes(3, cid);

		// Make channel not writable => envelopes are queued
		Mockito.when(channel.isWritable()).thenReturn(false);

		Assert.assertTrue(queue.enqueue(new Envelope(1, jid, cid)));
		Assert.assertTrue(queue.enqueue(new Envelope(2, jid, cid)));
		Assert.assertTrue(queue.enqueue(new Envelope(3, jid, cid)));

		// Verify idle event doesn't close channel
		controller.fireIdle();

		Mockito.verify(connectionManager, Mockito.times(0)).close(Mockito.any(RemoteReceiver.class));

		Boolean hasRequestedClose = Whitebox.<Boolean>getInternalState(queue, "hasRequestedClose");
		Assert.assertFalse("Close request while envelope in flight.", hasRequestedClose);

		// Change writability of channel back to writable
		Mockito.when(channel.isWritable()).thenReturn(true);
		channel.pipeline().fireChannelWritabilityChanged();

		// Wait for the processing of queued envelopes
		while (sync.getCount() != 0) {
			sync.await();
		}

		verifier.verifyEnvelopeReceived(cid, 3);

		// Now close again
		controller.fireIdle();
		verifier.waitForClose();

		Mockito.verify(connectionManager, Mockito.times(1)).close(Mockito.any(RemoteReceiver.class));
	}

	/**
	 * Verifies that envelopes are delegated back to the connection
	 * manager after a close.
	 */
	public void testEnqueueAfterClose() throws Exception {
		initTest(true);

		// Immediately close the channel
		controller.fireIdle();
		verifier.waitForClose();

		Assert.assertFalse(queue.enqueue(new Envelope(1, new JobID(), new ChannelID())));
	}

	/**
	 * Verifies that multiple idle events are handled correctly.
	 */
	public void testMultipleIdleEvents() throws Exception {
		initTest(true);

		controller.fireIdle();
		verifier.waitForClose();

		controller.fireIdle();

		// Second close should not cause an exception in the
		// verification handler.
		Assert.assertNull(exception);
	}

	/**
	 * Verifies that unknown user events throw an exception.
	 */
	public void testUnknownUserEvent() throws Exception {
		initTest(true);

		Assert.assertNull(exception);

		controller.context.fireUserEventTriggered("Unknown user event");

		Assert.assertNotNull(exception);
		Assert.assertTrue(exception instanceof IllegalStateException);
	}

	// ------------------------------------------------------------------------

	public void testConcurrentEnqueueAndClose() throws Exception {
		Integer[][] configs = new Integer[][]{
				{1, 512, 0, 0},
				{1, 512, 40, 80},
				{2, 512, 40, 80},
				{4, 512, 40, 80},
				{8, 512, 40, 80},
				{32, 512, 40, 80},
				{128, 512, 40, 80},
				{256, 512, 40, 80},
				{512, 512, 40, 80}
		};

		for (Integer[] params : configs) {
			System.out.println(String.format(
					"Running %s with config: %d producers, %d envelopes to send per producer, " +
							"%d ms min sleep time, %d ms max sleep time.", "testConcurrentEnqueueAndClose",
					params[0], params[1], params[2], params[3]));

			long start = System.currentTimeMillis();
			doTestConcurrentEnqueueAndClose(params[0], params[1], params[2], params[3]);
			long end = System.currentTimeMillis();

			System.out.println(String.format("Runtime: %d ms.", (end - start)));
		}
	}

	/**
	 * Verifies that concurrent enqueue and close events are handled
	 * correctly.
	 */
	private void doTestConcurrentEnqueueAndClose(
			final int numProducers,
			final int numEnvelopesPerProducer,
			final int minSleepTimeMs,
			final int maxSleepTimeMs) throws Exception {

		final InetAddress bindHost = InetAddress.getLocalHost();
		final int bindPort = 20000;

		// Testing concurrent enqueue and close requires real TCP channels,
		// because Netty's testing EmbeddedChannel does not implement the
		// same threading model as the NioEventLoopGroup (for example there
		// is no difference between being IN and OUTSIDE of the event loop
		// thread).

		final ServerBootstrap in = new ServerBootstrap();
		in.group(new NioEventLoopGroup(1))
				.channel(NioServerSocketChannel.class)
				.localAddress(bindHost, bindPort)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new ChannelInboundHandlerAdapter());
					}
				});

		final Bootstrap out = new Bootstrap();
		out.group(new NioEventLoopGroup(1))
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel channel) throws Exception {
						channel.pipeline()
								.addLast(new ChannelOutboundHandlerAdapter());
					}
				})
				.option(ChannelOption.TCP_NODELAY, false)
				.option(ChannelOption.SO_KEEPALIVE, true);

		in.bind().sync();

		// --------------------------------------------------------------------

		// The testing pipeline looks as follows:
		// - Test Verification Handler [OUT]
		// - Test Control Handler [IN]
		// - Idle State Handler [IN/OUT] [added by OutboundConnectionQueue]
		// - Outbound queue (SUT) [IN] [added by OutboundConnectionQueue]

		channel = out.connect(bindHost, bindPort).sync().channel();

		queue = new OutboundConnectionQueue(channel, receiver, connectionManager, 0);

		controller = new TestControlHandler(true);
		verifier = new TestVerificationHandler();

		channel.pipeline().addFirst("Test Control Handler", controller);
		channel.pipeline().addFirst("Test Verification Handler", verifier);

		// --------------------------------------------------------------------

		final Random rand = new Random(RANDOM_SEED);

		// Every producer works on their local reference of the queue and only
		// updates it to the new channel when enqueue returns false, which
		// should only happen if the channel has been closed.
		final ConcurrentMap<ChannelID, OutboundConnectionQueue> producerQueues =
				new ConcurrentHashMap<ChannelID, OutboundConnectionQueue>();

		final ChannelID[] ids = new ChannelID[numProducers];

		for (int i = 0; i < numProducers; i++) {
			ids[i] = new ChannelID();

			producerQueues.put(ids[i], queue);
		}

		final CountDownLatch receivedAllEnvelopesLatch =
				verifier.waitForEnvelopes(numEnvelopesPerProducer - 1, ids);

		final List<Channel> closedChannels = new ArrayList<Channel>();

		// --------------------------------------------------------------------

		final Runnable closer = new Runnable() {
			@Override
			public void run() {
				while (receivedAllEnvelopesLatch.getCount() != 0) {
					try {
						controller.fireIdle();

						// Test two idle events arriving "closely"
						// after each other
						if (rand.nextBoolean()) {
							controller.fireIdle();
						}

						Thread.sleep(minSleepTimeMs / 2);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};

		final Runnable[] producers = new Runnable[numProducers];

		for (int i = 0; i < numProducers; i++) {
			final int index = i;

			producers[i] = new Runnable() {
				@Override
				public void run() {
					final JobID jid = new JobID();
					final ChannelID cid = ids[index];

					for (int j = 0; j < numEnvelopesPerProducer; j++) {
						OutboundConnectionQueue localQueue = producerQueues.get(cid);

						try {
							// This code path is handled by the NetworkConnectionManager
							// in production to enqueue the envelope either to the current
							// channel or a new one if it was closed.
							while (!localQueue.enqueue(new Envelope(j, jid, cid))) {
								synchronized (lock) {
									if (localQueue == queue) {
										closedChannels.add(channel);

										channel = out.connect(bindHost, bindPort).sync().channel();

										queue = new OutboundConnectionQueue(channel, receiver, connectionManager, 0);

										channel.pipeline().addFirst("Test Control Handler", controller);
										channel.pipeline().addFirst("Test Verification Handler", verifier);
									}
								}

								producerQueues.put(cid, queue);
								localQueue = queue;
							}

							int sleepTime = rand.nextInt((maxSleepTimeMs - minSleepTimeMs) + 1) + minSleepTimeMs;
							Thread.sleep(sleepTime);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					}
				}
			};
		}

		for (int i = 0; i < numProducers; i++) {
			new Thread(producers[i], "Producer " + i).start();
		}

		new Thread(closer, "Closer").start();

		// --------------------------------------------------------------------

		while (receivedAllEnvelopesLatch.getCount() != 0) {
			receivedAllEnvelopesLatch.await();
		}

		// Final close, if the last close didn't make it.
		synchronized (lock) {
			if (channel != null) {
				controller.fireIdle();
			}
		}

		verifier.waitForClose();

		// If the producers do not sleep after each envelope, the close
		// should not make it through and no channel should have been
		// added to the list of closed channels
		if (minSleepTimeMs == 0 && maxSleepTimeMs == 0) {
			Assert.assertEquals(0, closedChannels.size());
		}

		for (Channel ch : closedChannels) {
			Assert.assertFalse(ch.isOpen());
		}

		System.out.println(closedChannels.size() + " channels were closed during execution.");

		out.group().shutdownGracefully().sync();
		in.group().shutdownGracefully().sync();
	}

	// ------------------------------------------------------------------------

	// Handler to control the flow of Netty's custom user events. Used
	// to fire idle events and manually trigger write events.
	@ChannelHandler.Sharable
	private static class TestControlHandler extends ChannelInboundHandlerAdapter {

		private final Queue<Object> events = new ArrayDeque<Object>();

		private final boolean forwardUserEvents;

		private ChannelHandlerContext context;

		private TestControlHandler(boolean forwardUserEvents) {
			this.forwardUserEvents = forwardUserEvents;
		}

		@Override
		public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
			synchronized (this) {
				context = ctx;
			}
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (forwardUserEvents) {
				ctx.fireUserEventTriggered(evt);
			}
			else {
				events.add(evt);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			super.exceptionCaught(ctx, cause);
		}

		public void triggerWrite() {
			synchronized (this) {
				if (!events.isEmpty()) {
					context.fireUserEventTriggered(events.remove());
				}
			}
		}

		public void fireIdle() {
			synchronized (this) {
				context.fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
			}
		}
	}

	// Handler to verify writes and close events.
	@ChannelHandler.Sharable
	private static class TestVerificationHandler extends ChannelOutboundHandlerAdapter {

		private final Map<ChannelID, Integer> receivedSequenceNums = new HashMap<ChannelID, Integer>();

		private final Map<ChannelID, Integer> expectedSequenceNums = new HashMap<ChannelID, Integer>();

		private final Map<ChannelID, CountDownLatch> envelopeLatches = new HashMap<ChannelID, CountDownLatch>();

		private CountDownLatch closeLatch;

		@Override
		public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
			closeLatch = new CountDownLatch(1);

			super.handlerAdded(ctx);
		}

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			if (closeLatch.getCount() == 0) {
				throw new IllegalStateException("Write on closed channel.");
			}

			if (msg.getClass() == Envelope.class) {
				Envelope env = (Envelope) msg;

				final int currentSeqNum = env.getSequenceNumber();
				final ChannelID source = env.getSource();

				Integer previousSeqNum = receivedSequenceNums.put(source, currentSeqNum);

				if (previousSeqNum != null) {
					String errMsg = String.format("Received %s with unexpected sequence number.", env);
					Assert.assertEquals(errMsg, previousSeqNum + 1, currentSeqNum);
				}

				promise.setSuccess();

				Integer expectedSeqNum = expectedSequenceNums.get(source);
				if (expectedSeqNum != null && expectedSeqNum.equals(currentSeqNum)) {
					envelopeLatches.remove(source).countDown();
				}
			}
		}

		@Override
		public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
			if (closeLatch.getCount() == 0) {
				throw new IllegalStateException("Received multiple close events.");
			}

			super.close(ctx, promise);

			closeLatch.countDown();
		}

		public void waitForClose() throws InterruptedException {
			while (closeLatch.getCount() != 0) {
				closeLatch.await();
			}
		}

		/**
		 * Verifies that envelope with expected sequence number has been
		 * processed by the handler (or no envelope has been received by
		 * the given source, if expected sequence number is null).
		 */
		public void verifyEnvelopeReceived(ChannelID source, Integer expectedSequenceNum) {
			if (expectedSequenceNum == null && receivedSequenceNums.containsKey(source)) {
				Assert.fail("Received unexpected envelope from channel " + source);
			}
			else if (receivedSequenceNums.containsKey(source)) {
				Assert.assertEquals(expectedSequenceNum, receivedSequenceNums.get(source));
			}
			else {
				Assert.fail("Did not receive any envelope from channel " + source);
			}
		}

		public CountDownLatch waitForEnvelopes(Integer expectedSequenceNum, ChannelID... ids) {
			CountDownLatch latch = new CountDownLatch(ids.length);

			for (ChannelID id : ids) {
				expectedSequenceNums.put(id, expectedSequenceNum);
				envelopeLatches.put(id, latch);
			}

			return latch;
		}
	}

	// --------------------------------------------------------------------

	public void runAllTests() throws Exception {
		testClose();
		testCloseWithQueuedEnvelopes();
		testEnqueueAfterClose();
		testUnknownUserEvent();
		testMultipleIdleEvents();
		testConcurrentEnqueueAndClose();
	}

	public static void main(String[] args) throws Exception {
		new OutboundConnectionQueueTest().runAllTests();
	}
}
