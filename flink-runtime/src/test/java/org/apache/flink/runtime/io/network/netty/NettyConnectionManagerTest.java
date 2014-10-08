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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

// TODO this test needs to be refactored for the new partition based protocol
public class NettyConnectionManagerTest {

	private final static long RANDOM_SEED = 520346508276087l;

	private final static Random random = new Random(RANDOM_SEED);

	private final static int BIND_PORT = 20000;

	private final static int BUFFER_SIZE = 32 * 1024;

	public void testEnqueueRaceAndDeadlockFreeMultipleChannels() throws Exception {
		Integer[][] configs = new Integer[][]{
				{64, 4096, 1, 1, 1},
				{128, 2048, 1, 1, 1},
				{256, 1024, 1, 1, 1},
				{512, 512, 1, 1, 1},
				{64, 4096, 4, 1, 1},
				{128, 2048, 4, 1, 1},
				{256, 1024, 4, 1, 1},
				{512, 512, 4, 1, 1},
				{64, 4096, 4, 2, 2},
				{128, 2048, 4, 2, 2},
				{256, 1024, 4, 2, 2},
				{512, 512, 4, 2, 2}
		};

		for (Integer[] params : configs) {
			System.out.println(String.format("Running %s with config: %d sub tasks, %d envelopes to send per subtasks, "
							+ "%d num channels, %d num in threads, %d num out threads.",
					"testEnqueueRaceAndDeadlockFreeMultipleChannels", params[0], params[1], params[2], params[3], params[4]));

			long start = System.currentTimeMillis();
			doTestEnqueueRaceAndDeadlockFreeMultipleChannels(params[0], params[1], params[2], params[3], params[4]);
			long end = System.currentTimeMillis();

			System.out.println(String.format("Runtime: %d ms.", (end - start)));
		}
	}

	private void doTestEnqueueRaceAndDeadlockFreeMultipleChannels(
			int numSubtasks, final int numToSendPerSubtask, int numChannels, int numInThreads, int numOutThreads)
			throws Exception {

		final InetAddress localhost = InetAddress.getLocalHost();
		final CountDownLatch latch = new CountDownLatch(numSubtasks);

		// --------------------------------------------------------------------
		// setup
		// --------------------------------------------------------------------
//		ChannelManager channelManager = mock(ChannelManager.class);
//		doAnswer(new VerifyEnvelopes(latch, numToSendPerSubtask))
//				.when(channelManager).dispatchFromNetwork(Matchers.<Envelope>anyObject());

		Configuration conf = new Configuration();
		conf.setInteger("taskmanager.net.client.numThreads", numInThreads);
		conf.setInteger("taskmanager.net.server.numThreads", numOutThreads);

		final NettyConnectionManager senderConnManager = new NettyConnectionManager(localhost, BIND_PORT, BUFFER_SIZE);
		senderConnManager.start();

		NettyConnectionManager receiverConnManager = new NettyConnectionManager(localhost, BIND_PORT + 1, BUFFER_SIZE);
		receiverConnManager.start();

		// --------------------------------------------------------------------
		// start sender threads
		// --------------------------------------------------------------------
		RemoteAddress[] receivers = new RemoteAddress[numChannels];

		for (int i = 0; i < numChannels; i++) {
			receivers[i] = new RemoteAddress(new InetSocketAddress(localhost, BIND_PORT + 1), i);
		}

		for (int i = 0; i < numSubtasks; i++) {
			final RemoteAddress receiver = receivers[random.nextInt(numChannels)];

			final AtomicInteger seqNum = new AtomicInteger(0);
			final JobID jobId = new JobID();
			final InputChannelID channelId = new InputChannelID();

			new Thread(new Runnable() {
				@Override
				public void run() {
					// enqueue envelopes with ascending seq numbers
					while (seqNum.get() < numToSendPerSubtask) {
						try {
							Envelope env = new Envelope(seqNum.getAndIncrement(), jobId, channelId);
							senderConnManager.connectOrGet(receiver);
						}
						catch (IOException e) {
							throw new RuntimeException("Unexpected exception while enqueuing envelope.");
						}
					}
				}
			}).start();
		}

		latch.await();

		senderConnManager.shutdown();
		receiverConnManager.shutdown();
	}

	/**
	 * Verifies correct ordering of received envelopes (per envelope source channel ID).
	 */
	private class VerifyEnvelopes implements Answer<Void> {

		private final ConcurrentMap<InputChannelID, Integer> received = new ConcurrentHashMap<InputChannelID, Integer>();

		private final CountDownLatch latch;

		private final int numExpectedEnvelopesPerSubtask;

		private VerifyEnvelopes(CountDownLatch latch, int numExpectedEnvelopesPerSubtask) {
			this.latch = latch;
			this.numExpectedEnvelopesPerSubtask = numExpectedEnvelopesPerSubtask;
		}

		@Override
		public Void answer(InvocationOnMock invocation) throws Throwable {
			Envelope env = (Envelope) invocation.getArguments()[0];

			InputChannelID channelId = env.getSource();
			int seqNum = env.getSequenceNumber();

			if (seqNum == 0) {
				Integer previousSeqNum = this.received.putIfAbsent(channelId, seqNum);

				String msg = String.format("Received envelope from %s before, but current seq num is 0", channelId);
				Assert.assertNull(msg, previousSeqNum);
			}
			else {
				boolean isExpectedPreviousSeqNum = this.received.replace(channelId, seqNum - 1, seqNum);

				String msg = String.format("Received seq num %d from %s, but previous was not %d.",
						seqNum, channelId, seqNum - 1);
				Assert.assertTrue(msg, isExpectedPreviousSeqNum);
			}

			// count down the latch if all envelopes received for this source
			if (seqNum == numExpectedEnvelopesPerSubtask - 1) {
				this.latch.countDown();
			}

			return null;
		}
	}

	private void runAllTests() throws Exception {
		testEnqueueRaceAndDeadlockFreeMultipleChannels();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		new NettyConnectionManagerTest().runAllTests();
	}
}
