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

import org.apache.flink.runtime.io.network.ChannelManager;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.RemoteReceiver;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Assert;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class NettyConnectionManagerTest {

	private final static long RANDOM_SEED = 520346508276087l;

	private Random rand = new Random(RANDOM_SEED);

	private NettyConnectionManager senderManager;

	private NettyConnectionManager receiverManager;

	private ChannelManager channelManager;

	private RemoteReceiver[] receivers;

	private CountDownLatch receivedAllEnvelopesLatch;

	private void initTest(
			int numProducers,
			final int numEnvelopesPerProducer,
			int numInThreads,
			int numOutThreads,
			int closeAfterIdleForMs) throws Exception {

		final InetAddress bindAddress = InetAddress.getLocalHost();
		final int bindPort = 20000;
		final int bufferSize = 32 * 1024;

		senderManager = Mockito.spy(new NettyConnectionManager(
				bindAddress, bindPort, bufferSize, numInThreads, numOutThreads, closeAfterIdleForMs));

		receiverManager = new NettyConnectionManager(
				bindAddress, bindPort + 1, bufferSize, numInThreads, numOutThreads, closeAfterIdleForMs);

		channelManager = Mockito.mock(ChannelManager.class);

		senderManager.start(channelManager);
		receiverManager.start(channelManager);

		receivers = new RemoteReceiver[numProducers];
		for (int i = 0; i < numProducers; i++) {
			receivers[i] = new RemoteReceiver(new InetSocketAddress(bindPort + 1), i);
		}

		// --------------------------------------------------------------------

		receivedAllEnvelopesLatch = new CountDownLatch(numProducers);

		final ConcurrentMap<ChannelID, Integer> receivedSequenceNums =
				new ConcurrentHashMap<ChannelID, Integer>();

		// Verifies that the sequence numbers of each producer are received
		// in ascending incremental order. In addition, manages a latch to
		// allow synchronization after all envelopes have been received.
		Mockito.doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				final Envelope env = (Envelope) invocation.getArguments()[0];

				final int currentSeqNum = env.getSequenceNumber();
				final ChannelID cid = env.getSource();

				if (currentSeqNum < 0 || currentSeqNum >= numEnvelopesPerProducer) {
					Assert.fail("Received more envelopes than expected from " + cid);
				}

				Integer previousSeqNum = receivedSequenceNums.put(cid, currentSeqNum);

				if (previousSeqNum != null) {
					String errMsg = String.format("Received %s with unexpected sequence number.", env);
					Assert.assertEquals(errMsg, previousSeqNum + 1, currentSeqNum);
				}

				if (currentSeqNum == numEnvelopesPerProducer - 1) {
					receivedAllEnvelopesLatch.countDown();
				}

				return null;
			}
		}).when(channelManager).dispatchFromNetwork(Matchers.any(Envelope.class));
	}

	private void finishTest() throws IOException {
		senderManager.shutdown();
		receiverManager.shutdown();
	}

	// ------------------------------------------------------------------------

	// Verifies that there are no race conditions or dead locks with
	// concurrent enqueues and closes.
	public void testConcurrentEnqueueAndClose() throws Exception {
		Integer[][] configs = new Integer[][]{
				// No close after idle
				{64, 4096, 1, 1, 1, 0, 0},
				{128, 2048, 1, 1, 1, 0, 0},
				{256, 1024, 1, 1, 1, 0, 0},
				{512, 512, 1, 1, 1, 0, 0},

				{64, 4096, 4, 1, 1, 0, 0},
				{128, 2048, 4, 1, 1, 0, 0},
				{256, 1024, 4, 1, 1, 0, 0},
				{512, 512, 4, 1, 1, 0, 0},

				{64, 4096, 4, 2, 2, 0, 0},
				{128, 2048, 4, 2, 2, 0, 0},
				{256, 1024, 4, 2, 2, 0, 0},
				{512, 512, 4, 2, 2, 0, 0},
				// Note: these need plenty of heap space for the threads
				{1024, 256, 4, 2, 2, 0, 0},
				{2048, 128, 4, 2, 2, 0, 0},
				{4096, 64, 4, 2, 2, 0, 0},

				// With close after idle
				{4, 1024, 1, 1, 1, 40, 80},
				{8, 1024, 1, 1, 1, 40, 80},
				{16, 1024, 1, 1, 1, 40, 80},
				{32, 1024, 1, 1, 1, 40, 80},
				{64, 1024, 1, 1, 1, 40, 80},

				{16, 1024, 4, 1, 1, 40, 80},
				{32, 1024, 4, 1, 1, 40, 80},
				{64, 1024, 4, 1, 1, 40, 80},

				{16, 1024, 4, 2, 2, 40, 80},
				{32, 1024, 4, 2, 2, 40, 80},
				{64, 1024, 4, 2, 2, 40, 80}
		};

		for (Integer[] params : configs) {
			System.out.println(String.format("Running testConcurrentEnqueueAndClose with config: " +
							"%d producers, %d envelopes per producer, %d num channels, " +
							"%d num in threads, %d num out threads, " +
							"%d ms min sleep time, %d ms max sleep time.",
					params[0], params[1], params[2], params[3], params[4], params[5], params[6]));

			long start = System.currentTimeMillis();
			doTestConcurrentEnqueueAndClose(params[0], params[1], params[2], params[3], params[4], params[5], params[6]);
			long end = System.currentTimeMillis();

			System.out.println(String.format("Runtime: %d ms.", (end - start)));
		}
	}

	private void doTestConcurrentEnqueueAndClose(
			int numProducers,
			final int numEnvelopesPerProducer,
			final int numChannels,
			int numInThreads,
			int numOutThreads,
			final int minSleepTimeMs,
			final int maxSleepTimeMs) throws Exception {

		// The idle time before a close is requested is 1/4th of the min sleep
		// time of each producer. Depending on the number of concurrent producers
		// and number of envelopes to send per producer, this will result in a
		// variable number of close requests.
		initTest(numProducers, numEnvelopesPerProducer, numInThreads, numOutThreads, minSleepTimeMs / 4);

		// --------------------------------------------------------------------

		Runnable[] producers = new Runnable[numProducers];

		for (int i = 0; i < numProducers; i++) {
			producers[i] = new Runnable() {
				@Override
				public void run() {
					final JobID jid = new JobID();
					final ChannelID cid = new ChannelID();
					final RemoteReceiver receiver = receivers[rand.nextInt(numChannels)];

					for (int sequenceNum = 0; sequenceNum < numEnvelopesPerProducer; sequenceNum++) {
						try {
							senderManager.enqueue(new Envelope(sequenceNum, jid, cid), receiver);

							int sleepTime = rand.nextInt((maxSleepTimeMs - minSleepTimeMs) + 1) + minSleepTimeMs;
							Thread.sleep(sleepTime);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			};
		}

		for (int i = 0; i < numProducers; i++) {
			new Thread(producers[i], "Producer " + i).start();
		}

		// --------------------------------------------------------------------

		while (receivedAllEnvelopesLatch.getCount() != 0) {
			receivedAllEnvelopesLatch.await();
		}

		finishTest();
	}

	private void runAllTests() throws Exception {
		testConcurrentEnqueueAndClose();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		new NettyConnectionManagerTest().runAllTests();
	}
}
