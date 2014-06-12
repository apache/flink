/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.netty;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.ChannelManager;
import eu.stratosphere.runtime.io.network.Envelope;
import eu.stratosphere.runtime.io.network.RemoteReceiver;
import junit.framework.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class NettyConnectionManagerTest {

	private final static long RANDOM_SEED = 520346508276087l;

	private final static Random random = new Random(RANDOM_SEED);

	private final static int BIND_PORT = 20000;

	private final static int HIGH_WATERMARK = 32 * 1024;

	private int numSubtasks;

	private int numToSendPerSubtask;

	private int numInThreads;

	private int numOutThreads;

	private int numChannels;

	public NettyConnectionManagerTest(int numSubtasks, int numToSendPerSubtask, int numChannels, int numInThreads, int numOutThreads) {
		this.numSubtasks = numSubtasks;
		this.numToSendPerSubtask = numToSendPerSubtask;
		this.numChannels = numChannels;
		this.numInThreads = numInThreads;
		this.numOutThreads = numOutThreads;
	}

	@Parameterized.Parameters
	public static Collection<Integer[]> configure() {
		return Arrays.asList(
				new Integer[][]{
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
				}
		);
	}

	public void testEnqueueRaceAndDeadlockFreeMultipleChannels() throws Exception {
		final InetAddress localhost = InetAddress.getLocalHost();
		final CountDownLatch latch = new CountDownLatch(this.numSubtasks);

		// --------------------------------------------------------------------
		// setup
		// --------------------------------------------------------------------
		ChannelManager channelManager = mock(ChannelManager.class);
		doAnswer(new VerifyEnvelopes(latch)).when(channelManager).dispatchFromNetwork(Matchers.<Envelope>anyObject());

		NettyConnectionManager connManagerToTest = new NettyConnectionManager(channelManager, localhost,
				BIND_PORT, HIGH_WATERMARK, this.numInThreads, this.numOutThreads, -1, -1);

		NettyConnectionManager connManagerReceiver = new NettyConnectionManager(channelManager, localhost,
				BIND_PORT + 1, HIGH_WATERMARK, this.numInThreads, this.numOutThreads, -1, -1);

		// --------------------------------------------------------------------
		// start sender threads
		// --------------------------------------------------------------------
		RemoteReceiver[] receivers = new RemoteReceiver[this.numChannels];

		for (int i = 0; i < this.numChannels; i++) {
			receivers[i] = new RemoteReceiver(new InetSocketAddress(localhost, BIND_PORT + 1), i);
		}

		for (int i = 0; i < this.numSubtasks; i++) {
			RemoteReceiver receiver = receivers[random.nextInt(this.numChannels)];
			new Thread(new SubtaskSenderThread(connManagerToTest, receiver)).start();
		}

		latch.await();

		connManagerToTest.shutdown();
		connManagerReceiver.shutdown();
	}


	private class VerifyEnvelopes implements Answer {

		private final ConcurrentMap<ChannelID, Integer> received = new ConcurrentHashMap<ChannelID, Integer>();

		private final CountDownLatch latch;

		private VerifyEnvelopes(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {
			Envelope env = (Envelope) invocation.getArguments()[0];

			ChannelID channelId = env.getSource();
			int seqNum = env.getSequenceNumber();

			if (seqNum == 0) {
				Assert.assertNull(
						String.format("Received envelope from %s before, but current seq num is 0", channelId),
						this.received.putIfAbsent(channelId, seqNum));
			}
			else {
				Assert.assertTrue(
						String.format("Received seq num %d from %s, but previous was not %d", seqNum, channelId, seqNum - 1),
						this.received.replace(channelId, seqNum - 1, seqNum));
			}

			// count down the latch if all envelopes received for this source
			if (seqNum == numToSendPerSubtask - 1) {
				this.latch.countDown();
			}

			return null;
		}
	}

	private class SubtaskSenderThread implements Runnable {

		private final NettyConnectionManager connectionManager;

		private final RemoteReceiver receiver;

		private final JobID jobId = new JobID();

		private final ChannelID channelId = new ChannelID();

		private int seqNum = 0;

		private SubtaskSenderThread(NettyConnectionManager connectionManager, RemoteReceiver receiver) {
			this.connectionManager = connectionManager;
			this.receiver = receiver;
		}

		@Override
		public void run() {
			// enqueue envelopes with ascending seq nums
			while (this.seqNum < numToSendPerSubtask) {
				try {
					Envelope env = new Envelope(this.seqNum++, this.jobId, this.channelId);
					this.connectionManager.enqueue(env, receiver);
				} catch (IOException e) {
					throw new RuntimeException("Unexpected exception while enqueing envelope");
				}
			}
		}
	}

	private void runAllTests() throws Exception {
		System.out.println(String.format("Running tests with config: %d sub tasks, %d envelopes to send per subtasks, "
				+ "%d num channels, %d num in threads, %d num out threads.",
				this.numSubtasks, this.numToSendPerSubtask, this.numChannels, this.numInThreads, this.numOutThreads));

		testEnqueueRaceAndDeadlockFreeMultipleChannels();

		System.out.println("Done.");
	}

	public static void main(String[] args) throws Exception {
		Collection<Integer[]> configs = configure();

		for (Integer[] params : configs) {

			NettyConnectionManagerTest test = new NettyConnectionManagerTest(params[0], params[1], params[2], params[3], params[4]);
			test.runAllTests();
		}
	}
}
