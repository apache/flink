/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The test generates two random streams (input channels) which independently
 * and randomly generate checkpoint barriers. The two streams are very
 * unaligned, putting heavy work on the BarrierBuffer.
 */
public class AlignedControllerMassiveRandomTest {

	private static final int PAGE_SIZE = 1024;

	@Test
	public void testWithTwoChannelsAndRandomBarriers() throws Exception {
		NetworkBufferPool networkBufferPool1 = null;
		NetworkBufferPool networkBufferPool2 = null;
		try {
			networkBufferPool1 = new NetworkBufferPool(100, PAGE_SIZE);
			networkBufferPool2 = new NetworkBufferPool(100, PAGE_SIZE);
			BufferPool pool1 = networkBufferPool1.createBufferPool(100, 100);
			BufferPool pool2 = networkBufferPool2.createBufferPool(100, 100);

			RandomGeneratingInputGate myIG = new RandomGeneratingInputGate(
					new BufferPool[] { pool1, pool2 },
					new BarrierGenerator[] { new CountBarrier(100000), new RandomBarrier(100000) });

			CheckpointedInputGate checkpointedInputGate =
				new CheckpointedInputGate(
					myIG,
					new SingleCheckpointBarrierHandler(
						"Testing: No task associated",
						new DummyCheckpointInvokable(),
						myIG.getNumberOfInputChannels(),
						new AlignedController(myIG)),
					new SyncMailboxExecutor());

			for (int i = 0; i < 2000000; i++) {
				BufferOrEvent boe = checkpointedInputGate.pollNext().get();
				if (boe.isBuffer()) {
					boe.getBuffer().recycleBuffer();
				}
			}
		}
		finally {
			if (networkBufferPool1 != null) {
				networkBufferPool1.destroyAllBufferPools();
				networkBufferPool1.destroy();
			}
			if (networkBufferPool2 != null) {
				networkBufferPool2.destroyAllBufferPools();
				networkBufferPool2.destroy();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Mocks and Generators
	// ------------------------------------------------------------------------

	private interface BarrierGenerator {
		boolean isNextBarrier();
	}

	private static class RandomBarrier implements BarrierGenerator {

		private static final Random rnd = new Random();

		private final double threshold;

		public RandomBarrier(double expectedEvery) {
			threshold = 1 / expectedEvery;
		}

		@Override
		public boolean isNextBarrier() {
			return rnd.nextDouble() < threshold;
		}
	}

	private static class CountBarrier implements BarrierGenerator {

		private final long every;
		private long c = 0;

		public CountBarrier(long every) {
			this.every = every;
		}

		@Override
		public boolean isNextBarrier() {
			return c++ % every == 0;
		}
	}

	private static class RandomGeneratingInputGate extends IndexedInputGate {

		private final int numberOfChannels;
		private final BufferPool[] bufferPools;
		private final int[] currentBarriers;
		private final boolean[] channelBlocked;
		private final BarrierGenerator[] barrierGens;
		private int currentChannel = 0;
		private long c = 0;

		public RandomGeneratingInputGate(BufferPool[] bufferPools, BarrierGenerator[] barrierGens) {
			this.numberOfChannels = bufferPools.length;
			this.currentBarriers = new int[numberOfChannels];
			this.channelBlocked = new boolean[numberOfChannels];
			this.bufferPools = bufferPools;
			this.barrierGens = barrierGens;
			availabilityHelper.resetAvailable();
		}

		@Override
		public int getNumberOfInputChannels() {
			return numberOfChannels;
		}

		@Override
		public boolean isFinished() {
			return false;
		}

		@Override
		public InputChannel getChannel(int channelIndex) {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<InputChannelInfo> getChannelInfos() {
			return IntStream.range(0, numberOfChannels)
					.mapToObj(channelIndex -> new InputChannelInfo(0, channelIndex))
					.collect(Collectors.toList());
		}

		@Override
		public Optional<BufferOrEvent> getNext() throws IOException {
			currentChannel = (currentChannel + 1) % numberOfChannels;
			if (channelBlocked[currentChannel]) {
				return getNext();
			}

			if (barrierGens[currentChannel].isNextBarrier()) {
				channelBlocked[currentChannel] = true;
				if (allChannelsBlocked()) {
					Arrays.fill(channelBlocked, false);
				}

				return Optional.of(
					new BufferOrEvent(
						new CheckpointBarrier(
							++currentBarriers[currentChannel],
							System.currentTimeMillis(),
							CheckpointOptions.forCheckpointWithDefaultLocation()),
						new InputChannelInfo(0, currentChannel)));
			} else {
				Buffer buffer = bufferPools[currentChannel].requestBuffer();
				if (buffer == null) {
					// we exhausted buffer pool for this channel that is supposed to be blocked
					// by the credit-based flow control, so we should poll from the second channel
					return getNext();
				}
				buffer.getMemorySegment().putLong(0, c++);
				return Optional.of(new BufferOrEvent(buffer, new InputChannelInfo(0, currentChannel)));
			}
		}

		@Override
		public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
			return getNext();
		}

		private boolean allChannelsBlocked() {
			for (boolean blocked : channelBlocked) {
				if (!blocked) {
					return false;
				}
			}
			return true;
		}

		@Override
		public void sendTaskEvent(TaskEvent event) {}

		@Override
		public void resumeConsumption(InputChannelInfo channelInfo) {
		}

		@Override
		public void setup() {
		}

		@Override
		public CompletableFuture<Void> getStateConsumedFuture() {
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public void finishReadRecoveredState() {
		}

		@Override
		public void requestPartitions() {
		}

		@Override
		public void close() {
		}

		@Override
		public int getGateIndex() {
			return 0;
		}
	}
}
