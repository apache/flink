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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * The test generates two random streams (input channels) which independently
 * and randomly generate checkpoint barriers. The two streams are very
 * unaligned, putting heavy work on the BarrierBuffer.
 */
public class BarrierBufferMassiveRandomTest {

	private static final int PAGE_SIZE = 1024;

	@Test
	public void testWithTwoChannelsAndRandomBarriers() {
		IOManager ioMan = null;
		NetworkBufferPool networkBufferPool1 = null;
		NetworkBufferPool networkBufferPool2 = null;
		try {
			ioMan = new IOManagerAsync();

			networkBufferPool1 = new NetworkBufferPool(100, PAGE_SIZE);
			networkBufferPool2 = new NetworkBufferPool(100, PAGE_SIZE);
			BufferPool pool1 = networkBufferPool1.createBufferPool(100, 100);
			BufferPool pool2 = networkBufferPool2.createBufferPool(100, 100);

			RandomGeneratingInputGate myIG = new RandomGeneratingInputGate(
					new BufferPool[] { pool1, pool2 },
					new BarrierGenerator[] { new CountBarrier(100000), new RandomBarrier(100000) });

			BarrierBuffer barrierBuffer = new BarrierBuffer(myIG, new BufferSpiller(ioMan, myIG.getPageSize()));

			for (int i = 0; i < 2000000; i++) {
				BufferOrEvent boe = barrierBuffer.getNextNonBlocked();
				if (boe.isBuffer()) {
					boe.getBuffer().recycleBuffer();
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (ioMan != null) {
				ioMan.shutdown();
			}
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

	private static class RandomGeneratingInputGate implements InputGate {

		private final int numChannels;
		private final BufferPool[] bufferPools;
		private final int[] currentBarriers;
		private final BarrierGenerator[] barrierGens;
		private int currentChannel = 0;
		private long c = 0;

		public RandomGeneratingInputGate(BufferPool[] bufferPools, BarrierGenerator[] barrierGens) {
			this.numChannels = bufferPools.length;
			this.currentBarriers = new int[numChannels];
			this.bufferPools = bufferPools;
			this.barrierGens = barrierGens;
		}

		@Override
		public int getNumberOfInputChannels() {
			return numChannels;
		}

		@Override
		public boolean isFinished() {
			return false;
		}

		@Override
		public void requestPartitions() {}

		@Override
		public Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException {
			currentChannel = (currentChannel + 1) % numChannels;

			if (barrierGens[currentChannel].isNextBarrier()) {
				return Optional.of(
					new BufferOrEvent(
						new CheckpointBarrier(
							++currentBarriers[currentChannel],
							System.currentTimeMillis(),
							CheckpointOptions.forCheckpointWithDefaultLocation()),
						currentChannel));
			} else {
				Buffer buffer = bufferPools[currentChannel].requestBuffer();
				buffer.getMemorySegment().putLong(0, c++);
				return Optional.of(new BufferOrEvent(buffer, currentChannel));
			}
		}

		@Override
		public Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException {
			return getNextBufferOrEvent();
		}

		@Override
		public void sendTaskEvent(TaskEvent event) {}

		@Override
		public void registerListener(InputGateListener listener) {}

		@Override
		public int getPageSize() {
			return PAGE_SIZE;
		}
	}
}
