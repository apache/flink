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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import org.junit.Test;

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
		try {
			ioMan = new IOManagerAsync();
			
			BufferPool pool1 = new NetworkBufferPool(100, PAGE_SIZE).createBufferPool(100, true);
			BufferPool pool2 = new NetworkBufferPool(100, PAGE_SIZE).createBufferPool(100, true);

			RandomGeneratingInputGate myIG = new RandomGeneratingInputGate(
					new BufferPool[] { pool1, pool2 },
					new BarrierGenerator[] { new CountBarrier(100000), new RandomBarrier(100000) });
	
			BarrierBuffer barrierBuffer = new BarrierBuffer(myIG, ioMan);
			
			for (int i = 0; i < 2000000; i++) {
				BufferOrEvent boe = barrierBuffer.getNextNonBlocked();
				if (boe.isBuffer()) {
					boe.getBuffer().recycle();
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
		}
	}

	// ------------------------------------------------------------------------
	//  Mocks and Generators
	// ------------------------------------------------------------------------
	
	protected interface BarrierGenerator {
		public boolean isNextBarrier();
	}

	protected static class RandomBarrier implements BarrierGenerator {
		
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

	protected static class RandomGeneratingInputGate implements InputGate {

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
		public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
			currentChannel = (currentChannel + 1) % numChannels;

			if (barrierGens[currentChannel].isNextBarrier()) {
				return new BufferOrEvent(
						new CheckpointBarrier(++currentBarriers[currentChannel], System.currentTimeMillis()),
							currentChannel);
			} else {
				Buffer buffer = bufferPools[currentChannel].requestBuffer();
				buffer.getMemorySegment().putLong(0, c++);
				return new BufferOrEvent(buffer, currentChannel);
			}
		}

		@Override
		public void sendTaskEvent(TaskEvent event) {}

		@Override
		public void registerListener(EventListener<InputGate> listener) {}

		@Override
		public int getPageSize() {
			return PAGE_SIZE;
		}
	}
}
