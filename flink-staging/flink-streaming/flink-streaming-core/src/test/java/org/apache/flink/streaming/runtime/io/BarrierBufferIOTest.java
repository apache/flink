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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;

public class BarrierBufferIOTest {

	@Test
	public void IOTest() throws IOException, InterruptedException {

		BufferPool pool1 = new NetworkBufferPool(100, 1024).createBufferPool(100, true);
		BufferPool pool2 = new NetworkBufferPool(100, 1024).createBufferPool(100, true);

		MockInputGate myIG = new MockInputGate(new BufferPool[] { pool1, pool2 },
				new BarrierGenerator[] { new CountBarrier(100000), new RandomBarrier(100000) });
		// new BarrierSimulator[] { new CountBarrier(1000), new
		// CountBarrier(1000) });

		BarrierBuffer barrierBuffer = new BarrierBuffer(myIG,
				new BarrierBufferTest.MockReader(myIG));

		try {
			// long time = System.currentTimeMillis();
			for (int i = 0; i < 2000000; i++) {
				BufferOrEvent boe = barrierBuffer.getNextNonBlocked();
				if (boe.isBuffer()) {
					boe.getBuffer().recycle();
				} else {
					barrierBuffer.processBarrier(boe);
				}
			}
			// System.out.println("Ran for " + (System.currentTimeMillis() -
			// time));
		} catch (Exception e) {
			fail();
		} finally {
			barrierBuffer.cleanup();
		}
	}

	private static class RandomBarrier implements BarrierGenerator {
		private static Random rnd = new Random();

		double threshold;

		public RandomBarrier(double expectedEvery) {
			threshold = 1 / expectedEvery;
		}

		@Override
		public boolean isNextBarrier() {
			return rnd.nextDouble() < threshold;
		}
	}

	private static class CountBarrier implements BarrierGenerator {

		long every;
		long c = 0;

		public CountBarrier(long every) {
			this.every = every;
		}

		@Override
		public boolean isNextBarrier() {
			return c++ % every == 0;
		}
	}

	protected static class MockInputGate implements InputGate {

		private int numChannels;
		private BufferPool[] bufferPools;
		private int[] currentBarriers;
		BarrierGenerator[] barrierGens;
		int currentChannel = 0;
		long c = 0;

		public MockInputGate(BufferPool[] bufferPools, BarrierGenerator[] barrierGens) {
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
		public void requestPartitions() throws IOException, InterruptedException {
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
			currentChannel = (currentChannel + 1) % numChannels;

			if (barrierGens[currentChannel].isNextBarrier()) {
				return BarrierBufferTest.createBarrier(++currentBarriers[currentChannel],
						currentChannel);
			} else {
				Buffer buffer = bufferPools[currentChannel].requestBuffer();
				buffer.getMemorySegment().putLong(0, c++);

				return new BufferOrEvent(buffer, currentChannel);
			}

		}

		@Override
		public void sendTaskEvent(TaskEvent event) throws IOException {
		}

		@Override
		public void registerListener(EventListener<InputGate> listener) {
		}

	}

	protected interface BarrierGenerator {
		public boolean isNextBarrier();
	}

}
