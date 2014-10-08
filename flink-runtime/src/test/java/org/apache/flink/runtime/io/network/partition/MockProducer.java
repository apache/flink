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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueue;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockProducer implements Callable<Boolean> {

	private static final int SLEEP_TIME_MS = 20;

	private final IntermediateResultPartitionQueue queue;

	private final BufferPool bufferPool;

	private final int numBuffersToProduce;

	private final boolean slowProducer;

	private final AtomicInteger discardAfter = new AtomicInteger(Integer.MAX_VALUE);

	private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

	public MockProducer(IntermediateResultPartitionQueue queue, BufferPool bufferPool, int numBuffersToProduce, boolean slowProducer) {
		this.queue = queue;
		this.bufferPool = bufferPool;
		this.numBuffersToProduce = numBuffersToProduce;
		this.slowProducer = slowProducer;
	}

	@Override
	public Boolean call() throws Exception {
		try {
			int currentNumber = 0;

			for (int i = 0; i < numBuffersToProduce; i++) {
				if (i >= discardAfter.get()) {
					queue.discard();
					return true;
				}

				Buffer buffer = bufferPool.requestBufferBlocking();

				currentNumber = fillBufferWithAscendingNumbers(buffer, currentNumber);

				queue.add(buffer);

				if (slowProducer) {
					Thread.sleep(SLEEP_TIME_MS);
				}
			}

			queue.finish();
		}
		catch (Throwable t) {
			error.compareAndSet(null, t);
			return false;
		}

		return true;
	}

	void discard() {
		discardAfter.set(0);
	}

	public void discardAfter(int numBuffers) {
		discardAfter.set(numBuffers);
	}

	public Throwable getError() {
		return error.get();
	}

	public static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		for (int i = 4; i < segment.size(); i += 4) {
			segment.putInt(i, currentNumber++);
		}

		return currentNumber;
	}
}
