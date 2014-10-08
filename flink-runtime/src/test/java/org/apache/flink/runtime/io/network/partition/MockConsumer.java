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
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class MockConsumer implements Callable<Boolean> {

	private static final int SLEEP_TIME_MS = 20;

	private final IntermediateResultPartitionQueueIterator iterator;

	private final boolean slowConsumer;

	private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

	public MockConsumer(IntermediateResultPartitionQueueIterator iterator, boolean slowConsumer) {
		this.iterator = iterator;
		this.slowConsumer = slowConsumer;
	}

	@Override
	public Boolean call() throws Exception {
		MockNotificationListener listener = new MockNotificationListener();

		int currentNumber = 0;

		try {
			while (true) {
				Buffer buffer = iterator.getNextBuffer();

				if (slowConsumer) {
					Thread.sleep(SLEEP_TIME_MS);
				}

				if (buffer == null) {
					if (iterator.subscribe(listener)) {
						listener.waitForNotification();
					}
					else if (iterator.isConsumed()) {
						break;
					}
				}
				else {
					try {
						if (buffer.isBuffer()) {
							currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
						}
					}
					finally {
						buffer.recycle();
					}
				}
			}
		}
		catch (Throwable t) {
			error.compareAndSet(null, t);
			return false;
		}

		return true;
	}

	public Throwable getError() {
		return error.get();
	}

	private int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
		MemorySegment segment = buffer.getMemorySegment();

		for (int i = 4; i < segment.size(); i += 4) {
			if (segment.getInt(i) != currentNumber++) {
				throw new IllegalStateException("Read unexpected number from buffer.");
			}
		}

		return currentNumber;
	}
}
