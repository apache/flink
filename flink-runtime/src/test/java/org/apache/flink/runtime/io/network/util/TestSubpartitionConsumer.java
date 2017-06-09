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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A test subpartition viewQueue consumer.
 *
 * <p> The behaviour of the consumer is customizable by specifying a callback.
 *
 * @see TestConsumerCallback
 */
public class TestSubpartitionConsumer implements Callable<Boolean>, BufferAvailabilityListener {

	private static final int MAX_SLEEP_TIME_MS = 20;

	/** The subpartition viewQueue to consume. */
	private volatile ResultSubpartitionView subpartitionView;

	private BlockingQueue<ResultSubpartitionView> viewQueue = new ArrayBlockingQueue<>(1);

	/**
	 * Flag indicating whether the consumer is slow. If true, the consumer will sleep a random
	 * number of milliseconds between returned buffers.
	 */
	private final boolean isSlowConsumer;

	/** The callback to handle a notifyNonEmpty buffer. */
	private final TestConsumerCallback callback;

	/** Random source for sleeps. */
	private final Random random;

	private final AtomicLong numBuffersAvailable = new AtomicLong();

	public TestSubpartitionConsumer(
		boolean isSlowConsumer,
		TestConsumerCallback callback) {

		this.isSlowConsumer = isSlowConsumer;
		this.random = isSlowConsumer ? new Random() : null;
		this.callback = checkNotNull(callback);
	}

	public void setSubpartitionView(ResultSubpartitionView subpartitionView) {
		this.subpartitionView = checkNotNull(subpartitionView);
	}

	@Override
	public Boolean call() throws Exception {
		try {
			while (true) {
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}

				if (numBuffersAvailable.get() == 0) {
					synchronized (numBuffersAvailable) {
						while (numBuffersAvailable.get() == 0) {
							numBuffersAvailable.wait();
						}
					}
				}

				final Buffer buffer = subpartitionView.getNextBuffer();

				if (isSlowConsumer) {
					Thread.sleep(random.nextInt(MAX_SLEEP_TIME_MS + 1));
				}

				if (buffer != null) {
					numBuffersAvailable.decrementAndGet();

					if (buffer.isBuffer()) {
						callback.onBuffer(buffer);
					} else {
						final AbstractEvent event = EventSerializer.fromBuffer(buffer,
							getClass().getClassLoader());

						callback.onEvent(event);

						buffer.recycle();

						if (event.getClass() == EndOfPartitionEvent.class) {
							subpartitionView.notifySubpartitionConsumed();

							return true;
						}
					}
				} else if (subpartitionView.isReleased()) {
					return true;
				}
			}
		} finally {
			subpartitionView.releaseAllResources();
		}
	}

	@Override
	public void notifyBuffersAvailable(long numBuffers) {
		if (numBuffers > 0 && numBuffersAvailable.getAndAdd(numBuffers) == 0) {
			synchronized (numBuffersAvailable) {
				numBuffersAvailable.notifyAll();
			}
			;
		}
	}
}
