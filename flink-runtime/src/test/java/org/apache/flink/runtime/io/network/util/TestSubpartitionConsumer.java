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
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionData;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A test subpartition viewQueue consumer.
 *
 * <p> The behaviour of the consumer is customizable by specifying a callback.
 *
 * @see TestConsumerCallback
 */
@Deprecated
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

	private final AtomicBoolean dataAvailableNotification = new AtomicBoolean(false);

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

				synchronized (dataAvailableNotification) {
					while (!dataAvailableNotification.getAndSet(false)) {
						dataAvailableNotification.wait();
					}
				}

				final PartitionData partitionData = subpartitionView.getNextData();

				if (isSlowConsumer) {
					Thread.sleep(random.nextInt(MAX_SLEEP_TIME_MS + 1));
				}

				if (partitionData != null) {
					if (partitionData.isDataAvailable()) {
						dataAvailableNotification.set(true);
					}
					if (partitionData.buffer().isBuffer()) {
						callback.onBuffer(partitionData.buffer());
					} else {
						final AbstractEvent event = EventSerializer.fromBuffer(partitionData.buffer(),
							getClass().getClassLoader());

						callback.onEvent(event);

						partitionData.buffer().recycleBuffer();

						if (event.getClass() == EndOfPartitionEvent.class) {
							subpartitionView.releaseAllResources();

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
	public void notifyDataAvailable() {
		synchronized (dataAvailableNotification) {
			dataAvailableNotification.set(true);
			dataAvailableNotification.notifyAll();
		}
	}
}
