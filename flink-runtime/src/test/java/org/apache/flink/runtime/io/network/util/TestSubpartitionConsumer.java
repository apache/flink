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
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.util.Random;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A test subpartition view consumer.
 *
 * <p> The behaviour of the consumer is customizable by specifying a callback.
 *
 * @see TestConsumerCallback
 */
public class TestSubpartitionConsumer implements Callable<Boolean> {

	private static final int MAX_SLEEP_TIME_MS = 20;

	/** The subpartition view to consume. */
	private final ResultSubpartitionView subpartitionView;

	/**
	 * Flag indicating whether the consumer is slow. If true, the consumer will sleep a random
	 * number of milliseconds between returned buffers.
	 */
	private final boolean isSlowConsumer;

	/** The callback to handle a read buffer. */
	private final TestConsumerCallback callback;

	/** Random source for sleeps. */
	private final Random random;

	public TestSubpartitionConsumer(
			ResultSubpartitionView subpartitionView,
			boolean isSlowConsumer,
			TestConsumerCallback callback) {

		this.subpartitionView = checkNotNull(subpartitionView);
		this.isSlowConsumer = isSlowConsumer;
		this.random = isSlowConsumer ? new Random() : null;
		this.callback = checkNotNull(callback);
	}

	@Override
	public Boolean call() throws Exception {
		final TestNotificationListener listener = new TestNotificationListener();

		try {
			while (true) {
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}

				final Buffer buffer = subpartitionView.getNextBuffer();

				if (isSlowConsumer) {
					Thread.sleep(random.nextInt(MAX_SLEEP_TIME_MS + 1));
				}

				if (buffer != null) {
					if (buffer.isBuffer()) {
						callback.onBuffer(buffer);
					}
					else {
						final AbstractEvent event = EventSerializer.fromBuffer(buffer,
								getClass().getClassLoader());

						callback.onEvent(event);

						buffer.recycle();

						if (event.getClass() == EndOfPartitionEvent.class) {
							subpartitionView.notifySubpartitionConsumed();

							return true;
						}
					}
				}
				else {
					int current = listener.getNumberOfNotifications();

					if (subpartitionView.registerListener(listener)) {
						listener.waitForNotification(current);
					}
					else if (subpartitionView.isReleased()) {
						return true;
					}
				}
			}
		}
		finally {
			subpartitionView.releaseAllResources();
		}
	}

}
