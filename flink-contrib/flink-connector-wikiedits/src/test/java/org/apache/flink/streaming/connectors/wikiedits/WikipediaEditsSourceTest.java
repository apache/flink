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

package org.apache.flink.streaming.connectors.wikiedits;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the WikipediaEditsSource.
 */
public class WikipediaEditsSourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(WikipediaEditsSourceTest.class);

	@Rule
	public RetryRule retryRule = new RetryRule();

	/**
	 * We first check the connection to the IRC server. If it fails, this test is ignored.
	 */
	@Test
	@RetryOnFailure(times = 1)
	public void testWikipediaEditsSource() throws Exception {
		if (canConnect(1, TimeUnit.SECONDS)) {
			final Time testTimeout = Time.seconds(60);
			final WikipediaEditsSource wikipediaEditsSource = new WikipediaEditsSource();

			ExecutorService executorService = null;
			try {
				executorService = Executors.newSingleThreadExecutor();
				BlockingQueue<Object> collectedEvents = new ArrayBlockingQueue<>(1);
				AtomicReference<Exception> asyncError = new AtomicReference<>();

				// Execute the source in a different thread and collect events into the queue.
				// We do this in a separate thread in order to not block the main test thread
				// indefinitely in case that something bad happens (like not receiving any
				// events)
				executorService.execute(() -> {
					try {
						wikipediaEditsSource.run(new CollectingSourceContext<>(collectedEvents));
					} catch (Exception e) {
						boolean interrupted = e instanceof InterruptedException;
						if (!interrupted) {
							LOG.warn("Failure in WikipediaEditsSource", e);
						}

						asyncError.compareAndSet(null, e);
					}
				});

				long deadline = deadlineNanos(testTimeout);

				Object event = null;
				Exception error = null;

				// Check event or error
				while (event == null && error == null && System.nanoTime() < deadline) {
					event = collectedEvents.poll(1, TimeUnit.SECONDS);
					error = asyncError.get();
				}

				if (error != null) {
					// We don't use assertNull, because we want to include the error message
					fail("Failure in WikipediaEditsSource: " + error.getMessage());
				}

				assertNotNull("Did not receive a WikipediaEditEvent within the desired timeout", event);
				assertTrue("Received unexpected event " + event, event instanceof WikipediaEditEvent);
			} finally {
				wikipediaEditsSource.cancel();

				if (executorService != null) {
					executorService.shutdownNow();
					executorService.awaitTermination(1, TimeUnit.SECONDS);
				}
			}
		} else {
			LOG.info("Skipping test, because not able to connect to IRC server.");
		}
	}

	private long deadlineNanos(Time testTimeout) {
		return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(testTimeout.toMilliseconds());
	}

	private static class CollectingSourceContext<T> implements SourceFunction.SourceContext<T> {

		private final BlockingQueue<Object> events;

		private CollectingSourceContext(BlockingQueue<Object> events) {
			this.events = Objects.requireNonNull(events, "events");
		}

		@Override
		public void collect(T element) {
			events.offer(element);
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			throw new UnsupportedOperationException();

		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void markAsTemporarilyIdle() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getCheckpointLock() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
		}

	}

	private static boolean canConnect(int timeout, TimeUnit unit) {
		String host = WikipediaEditsSource.DEFAULT_HOST;
		int port = WikipediaEditsSource.DEFAULT_PORT;

		try (Socket s = new Socket()) {
			s.connect(new InetSocketAddress(host, port), (int) unit.toMillis(timeout));
			return s.isConnected();
		} catch (Throwable ignored) {
		}

		return false;
	}
}
