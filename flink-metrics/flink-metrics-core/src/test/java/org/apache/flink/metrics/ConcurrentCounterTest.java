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

package org.apache.flink.metrics;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link ConcurrentCounter}.
 */
public class ConcurrentCounterTest {

	@Test
	public void testConcurrentCount() throws Exception {
		final int threadCount = 10;
		final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
		try {
			final ConcurrentCounter counter = new ConcurrentCounter();
			final CountDownLatch startSignel = new CountDownLatch(1);
			final CountDownLatch doneSignal = new CountDownLatch(threadCount);
			final AtomicBoolean failed = new AtomicBoolean(false);
			final int loopTimes = 10;

			for (int i = 0; i < threadCount; i++) {
				executorService.submit(() -> {
					try {
						startSignel.await();
					} catch (InterruptedException e) {
						failed.set(true);
					}
					for (int j = 0; j < loopTimes; j++) {
						counter.inc();
						counter.dec(10L);
						counter.dec();
						counter.inc(20L);
					}
					doneSignal.countDown();
				});
			}

			startSignel.countDown();
			doneSignal.await();

			assertFalse(failed.get());
			final long result = 1L + 20L - 10L - 1L;
			assertEquals(result * loopTimes * threadCount, counter.getCount());
		} finally {
			executorService.shutdown();
		}
	}
}
