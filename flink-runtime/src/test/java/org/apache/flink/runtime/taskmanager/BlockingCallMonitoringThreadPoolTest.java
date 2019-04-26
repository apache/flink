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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BlockingCallMonitoringThreadPool}.
 */
public class BlockingCallMonitoringThreadPoolTest {

	private final static int TIME_OUT = 30;

	private final OneShotLatch latch1 = new OneShotLatch();
	private final OneShotLatch latch2 = new OneShotLatch();
	private BlockingCallMonitoringThreadPool blockingCallThreadPool = new BlockingCallMonitoringThreadPool();

	@Before
	public void setup() {
		blockingCallThreadPool = new BlockingCallMonitoringThreadPool();
		latch1.reset();
		latch2.reset();
	}

	@After
	public void tearDown() {
		latch1.trigger();
		latch2.trigger();
		blockingCallThreadPool.shutdown();
	}

	@Test
	public void testSubmitNonBlockingCalls() throws Exception {
		blockingCallThreadPool.submit(() -> await(latch1), false);
		blockingCallThreadPool.submit(() -> await(latch2), false);

		assertEquals(1, blockingCallThreadPool.getMaximumPoolSize());
		assertEquals(1, blockingCallThreadPool.getQueueSize());
	}

	@Test
	public void testSubmitBlockingCall() throws Exception {
		CompletableFuture<?> latch1Future = blockingCallThreadPool.submit(() -> await(latch1), true);
		CompletableFuture<?> latch2Future = blockingCallThreadPool.submit(() -> await(latch2), false);

		assertEquals(2, blockingCallThreadPool.getMaximumPoolSize());
		assertEquals(0, blockingCallThreadPool.getQueueSize());

		latch2.trigger();
		latch2Future.get(TIME_OUT, TimeUnit.SECONDS);

		assertFalse(latch1Future.isDone());
		assertTrue(latch2Future.isDone());
	}

	@Test
	public void testDownsizePool() throws Exception {
		List<CompletableFuture<?>> futures = new ArrayList<>();

		futures.add(blockingCallThreadPool.submit(() -> await(latch1), true));
		futures.add(blockingCallThreadPool.submit(() -> await(latch1), true));
		futures.add(blockingCallThreadPool.submit(() -> await(latch1), false));

		assertEquals(3, blockingCallThreadPool.getMaximumPoolSize());

		latch1.trigger();

		for (CompletableFuture<?> future : futures) {
			future.get(TIME_OUT, TimeUnit.SECONDS);
		}

		blockingCallThreadPool.submit(() -> await(latch1), false).get(TIME_OUT, TimeUnit.SECONDS);
		assertEquals(1, blockingCallThreadPool.getMaximumPoolSize());
	}

	private void await(OneShotLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
