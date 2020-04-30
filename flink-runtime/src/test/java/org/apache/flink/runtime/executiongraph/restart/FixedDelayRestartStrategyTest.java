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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for the {@link FixedDelayRestartStrategy}.
 */
public class FixedDelayRestartStrategyTest {

	public final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

	public final ScheduledExecutor executor = new ScheduledExecutorServiceAdapter(executorService);

	@After
	public void shutdownExecutor() {
		executorService.shutdownNow();
	}

	// ------------------------------------------------------------------------

	@Test
	public void testNumberOfRestarts() throws Exception {
		final int numberRestarts = 10;

		final FixedDelayRestartStrategy strategy =
				new FixedDelayRestartStrategy(numberRestarts, 0L);

		for (int restartsLeft = numberRestarts; restartsLeft > 0; --restartsLeft) {
			// two calls to 'canRestart()' to make sure this is not used to maintain the counter
			assertTrue(strategy.canRestart());
			assertTrue(strategy.canRestart());

			strategy.restart(new NoOpRestarter(), executor);
		}

		assertFalse(strategy.canRestart());
	}

	@Test
	public void testDelay() throws Exception {
		final long restartDelay = 10;
		final int numberRestarts = 10;

		final FixedDelayRestartStrategy strategy =
				new FixedDelayRestartStrategy(numberRestarts, restartDelay);

		for (int restartsLeft = numberRestarts; restartsLeft > 0; --restartsLeft) {
			assertTrue(strategy.canRestart());

			final OneShotLatch sync = new OneShotLatch();
			final RestartCallback restarter = new LatchedRestarter(sync);

			final long time = System.nanoTime();
			strategy.restart(restarter, executor);
			sync.await();

			final long elapsed = System.nanoTime() - time;
			assertTrue("Not enough delay", elapsed >= restartDelay * 1_000_000);
		}

		assertFalse(strategy.canRestart());
	}
}
