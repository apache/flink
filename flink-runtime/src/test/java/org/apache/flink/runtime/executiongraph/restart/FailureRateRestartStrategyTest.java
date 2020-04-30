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

import org.apache.flink.api.common.time.Time;
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
 * Unit test for the {@link FailureRateRestartStrategy}.
 */
public class FailureRateRestartStrategyTest {

	public final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

	public final ScheduledExecutor executor = new ScheduledExecutorServiceAdapter(executorService);

	@After
	public void shutdownExecutor() {
		executorService.shutdownNow();
	}

	// ------------------------------------------------------------------------

	@Test
	public void testManyFailuresWithinRate() throws Exception {
		final int numAttempts = 10;
		final int intervalMillis = 1;

		final FailureRateRestartStrategy restartStrategy =
				new FailureRateRestartStrategy(1, Time.milliseconds(intervalMillis), Time.milliseconds(0));

		for (int attempsLeft = numAttempts; attempsLeft > 0; --attempsLeft) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.restart(new NoOpRestarter(), executor);
			sleepGuaranteed(2 * intervalMillis);
		}

		assertTrue(restartStrategy.canRestart());
	}

	@Test
	public void testFailuresExceedingRate() throws Exception {
		final int numFailures = 3;
		final int intervalMillis = 10_000;

		final FailureRateRestartStrategy restartStrategy =
				new FailureRateRestartStrategy(numFailures, Time.milliseconds(intervalMillis), Time.milliseconds(0));

		for (int failuresLeft = numFailures; failuresLeft > 0; --failuresLeft) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.restart(new NoOpRestarter(), executor);
		}

		// now the rate should be exceeded
		assertFalse(restartStrategy.canRestart());
	}

	@Test
	public void testDelay() throws Exception {
		final long restartDelay = 2;
		final int numberRestarts = 10;

		final FailureRateRestartStrategy strategy =
			new FailureRateRestartStrategy(numberRestarts + 1, Time.milliseconds(1), Time.milliseconds(restartDelay));

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
	}

	// ------------------------------------------------------------------------

	/**
	 * This method makes sure that the actual interval and is not spuriously waking up.
	 */
	private static void sleepGuaranteed(long millis) throws InterruptedException {
		final long deadline = System.nanoTime() + millis * 1_000_000;

		long nanosToSleep;
		while ((nanosToSleep = deadline - System.nanoTime()) > 0) {
			long millisToSleep = nanosToSleep / 1_000_000;
			if (nanosToSleep % 1_000_000 != 0) {
				millisToSleep++;
			}

			Thread.sleep(millisToSleep);
		}
	}
	
	
}
