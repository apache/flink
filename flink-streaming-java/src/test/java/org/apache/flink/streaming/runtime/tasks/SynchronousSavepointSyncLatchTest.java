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

package org.apache.flink.streaming.runtime.tasks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link SynchronousSavepointLatch}.
 */
public class SynchronousSavepointSyncLatchTest {

	private ExecutorService executors;

	@Before
	public void startExecutorService() {
		executors = Executors.newCachedThreadPool();
	}

	@After
	public void terminateExecutors() throws InterruptedException {
		while (!executors.isTerminated()) {
			executors.shutdownNow();
			executors.awaitTermination(10, TimeUnit.SECONDS);
		}
	}

	@Test
	public void waitAndThenTriggerWorks() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();
		final WaitingOnLatchCallable callable = new WaitingOnLatchCallable(latchUnderTest, 1L);

		executors.submit(callable);

		while (!latchUnderTest.isSet()) {
			Thread.sleep(5L);
		}

		// wrong checkpoint id.
		latchUnderTest.acknowledgeCheckpointAndTrigger(2L);
		assertTrue(latchUnderTest.isWaiting());

		latchUnderTest.acknowledgeCheckpointAndTrigger(1L);
		assertTrue(latchUnderTest.isCompleted());
	}

	@Test
	public void waitAndThenCancelWorks() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();
		final WaitingOnLatchCallable callable = new WaitingOnLatchCallable(latchUnderTest, 1L);

		final Future<Boolean> resultFuture = executors.submit(callable);

		while (!latchUnderTest.isSet()) {
			Thread.sleep(5L);
		}

		latchUnderTest.cancelCheckpointLatch();

		boolean result = resultFuture.get();

		assertFalse(result);
		assertTrue(latchUnderTest.isCanceled());
	}

	@Test
	public void triggeringReturnsTrueAtMostOnce() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		final WaitingOnLatchCallable firstCallable = new WaitingOnLatchCallable(latchUnderTest, 1L);
		final WaitingOnLatchCallable secondCallable = new WaitingOnLatchCallable(latchUnderTest, 1L);

		final Future<Boolean> firstFuture = executors.submit(firstCallable);
		final Future<Boolean> secondFuture = executors.submit(secondCallable);

		while (!latchUnderTest.isSet()) {
			Thread.sleep(5L);
		}

		latchUnderTest.acknowledgeCheckpointAndTrigger(1L);

		final boolean firstResult = firstFuture.get();
		final boolean secondResult = secondFuture.get();

		// only one of the two can be true (it is a race so we do not know which one)
		assertTrue(firstResult ^ secondResult);
	}

	@Test
	public void waitAfterTriggerReturnsTrueImmediately() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();
		latchUnderTest.setCheckpointId(1L);
		latchUnderTest.acknowledgeCheckpointAndTrigger(1L);
		final boolean triggerred = latchUnderTest.blockUntilCheckpointIsAcknowledged();
		assertTrue(triggerred);
	}

	@Test
	public void waitAfterCancelDoesNothing() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();
		latchUnderTest.setCheckpointId(1L);
		latchUnderTest.cancelCheckpointLatch();
		latchUnderTest.blockUntilCheckpointIsAcknowledged();
	}

	@Test
	public void checkpointIdIsSetOnlyOnce() throws InterruptedException {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		final WaitingOnLatchCallable firstCallable = new WaitingOnLatchCallable(latchUnderTest, 1L);
		executors.submit(firstCallable);

		while (!latchUnderTest.isSet()) {
			Thread.sleep(5L);
		}

		final WaitingOnLatchCallable secondCallable = new WaitingOnLatchCallable(latchUnderTest, 2L);
		executors.submit(secondCallable);

		latchUnderTest.acknowledgeCheckpointAndTrigger(2L);
		assertTrue(latchUnderTest.isWaiting());

		latchUnderTest.acknowledgeCheckpointAndTrigger(1L);
		assertTrue(latchUnderTest.isCompleted());
	}

	private static final class WaitingOnLatchCallable implements Callable<Boolean> {

		private final SynchronousSavepointLatch latch;
		private final long checkpointId;

		WaitingOnLatchCallable(
				final SynchronousSavepointLatch latch,
				final long checkpointId) {
			this.latch = checkNotNull(latch);
			this.checkpointId = checkpointId;
		}

		@Override
		public Boolean call() throws Exception {
			latch.setCheckpointId(checkpointId);
			return latch.blockUntilCheckpointIsAcknowledged();
		}
	}
}
