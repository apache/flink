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

import org.apache.flink.util.function.RunnableWithException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
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
	public void triggerUnblocksWait() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		assertFalse(latchUnderTest.isWaiting());

		Future<Void> future = runThreadWaitingForCheckpointAck(latchUnderTest);
		while (!latchUnderTest.isWaiting()) {
			Thread.sleep(5L);
		}

		final AtomicBoolean triggered = new AtomicBoolean();

		// wrong checkpoint id.
		latchUnderTest.acknowledgeCheckpointAndTrigger(2L, () -> triggered.set(true));
		assertFalse(triggered.get());
		assertFalse(latchUnderTest.isCompleted());
		assertTrue(latchUnderTest.isWaiting());

		latchUnderTest.acknowledgeCheckpointAndTrigger(1L, () -> triggered.set(true));
		assertTrue(triggered.get());
		assertTrue(latchUnderTest.isCompleted());

		future.get();
		assertFalse(latchUnderTest.isWaiting());
	}

	@Test
	public void cancelUnblocksWait() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		assertFalse(latchUnderTest.isWaiting());

		Future<Void> future = runThreadWaitingForCheckpointAck(latchUnderTest);
		while (!latchUnderTest.isWaiting()) {
			Thread.sleep(5L);
		}

		latchUnderTest.cancelCheckpointLatch();
		assertTrue(latchUnderTest.isCanceled());

		future.get();
		assertFalse(latchUnderTest.isWaiting());
	}

	@Test
	public void waitAfterTriggerIsNotBlocking() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		latchUnderTest.acknowledgeCheckpointAndTrigger(1L, () -> {});

		latchUnderTest.blockUntilCheckpointIsAcknowledged();
	}

	@Test
	public void waitAfterCancelIsNotBlocking() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		latchUnderTest.cancelCheckpointLatch();
		assertTrue(latchUnderTest.isCanceled());

		latchUnderTest.blockUntilCheckpointIsAcknowledged();
	}

	@Test
	public void triggeringInvokesCallbackAtMostOnce() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);

		AtomicInteger counter = new AtomicInteger();
		Future<Void> future1 = runThreadTriggeringCheckpoint(latchUnderTest, 1L, counter::incrementAndGet);
		Future<Void> future2 = runThreadTriggeringCheckpoint(latchUnderTest, 1L, counter::incrementAndGet);
		Future<Void> future3 = runThreadTriggeringCheckpoint(latchUnderTest, 1L, counter::incrementAndGet);
		future1.get();
		future2.get();
		future3.get();

		assertEquals(1, counter.get());
	}

	@Test
	public void triggeringAfterCancelDoesNotInvokeCallback() throws Exception {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		latchUnderTest.cancelCheckpointLatch();
		assertTrue(latchUnderTest.isCanceled());

		final AtomicBoolean triggered = new AtomicBoolean();
		latchUnderTest.acknowledgeCheckpointAndTrigger(1L, () -> triggered.set(true));
		assertFalse(triggered.get());
	}

	@Test
	public void checkpointIdIsSetOnlyOnce() {
		final SynchronousSavepointLatch latchUnderTest = new SynchronousSavepointLatch();

		latchUnderTest.setCheckpointId(1L);
		assertTrue(latchUnderTest.isSet());
		assertEquals(1L, latchUnderTest.getCheckpointId());

		latchUnderTest.setCheckpointId(2L);
		assertTrue(latchUnderTest.isSet());
		assertEquals(1L, latchUnderTest.getCheckpointId());
	}

	private Future<Void> runThreadWaitingForCheckpointAck(SynchronousSavepointLatch latch) {
		return executors.submit(() -> {
			latch.blockUntilCheckpointIsAcknowledged();
			return null;
		});
	}

	private Future<Void> runThreadTriggeringCheckpoint(SynchronousSavepointLatch latch, long checkpointId, RunnableWithException runnable) {
		return executors.submit(() -> {
			latch.acknowledgeCheckpointAndTrigger(checkpointId, runnable);
			return null;
		});
	}
}
