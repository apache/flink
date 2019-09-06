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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the synchronous checkpoint execution at the {@link StreamTask}.
 */
public class SynchronousCheckpointTest {

	private OneShotLatch execLatch;

	private AtomicReference<Throwable> error;

	private StreamTask streamTaskUnderTest;
	private Thread mainThreadExecutingTaskUnderTest;
	private Thread checkpointingThread;

	@Before
	public void setupTestEnvironment() throws InterruptedException {
		final OneShotLatch runningLatch = new OneShotLatch();

		execLatch = new OneShotLatch();
		error = new AtomicReference<>();

		mainThreadExecutingTaskUnderTest = launchOnSeparateThread(() -> {
			streamTaskUnderTest = createTask(runningLatch, execLatch);
			try {
				streamTaskUnderTest.invoke();
			} catch (Exception e) {
				error.set(e);
			}
		});
		runningLatch.await();
	}

	@Test(timeout = 1000)
	public void synchronousCheckpointBlocksUntilNotificationForCorrectCheckpointComes() throws Exception {
		final SynchronousSavepointLatch syncSavepointLatch = launchSynchronousSavepointAndGetTheLatch();
		assertFalse(syncSavepointLatch.isCompleted());

		streamTaskUnderTest.notifyCheckpointComplete(41);
		assertFalse(syncSavepointLatch.isCompleted());

		streamTaskUnderTest.notifyCheckpointComplete(42);
		assertTrue(syncSavepointLatch.isCompleted());

		waitUntilCheckpointingThreadIsFinished();
		allowTaskToExitTheRunLoop();
		waitUntilMainExecutionThreadIsFinished();

		assertFalse(streamTaskUnderTest.isCanceled());
	}

	@Test(timeout = 1000)
	public void cancelShouldAlsoCancelPendingSynchronousCheckpoint() throws Throwable {
		final SynchronousSavepointLatch syncSavepointLatch = launchSynchronousSavepointAndGetTheLatch();
		assertFalse(syncSavepointLatch.isCompleted());

		allowTaskToExitTheRunLoop();

		assertFalse(syncSavepointLatch.isCompleted());
		streamTaskUnderTest.cancel();
		assertTrue(syncSavepointLatch.isCanceled());

		waitUntilCheckpointingThreadIsFinished();
		waitUntilMainExecutionThreadIsFinished();

		assertTrue(streamTaskUnderTest.isCanceled());
	}

	private SynchronousSavepointLatch launchSynchronousSavepointAndGetTheLatch() throws InterruptedException {
		checkpointingThread = launchOnSeparateThread(() -> {
			try {
				streamTaskUnderTest.triggerCheckpoint(
						new CheckpointMetaData(42, System.currentTimeMillis()),
						new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
						false
				);
			} catch (Exception e) {
				error.set(e);
			}
		});
		return waitForSyncSavepointLatchToBeSet(streamTaskUnderTest);
	}

	private void waitUntilMainExecutionThreadIsFinished() throws InterruptedException {
		mainThreadExecutingTaskUnderTest.join();
	}

	private void waitUntilCheckpointingThreadIsFinished() throws InterruptedException {
		checkpointingThread.join();
	}

	private void allowTaskToExitTheRunLoop() {
		execLatch.trigger();
	}

	private SynchronousSavepointLatch waitForSyncSavepointLatchToBeSet(final StreamTask streamTaskUnderTest) throws InterruptedException {

		SynchronousSavepointLatch syncSavepointFuture = streamTaskUnderTest.getSynchronousSavepointLatch();
		while (!syncSavepointFuture.isSet()) {
			Thread.sleep(10L);

			if (error.get() != null && !(error.get() instanceof CancelTaskException)) {
				fail();
			}
		}
		return syncSavepointFuture;
	}

	private Thread launchOnSeparateThread(final Runnable runnable) {
		final Thread thread = new Thread(runnable);
		thread.start();
		return thread;
	}

	private StreamTask createTask(final OneShotLatch runningLatch, final OneShotLatch execLatch) {
		final DummyEnvironment environment =
				new DummyEnvironment("test", 1, 0);
		return new StreamTaskUnderTest(environment, runningLatch, execLatch);
	}

	private static class StreamTaskUnderTest extends NoOpStreamTask {

		private final OneShotLatch runningLatch;
		private final OneShotLatch execLatch;

		StreamTaskUnderTest(
				final Environment env,
				final OneShotLatch runningLatch,
				final OneShotLatch execLatch) {
			super(env);
			this.runningLatch = checkNotNull(runningLatch);
			this.execLatch = checkNotNull(execLatch);
		}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			runningLatch.trigger();
			execLatch.await();
			super.processInput(context);
		}
	}
}
