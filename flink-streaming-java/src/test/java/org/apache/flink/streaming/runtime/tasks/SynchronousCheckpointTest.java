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
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the synchronous checkpoint execution at the {@link StreamTask}.
 */
public class SynchronousCheckpointTest {

	private static OneShotLatch runningLatch;
	private static OneShotLatch execLatch;

	private static AtomicReference<Throwable> error = new AtomicReference<>();

	@Before
	public void createQueuesAndActors() {
		runningLatch = new OneShotLatch();
		execLatch = new OneShotLatch();
		error.set(null);
	}

	@Test
	public void synchronousCheckpointBlocksUntilNotificationForCorrectCheckpointComes() throws Exception {
		final StreamTask streamTaskUnderTest = createTask();

		final Thread executingThread = launchOnSeparateThread(() -> {
			try {
				streamTaskUnderTest.invoke();
			} catch (Exception e) {
				error.set(e);
			}
		});

		runningLatch.await();

		final Thread checkpointingThread = launchOnSeparateThread(() -> {
			try {
				streamTaskUnderTest.triggerCheckpoint(
						new CheckpointMetaData(42, System.currentTimeMillis()),
						new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault())
				);
			} catch (Exception e) {
				error.set(e);
			}
		});

		final SynchronousSavepointLatch future = waitForSyncSavepointFutureToBeSet(streamTaskUnderTest);
		assertFalse(future.isCompleted());

		streamTaskUnderTest.notifyCheckpointComplete(41);
		assertFalse(future.isCompleted());

		streamTaskUnderTest.notifyCheckpointComplete(42);
		assertTrue(future.isCompleted());

		checkpointingThread.join();
		execLatch.trigger();
		executingThread.join();

		assertFalse(streamTaskUnderTest.isCanceled());
	}

	@Test
	public void cancelShouldAlsoCancelPendingSynchronousCheckpoint() throws Throwable {
		final StreamTask streamTaskUnderTest = createTask();

		final Thread executingThread = launchOnSeparateThread(() -> {
			try {
				streamTaskUnderTest.invoke();
			} catch (Exception e) {
				error.set(e);
			}
		});

		runningLatch.await();

		final Thread checkpointingThread = launchOnSeparateThread(() -> {
			try {
				streamTaskUnderTest.triggerCheckpoint(
						new CheckpointMetaData(42, System.currentTimeMillis()),
						new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault())
				);
			} catch (Exception e) {
				error.set(e);
			}
		});

		final SynchronousSavepointLatch future = waitForSyncSavepointFutureToBeSet(streamTaskUnderTest);
		assertFalse(future.isCompleted());

		execLatch.trigger();

		assertFalse(future.isCompleted());
		streamTaskUnderTest.cancel();
		assertTrue(future.isCanceled());

		checkpointingThread.join();
		executingThread.join();

		assertTrue(streamTaskUnderTest.isCanceled());
	}

	private SynchronousSavepointLatch waitForSyncSavepointFutureToBeSet(final StreamTask streamTaskUnderTest) throws InterruptedException {

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

	private StreamTask createTask() {

		final DummyEnvironment environment =
				new DummyEnvironment("test", 1, 0);

		return new StreamTask(environment, null) {

			@Override
			protected void init() {}

			@Override
			protected void run() throws Exception {
				runningLatch.trigger();
				execLatch.await();
			}

			@Override
			protected void cleanup() {}

			@Override
			protected void cancelTask() {}
		};
	}
}
