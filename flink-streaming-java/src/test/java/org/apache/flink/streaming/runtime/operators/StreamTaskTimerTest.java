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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the timer service of {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
 */
@SuppressWarnings("serial")
public class StreamTaskTimerTest extends TestLogger {

	private StreamTaskTestHarness<?> testHarness;
	private ProcessingTimeService timeService;

	@Before
	public void setup() throws Exception {
		testHarness = startTestHarness();

		StreamTask<?, ?> task = testHarness.getTask();
		timeService = task.getProcessingTimeServiceFactory().createProcessingTimeService(
			task.getMailboxExecutorFactory().createExecutor(testHarness.getStreamConfig().getChainIndex()));
	}

	@After
	public void teardown() throws Exception {
		stopTestHarness(testHarness, 4000L);
	}

	@Test
	public void testOpenCloseAndTimestamps() {
		// first one spawns thread
		timeService.registerTimer(System.currentTimeMillis(), timestamp -> {});

		assertEquals(1, StreamTask.TRIGGER_THREAD_GROUP.activeCount());
	}

	@Test
	public void testErrorReporting() throws Exception {
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		OneShotLatch latch = new OneShotLatch();
		testHarness.getEnvironment().setExternalExceptionHandler(ex -> {
			errorRef.set(ex);
			latch.trigger();
		});

		ProcessingTimeCallback callback = timestamp -> {
			throw new Exception("Exception in Timer");
		};

		timeService.registerTimer(System.currentTimeMillis(), callback);
		latch.await();
		assertThat(errorRef.get(), instanceOf(Exception.class));
	}

	@Test
	public void checkScheduledTimestamps() throws Exception {
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final long t1 = System.currentTimeMillis();
		final long t2 = System.currentTimeMillis() - 200;
		final long t3 = System.currentTimeMillis() + 100;
		final long t4 = System.currentTimeMillis() + 200;

		timeService.registerTimer(t1, new ValidatingProcessingTimeCallback(errorRef, t1, 0));
		timeService.registerTimer(t2, new ValidatingProcessingTimeCallback(errorRef, t2, 1));
		timeService.registerTimer(t3, new ValidatingProcessingTimeCallback(errorRef, t3, 2));
		timeService.registerTimer(t4, new ValidatingProcessingTimeCallback(errorRef, t4, 3));

		long deadline = System.currentTimeMillis() + 20000;
		while (errorRef.get() == null &&
				ValidatingProcessingTimeCallback.numInSequence < 4 &&
				System.currentTimeMillis() < deadline) {
			Thread.sleep(100);
		}

		verifyNoException(errorRef.get());
		assertEquals(4, ValidatingProcessingTimeCallback.numInSequence);
	}

	private static class ValidatingProcessingTimeCallback implements ProcessingTimeCallback {

		static int numInSequence;

		private final AtomicReference<Throwable> errorRef;

		private final long expectedTimestamp;
		private final int expectedInSequence;

		private ValidatingProcessingTimeCallback(AtomicReference<Throwable> errorRef, long expectedTimestamp, int expectedInSequence) {
			this.errorRef = errorRef;
			this.expectedTimestamp = expectedTimestamp;
			this.expectedInSequence = expectedInSequence;
		}

		@Override
		public void onProcessingTime(long timestamp) {
			try {
				assertEquals(expectedTimestamp, timestamp);
				assertEquals(expectedInSequence, numInSequence);
				numInSequence++;
			}
			catch (Throwable t) {
				errorRef.compareAndSet(null, t);
			}
		}
	}

	private static void verifyNoException(@Nullable Throwable exception) {
		if (exception != null) {
			exception.printStackTrace();
			fail(exception.getMessage());
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Identity mapper.
	 */
	public static class DummyMapFunction<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) {
			return value;
		}
	}

	private StreamTaskTestHarness<?> startTestHarness() throws Exception {
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setChainIndex(0);
		streamConfig.setStreamOperator(new StreamMap<String, String>(new DummyMapFunction<>()));

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		return testHarness;
	}

	private void stopTestHarness(StreamTaskTestHarness<?> testHarness, long timeout) throws Exception {
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		// thread needs to die in time
		long deadline = System.currentTimeMillis() + timeout;
		while (StreamTask.TRIGGER_THREAD_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}

		assertEquals("Trigger timer thread did not properly shut down",
				0, StreamTask.TRIGGER_THREAD_GROUP.activeCount());
	}
}
