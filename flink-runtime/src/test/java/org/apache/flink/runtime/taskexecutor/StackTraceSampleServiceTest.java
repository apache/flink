/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StackTraceSampleService}.
 */
public class StackTraceSampleServiceTest extends TestLogger {

	private ScheduledExecutorService scheduledExecutorService;

	private StackTraceSampleService stackTraceSampleService;

	@Before
	public void setUp() throws Exception {
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(scheduledExecutorService);

		stackTraceSampleService = new StackTraceSampleService(scheduledExecutor);
	}

	@After
	public void tearDown() throws Exception {
		if (scheduledExecutorService != null) {
			ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, scheduledExecutorService);
		}
	}

	@Test
	public void testShouldReturnStackTraces() throws Exception {
		final int numSamples = 10;
		final List<StackTraceElement[]> stackTraces = stackTraceSampleService.requestStackTraceSample(
			new TestTask(),
			numSamples,
			Time.milliseconds(0),
			-1).get();

		assertThat(stackTraces, hasSize(numSamples));
		final StackTraceElement[] firstStackTrace = stackTraces.get(0);
		assertThat(firstStackTrace[1].getClassName(), is(equalTo((TestTask.class.getName()))));
	}

	@Test
	public void testShouldThrowExceptionIfNumSamplesIsNegative() {
		try {
			stackTraceSampleService.requestStackTraceSample(
				new TestTask(),
				-1,
				Time.milliseconds(0),
				10);
			fail("Expected exception not thrown");
		} catch (final IllegalArgumentException e) {
			assertThat(e.getMessage(), is(equalTo("numSamples must be positive")));
		}
	}

	@Test
	public void testShouldTruncateStackTraceIfLimitIsSpecified() throws Exception {
		final int maxStackTraceDepth = 1;
		final List<StackTraceElement[]> stackTraces = stackTraceSampleService.requestStackTraceSample(
			new TestTask(),
			10,
			Time.milliseconds(0),
			maxStackTraceDepth).get();

		assertThat(stackTraces.get(0), is(arrayWithSize(maxStackTraceDepth)));
	}

	@Test
	public void testShouldReturnPartialResultIfTaskStopsRunningDuringSampling() throws Exception {
		final List<StackTraceElement[]> stackTraces = stackTraceSampleService.requestStackTraceSample(
			new NotRunningAfterBeingSampledTask(),
			10,
			Time.milliseconds(0),
			1).get();

		assertThat(stackTraces, hasSize(lessThan(10)));
	}

	@Test
	public void testShouldThrowExceptionIfTaskIsNotRunningBeforeSampling() {
		try {
			stackTraceSampleService.requestStackTraceSample(
				new NotRunningTask(),
				10,
				Time.milliseconds(0),
				-1);
			fail("Expected exception not thrown");
		} catch (final IllegalStateException e) {
			assertThat(e.getMessage(), containsString("Cannot sample task"));
		}
	}

	/**
	 * Task that is always running.
	 */
	private static class TestTask implements StackTraceSampleableTask {

		private final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

		@Override
		public boolean isRunning() {
			return true;
		}

		@Override
		public StackTraceElement[] getStackTrace() {
			return Thread.currentThread().getStackTrace();
		}

		@Override
		public ExecutionAttemptID getExecutionId() {
			return executionAttemptID;
		}
	}

	/**
	 * Task that stops running after being sampled for the first time.
	 */
	private static class NotRunningAfterBeingSampledTask extends TestTask {

		private volatile boolean stackTraceSampled;

		@Override
		public boolean isRunning() {
			return !stackTraceSampled;
		}

		@Override
		public StackTraceElement[] getStackTrace() {
			stackTraceSampled = true;
			return super.getStackTrace();
		}
	}

	/**
	 * Task that never runs.
	 */
	private static class NotRunningTask extends TestTask {

		@Override
		public boolean isRunning() {
			return false;
		}

	}
}
