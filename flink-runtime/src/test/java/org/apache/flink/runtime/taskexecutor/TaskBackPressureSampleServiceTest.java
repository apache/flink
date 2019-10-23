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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link TaskBackPressureSampleService}.
 */
public class TaskBackPressureSampleServiceTest extends TestLogger {

	private ScheduledExecutorService scheduledExecutorService;

	private TaskBackPressureSampleService backPressureSampleService;

	@Before
	public void setUp() throws Exception {
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(scheduledExecutorService);

		backPressureSampleService = new TaskBackPressureSampleService(scheduledExecutor);
	}

	@After
	public void tearDown() throws Exception {
		if (scheduledExecutorService != null) {
			scheduledExecutorService.shutdown();
		}
	}

	@Test(timeout = 10000L)
	public void testSampleTaskBackPressure() throws Exception {
		final double backPressureRatio = backPressureSampleService.sampleTaskBackPressure(
			new TestTask(),
			10,
			Time.milliseconds(0)).get();

		assertEquals(0.5, backPressureRatio, 0.0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testShouldThrowExceptionIfNumSamplesIsNegative() throws Exception {
		backPressureSampleService.sampleTaskBackPressure(new TestTask(), -1, Time.milliseconds(0));

		fail("Exception expected.");
	}

	@Test(timeout = 10000L)
	public void testTaskStopsWithPartialSampling() throws Exception {
		final double backPressureRatio = backPressureSampleService.sampleTaskBackPressure(
			new NotRunningAfterFirstSamplingTask(),
			10,
			Time.milliseconds(0)).get();

		assertEquals(1.0, backPressureRatio, 0.0);
	}

	@Test(expected = IllegalStateException.class)
	public void testShouldThrowExceptionIfTaskIsNotRunningBeforeSampling() {
		backPressureSampleService.sampleTaskBackPressure(new NeverRunningTask(), 10, Time.milliseconds(0));

		fail("Exception expected.");
	}

	/**
	 * Task that is always running.
	 */
	private static class TestTask implements OutputAvailabilitySampleableTask {

		private final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

		protected volatile long counter = 0;

		@Override
		public boolean isRunning() {
			return true;
		}

		@Override
		public boolean isAvailableForOutput() {
			return ++counter % 2 == 0;
		}

		@Override
		public ExecutionAttemptID getExecutionId() {
			return executionAttemptID;
		}
	}

	/**
	 * Task that stops running after sampled for the first time.
	 */
	private static class NotRunningAfterFirstSamplingTask extends TestTask {

		@Override
		public boolean isRunning() {
			return counter == 0;
		}
	}

	/**
	 * Task that never runs.
	 */
	private static class NeverRunningTask extends TestTask {

		@Override
		public boolean isRunning() {
			return false;
		}

	}
}
