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
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BackPressureSampleService}.
 */
public class BackPressureSampleServiceTest extends TestLogger {

	private static ScheduledExecutorService scheduledExecutorService;

	private static BackPressureSampleService backPressureSampleService;

	@BeforeClass
	public static void setUp() throws Exception {
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(scheduledExecutorService);

		backPressureSampleService = new BackPressureSampleService(10, Time.milliseconds(10), scheduledExecutor);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (scheduledExecutorService != null) {
			scheduledExecutorService.shutdown();
		}
	}

	@Test(timeout = 10000L)
	public void testSampleTaskBackPressure() throws Exception {
		final double backPressureRatio = backPressureSampleService.
			sampleTaskBackPressure(new TestTask()).get();

		assertEquals(0.5, backPressureRatio, 0.0);
	}

	@Test(timeout = 10000L)
	public void testTaskStopsWithPartialSampling() throws Exception {
		final double backPressureRatio = backPressureSampleService.
			sampleTaskBackPressure(new NotRunningAfterFirstSamplingTask()).get();

		assertEquals(1.0, backPressureRatio, 0.0);
	}

	@Test(expected = IllegalStateException.class)
	public void testShouldThrowExceptionIfTaskIsNotRunningBeforeSampling() {
		backPressureSampleService.sampleTaskBackPressure(new NeverRunningTask());

		fail("Exception expected.");
	}

	/**
	 * Task that is always running.
	 */
	private static class TestTask implements BackPressureSampleableTask {

		protected volatile long counter = 0;

		@Override
		public boolean isRunning() {
			return true;
		}

		@Override
		public boolean isBackPressured() {
			return counter++ % 2 == 0;
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
