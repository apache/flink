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

package org.apache.flink.runtime.executiongraph.metrics;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.TestingJobStatusProvider;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RestartTimeGauge}.
 */
public class RestartTimeGaugeTest extends TestLogger {

	@Test
	public void testNotRestarted() {
		final RestartTimeGauge gauge = new RestartTimeGauge(new TestingJobStatusProvider(JobStatus.RUNNING, -1));
		assertThat(gauge.getValue(), is(0L));
	}

	@Test
	public void testInRestarting() {
		final Map<JobStatus, Long> statusTimestampMap = new HashMap<>();
		statusTimestampMap.put(JobStatus.RESTARTING, 1L);

		final RestartTimeGauge gauge = new RestartTimeGauge(
			new TestingJobStatusProvider(
				JobStatus.RESTARTING,
				status -> statusTimestampMap.getOrDefault(status, -1L)));
		assertThat(gauge.getValue(), greaterThan(0L));
	}

	@Test
	public void testRunningAfterRestarting() {
		final Map<JobStatus, Long> statusTimestampMap = new HashMap<>();
		statusTimestampMap.put(JobStatus.RESTARTING, 123L);
		statusTimestampMap.put(JobStatus.RUNNING, 234L);

		final RestartTimeGauge gauge = new RestartTimeGauge(
			new TestingJobStatusProvider(
				JobStatus.RUNNING,
				status -> statusTimestampMap.getOrDefault(status, -1L)));
		assertThat(gauge.getValue(), is(111L));
	}

	@Test
	public void testFailedAfterRestarting() {
		final Map<JobStatus, Long> statusTimestampMap = new HashMap<>();
		statusTimestampMap.put(JobStatus.RESTARTING, 123L);
		statusTimestampMap.put(JobStatus.FAILED, 456L);

		final RestartTimeGauge gauge = new RestartTimeGauge(
			new TestingJobStatusProvider(
				JobStatus.FAILED,
				status -> statusTimestampMap.getOrDefault(status, -1L)));
		assertThat(gauge.getValue(), is(333L));
	}
}
