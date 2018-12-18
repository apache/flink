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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobResult}.
 */
public class JobResultTest extends TestLogger {

	@Test
	public void testNetRuntimeMandatory() {
		try {
			new JobResult.Builder()
				.jobId(new JobID())
				.build();
			fail("Expected exception not thrown");
		} catch (final IllegalArgumentException e) {
			assertThat(e.getMessage(), equalTo("netRuntime must be greater than or equals 0"));
		}
	}

	@Test
	public void testIsNotSuccess() throws Exception {
		final JobResult jobResult = new JobResult.Builder()
			.jobId(new JobID())
			.serializedThrowable(new SerializedThrowable(new RuntimeException()))
			.netRuntime(Long.MAX_VALUE)
			.build();

		assertThat(jobResult.isSuccess(), equalTo(false));
	}

	@Test
	public void testIsSuccess() throws Exception {
		final JobResult jobResult = new JobResult.Builder()
			.jobId(new JobID())
			.netRuntime(Long.MAX_VALUE)
			.build();

		assertThat(jobResult.isSuccess(), equalTo(true));
	}

	@Test
	public void testCancelledJobIsFailureResult() {
		final JobResult jobResult = JobResult.createFrom(
			new ArchivedExecutionGraphBuilder()
				.setJobID(new JobID())
				.setState(JobStatus.CANCELED)
				.build());

		assertThat(jobResult.isSuccess(), is(false));
	}

	@Test
	public void testFailedJobIsFailureResult() {
		final JobResult jobResult = JobResult.createFrom(
			new ArchivedExecutionGraphBuilder()
				.setJobID(new JobID())
				.setState(JobStatus.FAILED)
				.setFailureCause(new ErrorInfo(new FlinkException("Test exception"), 42L))
				.build());

		assertThat(jobResult.isSuccess(), is(false));
	}

	@Test
	public void testCancelledJobThrowsJobCancellationException() throws Exception {
		final FlinkException cause = new FlinkException("Test exception");
		final JobResult jobResult = JobResult.createFrom(
			new ArchivedExecutionGraphBuilder()
				.setJobID(new JobID())
				.setState(JobStatus.CANCELED)
				.setFailureCause(new ErrorInfo(cause, 42L))
				.build());

		try {
			jobResult.toJobExecutionResult(getClass().getClassLoader());
			fail("Job should fail with an JobCancellationException.");
		} catch (JobCancellationException expected) {
			assertThat(expected.getCause(), is(equalTo(cause)));
		}
	}

	@Test
	public void testFailedJobThrowsJobExecutionException() throws Exception {
		final FlinkException cause = new FlinkException("Test exception");
		final JobResult jobResult = JobResult.createFrom(
			new ArchivedExecutionGraphBuilder()
				.setJobID(new JobID())
				.setState(JobStatus.FAILED)
				.setFailureCause(new ErrorInfo(cause, 42L))
				.build());

		try {
			jobResult.toJobExecutionResult(getClass().getClassLoader());
			fail("Job should fail with JobExecutionException.");
		} catch (JobExecutionException expected) {
			assertThat(expected.getCause(), is(equalTo(cause)));
		}
	}
}
