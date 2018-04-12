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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobExecutionResultHandler}.
 */
@Category(New.class)
public class JobExecutionResultHandlerTest extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();

	private JobExecutionResultHandler jobExecutionResultHandler;

	private HandlerRequest<EmptyRequestBody, JobMessageParameters> testRequest;

	@Before
	public void setUp() throws Exception {
		final TestingRestfulGateway testingRestfulGateway = TestingRestfulGateway.newBuilder().build();

		jobExecutionResultHandler = new JobExecutionResultHandler(
			CompletableFuture.completedFuture("localhost:12345"),
			() -> CompletableFuture.completedFuture(testingRestfulGateway),
			Time.seconds(10),
			Collections.emptyMap());

		testRequest = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new JobMessageParameters(),
			Collections.singletonMap("jobid", TEST_JOB_ID.toString()),
			Collections.emptyMap());
	}

	@Test
	public void testResultInProgress() throws Exception {
		final TestingRestfulGateway testingRestfulGateway = TestingRestfulGateway.newBuilder()
			.setRequestJobStatusFunction(
				jobId -> CompletableFuture.completedFuture(JobStatus.RUNNING))
			.build();

		final JobExecutionResultResponseBody responseBody = jobExecutionResultHandler.handleRequest(
			testRequest,
			testingRestfulGateway).get();

		assertThat(
			responseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.IN_PROGRESS));
	}

	@Test
	public void testCompletedResult() throws Exception {
		final JobStatus jobStatus = JobStatus.FINISHED;
		final ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(TEST_JOB_ID)
			.setState(jobStatus)
			.build();

		final TestingRestfulGateway testingRestfulGateway = TestingRestfulGateway.newBuilder()
			.setRequestJobStatusFunction(
				jobId -> {
					assertThat(jobId, equalTo(TEST_JOB_ID));
					return CompletableFuture.completedFuture(jobStatus);
				})
			.setRequestJobResultFunction(
				jobId -> {
					assertThat(jobId, equalTo(TEST_JOB_ID));
					return CompletableFuture.completedFuture(JobResult.createFrom(executionGraph));
				}
			)
			.build();

		final JobExecutionResultResponseBody responseBody = jobExecutionResultHandler.handleRequest(
			testRequest,
			testingRestfulGateway).get();

		assertThat(
			responseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.COMPLETED));
		assertThat(responseBody.getJobExecutionResult(), not(nullValue()));
	}

	@Test
	public void testPropagateFlinkJobNotFoundExceptionAsRestHandlerException() throws Exception {
		final TestingRestfulGateway testingRestfulGateway = TestingRestfulGateway.newBuilder()
			.setRequestJobStatusFunction(
				jobId -> FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId))
			)
			.build();

		try {
			jobExecutionResultHandler.handleRequest(
				testRequest,
				testingRestfulGateway).get();
			fail("Expected exception not thrown");
		} catch (final ExecutionException e) {
			final Throwable cause = ExceptionUtils.stripCompletionException(e.getCause());
			assertThat(cause, instanceOf(RestHandlerException.class));
			assertThat(
				((RestHandlerException) cause).getHttpResponseStatus(),
				equalTo(HttpResponseStatus.NOT_FOUND));
		}
	}
}
