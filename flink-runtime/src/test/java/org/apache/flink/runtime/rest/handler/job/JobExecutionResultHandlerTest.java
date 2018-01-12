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
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.JobExecutionResultGoneException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link JobExecutionResultHandler}.
 */
public class JobExecutionResultHandlerTest extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();

	private JobExecutionResultHandler jobExecutionResultHandler;

	@Mock
	private RestfulGateway mockRestfulGateway;

	private HandlerRequest<EmptyRequestBody, JobMessageParameters> testRequest;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		jobExecutionResultHandler = new JobExecutionResultHandler(
			CompletableFuture.completedFuture("localhost:12345"),
			new GatewayRetriever<RestfulGateway>() {
				@Override
				public CompletableFuture<RestfulGateway> getFuture() {
					return CompletableFuture.completedFuture(mockRestfulGateway);
				}
			},
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
		when(mockRestfulGateway.isJobExecutionResultPresent(any(JobID.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(false));

		final JobExecutionResultResponseBody responseBody = jobExecutionResultHandler.handleRequest(
			testRequest,
			mockRestfulGateway).get();

		assertThat(
			responseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.IN_PROGRESS));
	}

	@Test
	public void testCompletedResult() throws Exception {
		when(mockRestfulGateway.isJobExecutionResultPresent(any(JobID.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(true));

		when(mockRestfulGateway.getJobExecutionResult(any(JobID.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new JobResult.Builder()
				.jobId(TEST_JOB_ID)
				.netRuntime(Long.MAX_VALUE)
				.build()));

		final JobExecutionResultResponseBody responseBody = jobExecutionResultHandler.handleRequest(
			testRequest,
			mockRestfulGateway).get();

		assertThat(
			responseBody.getStatus().getId(),
			equalTo(QueueStatus.Id.COMPLETED));
		assertThat(responseBody.getJobExecutionResult(), not(nullValue()));
	}

	@Test
	public void testPropagateFlinkJobNotFoundExceptionAsRestHandlerException() throws Exception {
		assertPropagateAsRestHandlerException(
			new CompletionException(new FlinkJobNotFoundException(new JobID())));
	}

	@Test
	public void testPropagateJobExecutionResultGoneExceptionAsRestHandlerException() throws Exception {
		assertPropagateAsRestHandlerException(
			new CompletionException(new JobExecutionResultGoneException(new JobID())));
	}

	private void assertPropagateAsRestHandlerException(final Exception exception) throws Exception {
		when(mockRestfulGateway.isJobExecutionResultPresent(any(JobID.class), any(Time.class)))
			.thenReturn(FutureUtils.completedExceptionally(
				exception));

		try {
			jobExecutionResultHandler.handleRequest(
				testRequest,
				mockRestfulGateway).get();
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
