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

package org.apache.flink.client.program.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.BlobServerPortHeaders;
import org.apache.flink.runtime.rest.messages.BlobServerPortResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTargetDirectoryParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RestClusterClient}.
 *
 * <p>These tests verify that the client uses the appropriate headers for each
 * request, properly constructs the request bodies/parameters and processes the responses correctly.
 */
@Category(Flip6.class)
public class RestClusterClientTest extends TestLogger {

	private static final String REST_ADDRESS = "http://localhost:1234";

	@Mock
	private Dispatcher mockRestfulGateway;

	@Mock
	private GatewayRetriever<DispatcherGateway> mockGatewayRetriever;

	private RestServerEndpointConfiguration restServerEndpointConfiguration;

	private RestClusterClient restClusterClient;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(REST_ADDRESS));

		final Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		restServerEndpointConfiguration = RestServerEndpointConfiguration.fromConfiguration(config);
		mockGatewayRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);
		restClusterClient = new RestClusterClient(config, (attempt) -> 0);
	}

	@After
	public void tearDown() throws Exception {
		if (restClusterClient != null) {
			restClusterClient.shutdown();
		}
	}

	@Test
	public void testJobSubmitCancelStop() throws Exception {
		final JobGraph job = new JobGraph("testjob");
		final JobID id = job.getJobID();

		TestBlobServerPortHandler portHandler = new TestBlobServerPortHandler();
		TestJobSubmitHandler submitHandler = new TestJobSubmitHandler();
		TestJobTerminationHandler terminationHandler = new TestJobTerminationHandler();
		TestJobExecutionResultHandler testJobExecutionResultHandler =
			new TestJobExecutionResultHandler(Collections.singletonList(
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.jobId(id)
					.netRuntime(Long.MAX_VALUE)
					.build())).iterator());

		try (TestRestServerEndpoint ignored = createRestServerEndpoint(
			portHandler,
			submitHandler,
			terminationHandler,
			testJobExecutionResultHandler)) {

			Assert.assertFalse(portHandler.portRetrieved);
			Assert.assertFalse(submitHandler.jobSubmitted);
			restClusterClient.submitJob(job, ClassLoader.getSystemClassLoader());
			Assert.assertTrue(portHandler.portRetrieved);
			Assert.assertTrue(submitHandler.jobSubmitted);

			Assert.assertFalse(terminationHandler.jobCanceled);
			restClusterClient.cancel(id);
			Assert.assertTrue(terminationHandler.jobCanceled);

			Assert.assertFalse(terminationHandler.jobStopped);
			restClusterClient.stop(id);
			Assert.assertTrue(terminationHandler.jobStopped);
		}
	}

	private class TestBlobServerPortHandler extends TestHandler<EmptyRequestBody, BlobServerPortResponseBody, EmptyMessageParameters> {
		private volatile boolean portRetrieved = false;

		private TestBlobServerPortHandler() {
			super(BlobServerPortHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<BlobServerPortResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			portRetrieved = true;
			return CompletableFuture.completedFuture(new BlobServerPortResponseBody(12000));
		}
	}

	private class TestJobSubmitHandler extends TestHandler<JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {
		private volatile boolean jobSubmitted = false;

		private TestJobSubmitHandler() {
			super(JobSubmitHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			jobSubmitted = true;
			return CompletableFuture.completedFuture(new JobSubmitResponseBody("/url"));
		}
	}

	private class TestJobTerminationHandler extends TestHandler<EmptyRequestBody, EmptyResponseBody, JobTerminationMessageParameters> {
		private volatile boolean jobCanceled = false;
		private volatile boolean jobStopped = false;

		private TestJobTerminationHandler() {
			super(JobTerminationHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobTerminationMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			switch (request.getQueryParameter(TerminationModeQueryParameter.class).get(0)) {
				case CANCEL:
					jobCanceled = true;
					break;
				case STOP:
					jobStopped = true;
					break;
			}
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}
	}

	private class TestJobExecutionResultHandler
		extends TestHandler<EmptyRequestBody, JobExecutionResultResponseBody, JobMessageParameters> {

		private final Iterator<JobExecutionResultResponseBody> jobExecutionResults;

		private JobExecutionResultResponseBody lastJobExecutionResult;

		private TestJobExecutionResultHandler(
				final Iterator<JobExecutionResultResponseBody> jobExecutionResults) {
			super(JobExecutionResultHeaders.getInstance());
			this.jobExecutionResults = jobExecutionResults;
		}

		@Override
		protected CompletableFuture<JobExecutionResultResponseBody> handleRequest(
				@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
				@Nonnull DispatcherGateway gateway) throws RestHandlerException {
			if (jobExecutionResults.hasNext()) {
				lastJobExecutionResult = jobExecutionResults.next();
			}
			checkState(lastJobExecutionResult != null);
			return CompletableFuture.completedFuture(lastJobExecutionResult);
		}
	}

	@Test
	public void testSubmitJobAndWaitForExecutionResult() throws Exception {
		final JobGraph jobGraph = new JobGraph("testjob");
		final JobID jobId = jobGraph.getJobID();

		final TestJobExecutionResultHandler testJobExecutionResultHandler =
			new TestJobExecutionResultHandler(Arrays.asList(
				JobExecutionResultResponseBody.inProgress(),
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.jobId(jobId)
					.netRuntime(Long.MAX_VALUE)
					.accumulatorResults(Collections.singletonMap("testName", new SerializedValue<>(1.0)))
					.build()),
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.jobId(jobId)
					.netRuntime(Long.MAX_VALUE)
					.serializedThrowable(new SerializedThrowable(new RuntimeException("expected")))
					.build())).iterator());

		try (TestRestServerEndpoint ignored = createRestServerEndpoint(
			testJobExecutionResultHandler,
			new TestBlobServerPortHandler(),
			new TestJobSubmitHandler())) {

			final org.apache.flink.api.common.JobExecutionResult jobExecutionResult =
				(org.apache.flink.api.common.JobExecutionResult) restClusterClient.submitJob(
					jobGraph,
					ClassLoader.getSystemClassLoader());
			assertThat(jobExecutionResult.getJobID(), equalTo(jobId));
			assertThat(jobExecutionResult.getNetRuntime(), equalTo(Long.MAX_VALUE));
			assertThat(
				jobExecutionResult.getAllAccumulatorResults(),
				equalTo(Collections.singletonMap("testName", 1.0)));

			try {
				restClusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());
				fail("Expected exception not thrown.");
			} catch (final ProgramInvocationException e) {
				assertThat(e.getCause(), instanceOf(RuntimeException.class));
				assertThat(e.getCause().getMessage(), equalTo("expected"));
			}
		}
	}

	@Test
	public void testTriggerSavepoint() throws Exception {
		String targetSavepointDirectory = "/alternate";
		TestSavepointTriggerHandler triggerHandler = new TestSavepointTriggerHandler(targetSavepointDirectory);
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(triggerHandler)) {
			JobID id = new JobID();
			{
				CompletableFuture<String> savepointPathFuture = restClusterClient.triggerSavepoint(id, null);
				String savepointPath = savepointPathFuture.get();
				Assert.assertEquals("/universe", savepointPath);
			}

			{
				CompletableFuture<String> savepointPathFuture = restClusterClient.triggerSavepoint(id, targetSavepointDirectory);
				String savepointPath = savepointPathFuture.get();
				Assert.assertEquals(targetSavepointDirectory + "/universe", savepointPath);
			}
		}
	}

	private class TestSavepointTriggerHandler extends TestHandler<EmptyRequestBody, SavepointTriggerResponseBody, SavepointMessageParameters> {

		private final String expectedSavepointDirectory;

		TestSavepointTriggerHandler(String expectedSavepointDirectory) {
			super(SavepointTriggerHeaders.getInstance());
			this.expectedSavepointDirectory = expectedSavepointDirectory;
		}

		@Override
		protected CompletableFuture<SavepointTriggerResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, SavepointMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			List<String> targetDirectories = request.getQueryParameter(SavepointTargetDirectoryParameter.class);
			if (targetDirectories.isEmpty()) {
				return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", "/universe", "big-bang"));
			} else {
				String targetDir = targetDirectories.get(0);
				if (targetDir.equals(expectedSavepointDirectory)) {
					return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", targetDir + "/universe", "big-bang"));
				} else {
					return CompletableFuture.completedFuture(new SavepointTriggerResponseBody("growing", "savepoint directory (" + targetDir + ") did not match expected (" + expectedSavepointDirectory + ')', "big-bang"));
				}
			}
		}
	}

	@Test
	public void testListJobs() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestListJobsHandler())) {
			{
				CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = restClusterClient.listJobs();
				Collection<JobStatusMessage> jobDetails = jobDetailsFuture.get();
				Iterator<JobStatusMessage> jobDetailsIterator = jobDetails.iterator();
				JobStatusMessage job1 = jobDetailsIterator.next();
				JobStatusMessage job2 = jobDetailsIterator.next();
				Assert.assertNotEquals("The job statues should not be equal.", job1.getJobState(), job2.getJobState());
			}
		}
	}

	private class TestListJobsHandler extends TestHandler<EmptyRequestBody, MultipleJobsDetails, EmptyMessageParameters> {

		private TestListJobsHandler() {
			super(JobsOverviewHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<MultipleJobsDetails> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			JobDetails running = new JobDetails(new JobID(), "job1", 0, 0, 0, JobStatus.RUNNING, 0, new int[9], 0);
			JobDetails finished = new JobDetails(new JobID(), "job2", 0, 0, 0, JobStatus.FINISHED, 0, new int[9], 0);
			return CompletableFuture.completedFuture(new MultipleJobsDetails(Arrays.asList(running, finished)));
		}
	}

	private abstract class TestHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends AbstractRestHandler<DispatcherGateway, R, P, M> {

		private TestHandler(MessageHeaders<R, P, M> headers) {
			super(
				CompletableFuture.completedFuture(REST_ADDRESS),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				Collections.emptyMap(),
				headers);
		}
	}

	private TestRestServerEndpoint createRestServerEndpoint(
			final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws Exception {
		final TestRestServerEndpoint testRestServerEndpoint = new TestRestServerEndpoint(abstractRestHandlers);
		testRestServerEndpoint.start();
		return testRestServerEndpoint;
	}

	private class TestRestServerEndpoint extends RestServerEndpoint implements AutoCloseable {

		private final AbstractRestHandler<?, ?, ?, ?>[] abstractRestHandlers;

		TestRestServerEndpoint(final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) {
			super(restServerEndpointConfiguration);
			this.abstractRestHandlers = abstractRestHandlers;
		}

		@Override
		protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>>
				initializeHandlers(CompletableFuture<String> restAddressFuture) {
			final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
			for (final AbstractRestHandler abstractRestHandler : abstractRestHandlers) {
				handlers.add(Tuple2.of(
					abstractRestHandler.getMessageHeaders(),
					abstractRestHandler));
			}
			return handlers;
		}

		@Override
		public void close() throws Exception {
			shutdown(Time.seconds(5));
		}
	}
}
