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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.AccumulatorsIncludeSerializedValueQueryParameter;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsMessageParameters;
import org.apache.flink.runtime.rest.messages.JobCancellationHeaders;
import org.apache.flink.runtime.rest.messages.JobCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link RestClusterClient}.
 *
 * <p>These tests verify that the client uses the appropriate headers for each
 * request, properly constructs the request bodies/parameters and processes the responses correctly.
 */
public class RestClusterClientTest extends TestLogger {

	private final DispatcherGateway mockRestfulGateway = new TestingDispatcherGateway.Builder().build();

	private GatewayRetriever<DispatcherGateway> mockGatewayRetriever;

	private RestServerEndpointConfiguration restServerEndpointConfiguration;

	private volatile FailHttpRequestPredicate failHttpRequest = FailHttpRequestPredicate.never();

	private ExecutorService executor;

	private JobGraph jobGraph;
	private JobID jobId;

	private static final Configuration restConfig;

	static {
		final Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
		config.setLong(RestOptions.RETRY_DELAY, 0);
		config.setInteger(RestOptions.PORT, 0);

		restConfig = config;
	}

	@Before
	public void setUp() throws Exception {
		restServerEndpointConfiguration = RestServerEndpointConfiguration.fromConfiguration(restConfig);
		mockGatewayRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);

		executor = Executors.newSingleThreadExecutor(new ExecutorThreadFactory(RestClusterClientTest.class.getSimpleName()));

		jobGraph = new JobGraph("testjob");
		jobId = jobGraph.getJobID();
	}

	@After
	public void tearDown() {
		if (executor != null) {
			executor.shutdown();
		}
	}

	private RestClusterClient<StandaloneClusterId> createRestClusterClient(final int port) throws Exception {
		final Configuration clientConfig = new Configuration(restConfig);
		clientConfig.setInteger(RestOptions.PORT, port);
		return new RestClusterClient<>(
			clientConfig,
			createRestClient(),
			StandaloneClusterId.getInstance(),
			(attempt) -> 0,
			null);
	}

	@Nonnull
	private RestClient createRestClient() throws ConfigurationException {
		return new RestClient(RestClientConfiguration.fromConfiguration(restConfig), executor) {
			@Override
			public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P>
			sendRequest(
				final String targetAddress,
				final int targetPort,
				final M messageHeaders,
				final U messageParameters,
				final R request,
				final Collection<FileUpload> files) throws IOException {
				if (failHttpRequest.test(messageHeaders, messageParameters, request)) {
					return FutureUtils.completedExceptionally(new IOException("expected"));
				} else {
					return super.sendRequest(targetAddress, targetPort, messageHeaders, messageParameters, request, files);
				}
			}
		};
	}

	@Test
	public void testJobSubmitCancel() throws Exception {
		TestJobSubmitHandler submitHandler = new TestJobSubmitHandler();
		TestJobCancellationHandler terminationHandler = new TestJobCancellationHandler();
		TestJobExecutionResultHandler testJobExecutionResultHandler =
			new TestJobExecutionResultHandler(
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.applicationStatus(ApplicationStatus.SUCCEEDED)
					.jobId(jobId)
					.netRuntime(Long.MAX_VALUE)
					.build()));

		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(
			submitHandler,
			terminationHandler,
			testJobExecutionResultHandler)) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				Assert.assertFalse(submitHandler.jobSubmitted);
				restClusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());
				Assert.assertTrue(submitHandler.jobSubmitted);

				Assert.assertFalse(terminationHandler.jobCanceled);
				restClusterClient.cancel(jobId);
				Assert.assertTrue(terminationHandler.jobCanceled);
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	/**
	 * Tests that we can submit a jobGraph in detached mode.
	 */
	@Test
	public void testDetachedJobSubmission() throws Exception {

		final TestJobSubmitHandler testJobSubmitHandler = new TestJobSubmitHandler();

		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(
			testJobSubmitHandler)) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				restClusterClient.setDetached(true);
				final JobSubmissionResult jobSubmissionResult = restClusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());

				// if the detached mode didn't work, then we would not reach this point because the execution result
				// retrieval would have failed.
				assertThat(jobSubmissionResult, is(not(instanceOf(JobExecutionResult.class))));
				assertThat(jobSubmissionResult.getJobID(), is(jobId));
			} finally {
				restClusterClient.shutdown();
			}
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

	private class TestJobCancellationHandler extends TestHandler<EmptyRequestBody, EmptyResponseBody, JobCancellationMessageParameters> {
		private volatile boolean jobCanceled = false;

		private TestJobCancellationHandler() {
			super(JobCancellationHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobCancellationMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			jobCanceled = true;
			return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
		}
	}

	private class TestJobExecutionResultHandler
		extends TestHandler<EmptyRequestBody, JobExecutionResultResponseBody, JobMessageParameters> {

		private final Iterator<Object> jobExecutionResults;

		private Object lastJobExecutionResult;

		private TestJobExecutionResultHandler(
				final Object... jobExecutionResults) {
			super(JobExecutionResultHeaders.getInstance());
			checkArgument(Arrays.stream(jobExecutionResults)
				.allMatch(object -> object instanceof JobExecutionResultResponseBody
					|| object instanceof RestHandlerException));
			this.jobExecutionResults = Arrays.asList(jobExecutionResults).iterator();
		}

		@Override
		protected CompletableFuture<JobExecutionResultResponseBody> handleRequest(
				@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
				@Nonnull DispatcherGateway gateway) throws RestHandlerException {
			if (jobExecutionResults.hasNext()) {
				lastJobExecutionResult = jobExecutionResults.next();
			}
			checkState(lastJobExecutionResult != null);
			if (lastJobExecutionResult instanceof JobExecutionResultResponseBody) {
				return CompletableFuture.completedFuture((JobExecutionResultResponseBody) lastJobExecutionResult);
			} else if (lastJobExecutionResult instanceof RestHandlerException) {
				return FutureUtils.completedExceptionally((RestHandlerException) lastJobExecutionResult);
			} else {
				throw new AssertionError();
			}
		}
	}

	@Test
	public void testSubmitJobAndWaitForExecutionResult() throws Exception {
		final TestJobExecutionResultHandler testJobExecutionResultHandler =
			new TestJobExecutionResultHandler(
				new RestHandlerException("should trigger retry", HttpResponseStatus.SERVICE_UNAVAILABLE),
				JobExecutionResultResponseBody.inProgress(),
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.applicationStatus(ApplicationStatus.SUCCEEDED)
					.jobId(jobId)
					.netRuntime(Long.MAX_VALUE)
					.accumulatorResults(Collections.singletonMap("testName", new SerializedValue<>(OptionalFailure.of(1.0))))
					.build()),
				JobExecutionResultResponseBody.created(new JobResult.Builder()
					.applicationStatus(ApplicationStatus.FAILED)
					.jobId(jobId)
					.netRuntime(Long.MAX_VALUE)
					.serializedThrowable(new SerializedThrowable(new RuntimeException("expected")))
					.build()));

		// fail first HTTP polling attempt, which should not be a problem because of the retries
		final AtomicBoolean firstPollFailed = new AtomicBoolean();
		failHttpRequest = (messageHeaders, messageParameters, requestBody) ->
			messageHeaders instanceof JobExecutionResultHeaders && !firstPollFailed.getAndSet(true);

		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(
			testJobExecutionResultHandler,
			new TestJobSubmitHandler())) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				JobExecutionResult jobExecutionResult;

				jobExecutionResult = (JobExecutionResult) restClusterClient.submitJob(
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
					final Optional<RuntimeException> cause = ExceptionUtils.findThrowable(e, RuntimeException.class);

					assertThat(cause.isPresent(), is(true));
					assertThat(cause.get().getMessage(), equalTo("expected"));
				}
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	@Test
	public void testDisposeSavepoint() throws Exception {
		final String savepointPath = "foobar";
		final String exceptionMessage = "Test exception.";
		final FlinkException testException = new FlinkException(exceptionMessage);

		final TestSavepointDisposalHandlers testSavepointDisposalHandlers = new TestSavepointDisposalHandlers(savepointPath);
		final TestSavepointDisposalHandlers.TestSavepointDisposalTriggerHandler testSavepointDisposalTriggerHandler = testSavepointDisposalHandlers.new TestSavepointDisposalTriggerHandler();
		final TestSavepointDisposalHandlers.TestSavepointDisposalStatusHandler testSavepointDisposalStatusHandler = testSavepointDisposalHandlers.new TestSavepointDisposalStatusHandler(
			OptionalFailure.of(AsynchronousOperationInfo.complete()),
			OptionalFailure.of(AsynchronousOperationInfo.completeExceptional(new SerializedThrowable(testException))),
			OptionalFailure.ofFailure(testException));

		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(
			testSavepointDisposalStatusHandler,
			testSavepointDisposalTriggerHandler)) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				{
					final CompletableFuture<Acknowledge> disposeSavepointFuture = restClusterClient.disposeSavepoint(savepointPath);
					assertThat(disposeSavepointFuture.get(), is(Acknowledge.get()));
				}

				{
					final CompletableFuture<Acknowledge> disposeSavepointFuture = restClusterClient.disposeSavepoint(savepointPath);

					try {
						disposeSavepointFuture.get();
						fail("Expected an exception");
					} catch (ExecutionException ee) {
						assertThat(ExceptionUtils.findThrowableWithMessage(ee, exceptionMessage).isPresent(), is(true));
					}
				}

				{
					try {
						restClusterClient.disposeSavepoint(savepointPath).get();
						fail("Expected an exception.");
					} catch (ExecutionException ee) {
						assertThat(ExceptionUtils.findThrowable(ee, RestClientException.class).isPresent(), is(true));
					}
				}
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	private class TestSavepointDisposalHandlers {

		private final TriggerId triggerId = new TriggerId();

		private final String savepointPath;

		private TestSavepointDisposalHandlers(String savepointPath) {
			this.savepointPath = Preconditions.checkNotNull(savepointPath);
		}

		private class TestSavepointDisposalTriggerHandler extends TestHandler<SavepointDisposalRequest, TriggerResponse, EmptyMessageParameters> {
			private TestSavepointDisposalTriggerHandler() {
				super(SavepointDisposalTriggerHeaders.getInstance());
			}

			@Override
			protected CompletableFuture<TriggerResponse> handleRequest(@Nonnull HandlerRequest<SavepointDisposalRequest, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) {
				assertThat(request.getRequestBody().getSavepointPath(), is(savepointPath));
				return CompletableFuture.completedFuture(new TriggerResponse(triggerId));
			}
		}

		private class TestSavepointDisposalStatusHandler extends TestHandler<EmptyRequestBody, AsynchronousOperationResult<AsynchronousOperationInfo>, SavepointDisposalStatusMessageParameters> {

			private final Queue<OptionalFailure<AsynchronousOperationInfo>> responses;

			private TestSavepointDisposalStatusHandler(OptionalFailure<AsynchronousOperationInfo>... responses) {
				super(SavepointDisposalStatusHeaders.getInstance());
				this.responses = new ArrayDeque<>(Arrays.asList(responses));
			}

			@Override
			protected CompletableFuture<AsynchronousOperationResult<AsynchronousOperationInfo>> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, SavepointDisposalStatusMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
				final TriggerId actualTriggerId = request.getPathParameter(TriggerIdPathParameter.class);

				if (actualTriggerId.equals(triggerId)) {
					final OptionalFailure<AsynchronousOperationInfo> nextResponse = responses.poll();

					if (nextResponse != null) {
						if (nextResponse.isFailure()) {
							throw new RestHandlerException("Failure", HttpResponseStatus.BAD_REQUEST, nextResponse.getFailureCause());
						} else {
							return CompletableFuture.completedFuture(AsynchronousOperationResult.completed(nextResponse.getUnchecked()));
						}
					} else {
						throw new AssertionError();
					}
				} else {
					throw new AssertionError();
				}
			}
		}
	}

	@Test
	public void testListJobs() throws Exception {
		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(new TestListJobsHandler())) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = restClusterClient.listJobs();
				Collection<JobStatusMessage> jobDetails = jobDetailsFuture.get();
				Iterator<JobStatusMessage> jobDetailsIterator = jobDetails.iterator();
				JobStatusMessage job1 = jobDetailsIterator.next();
				JobStatusMessage job2 = jobDetailsIterator.next();
				Assert.assertNotEquals("The job status should not be equal.", job1.getJobState(), job2.getJobState());
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	@Test
	public void testGetAccumulators() throws Exception {
		TestAccumulatorHandler accumulatorHandler = new TestAccumulatorHandler();

		try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(accumulatorHandler)){
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				JobID id = new JobID();

				{
					Map<String, OptionalFailure<Object>> accumulators = restClusterClient.getAccumulators(id);
					assertNotNull(accumulators);
					assertEquals(1, accumulators.size());

					assertEquals(true, accumulators.containsKey("testKey"));
					assertEquals("testValue", accumulators.get("testKey").get().toString());
				}
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	/**
	 * Tests that command line options override the configuration settings.
	 */
	@Test
	public void testRESTManualConfigurationOverride() throws Exception {
		final String configuredHostname = "localhost";
		final int configuredPort = 1234;
		final Configuration configuration = new Configuration();

		configuration.setString(JobManagerOptions.ADDRESS, configuredHostname);
		configuration.setInteger(JobManagerOptions.PORT, configuredPort);
		configuration.setString(RestOptions.ADDRESS, configuredHostname);
		configuration.setInteger(RestOptions.PORT, configuredPort);

		final DefaultCLI defaultCLI = new DefaultCLI(configuration);

		final String manualHostname = "123.123.123.123";
		final int manualPort = 4321;
		final String[] args = {"-m", manualHostname + ':' + manualPort};

		CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);

		final StandaloneClusterDescriptor clusterDescriptor = defaultCLI.createClusterDescriptor(commandLine);

		final RestClusterClient<?> clusterClient = clusterDescriptor.retrieve(defaultCLI.getClusterId(commandLine));

		URL webMonitorBaseUrl = clusterClient.getWebMonitorBaseUrl().get();
		assertThat(webMonitorBaseUrl.getHost(), equalTo(manualHostname));
		assertThat(webMonitorBaseUrl.getPort(), equalTo(manualPort));
	}

	/**
	 * Tests that the send operation is being retried.
	 */
	@Test
	public void testRetriableSendOperationIfConnectionErrorOrServiceUnavailable() throws Exception {
		final PingRestHandler pingRestHandler = new PingRestHandler(
			FutureUtils.completedExceptionally(new RestHandlerException("test exception", HttpResponseStatus.SERVICE_UNAVAILABLE)),
			CompletableFuture.completedFuture(EmptyResponseBody.getInstance()));

		try (final TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(pingRestHandler)) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				final AtomicBoolean firstPollFailed = new AtomicBoolean();
				failHttpRequest = (messageHeaders, messageParameters, requestBody) ->
					messageHeaders instanceof PingRestHandlerHeaders && !firstPollFailed.getAndSet(true);

				restClusterClient.sendRequest(PingRestHandlerHeaders.INSTANCE).get();
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	@Test
	public void testJobSubmissionFailureThrowsProgramInvocationException() throws Exception {
		try (final TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(new SubmissionFailingHandler())) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				restClusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());
			} catch (final ProgramInvocationException expected) {
				// expected
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	private final class SubmissionFailingHandler extends TestHandler<JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

		private SubmissionFailingHandler() {
			super(JobSubmitHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobSubmitResponseBody> handleRequest(
				@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request,
				@Nonnull DispatcherGateway gateway) throws RestHandlerException {
			throw new RestHandlerException("expected", HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Tests that the send operation is not being retried when receiving a NOT_FOUND return code.
	 */
	@Test
	public void testSendIsNotRetriableIfHttpNotFound() throws Exception {
		final String exceptionMessage = "test exception";
		final PingRestHandler pingRestHandler = new PingRestHandler(
			FutureUtils.completedExceptionally(new RestHandlerException(exceptionMessage, HttpResponseStatus.NOT_FOUND)));

		try (final TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(pingRestHandler)) {
			RestClusterClient<?> restClusterClient = createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

			try {
				restClusterClient.sendRequest(PingRestHandlerHeaders.INSTANCE).get();
				fail("The rest request should have failed.");
			}  catch (Exception e) {
				assertThat(ExceptionUtils.findThrowableWithMessage(e, exceptionMessage).isPresent(), is(true));
			} finally {
				restClusterClient.shutdown();
			}
		}
	}

	private class PingRestHandler extends TestHandler<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

		private final Queue<CompletableFuture<EmptyResponseBody>> responseQueue;

		private PingRestHandler(CompletableFuture<EmptyResponseBody> ... responses) {
			super(PingRestHandlerHeaders.INSTANCE);
			responseQueue = new ArrayDeque<>(Arrays.asList(responses));
		}

		@Override
		protected CompletableFuture<EmptyResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			final CompletableFuture<EmptyResponseBody> result = responseQueue.poll();

			if (result != null) {
				return result;
			} else {
				return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
			}
		}
	}

	private static final class PingRestHandlerHeaders implements MessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

		static final PingRestHandlerHeaders INSTANCE = new PingRestHandlerHeaders();

		@Override
		public Class<EmptyResponseBody> getResponseClass() {
			return EmptyResponseBody.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public String getDescription() {
			return "foobar";
		}

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/foobar";
		}
	}

	private class TestAccumulatorHandler extends TestHandler<EmptyRequestBody, JobAccumulatorsInfo, JobAccumulatorsMessageParameters> {

		public TestAccumulatorHandler() {
			super(JobAccumulatorsHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobAccumulatorsInfo> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody,
				JobAccumulatorsMessageParameters> request,
			@Nonnull DispatcherGateway gateway) throws RestHandlerException {
			JobAccumulatorsInfo accumulatorsInfo;
			List<Boolean> queryParams = request.getQueryParameter(AccumulatorsIncludeSerializedValueQueryParameter.class);

			final boolean includeSerializedValue;
			if (!queryParams.isEmpty()) {
				includeSerializedValue = queryParams.get(0);
			} else {
				includeSerializedValue = false;
			}

			List<JobAccumulatorsInfo.UserTaskAccumulator> userTaskAccumulators = new ArrayList<>(1);

			userTaskAccumulators.add(new JobAccumulatorsInfo.UserTaskAccumulator("testName", "testType", "testValue"));

			if (includeSerializedValue) {
				Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserTaskAccumulators = new HashMap<>(1);
				try {
					serializedUserTaskAccumulators.put("testKey", new SerializedValue<>(OptionalFailure.of("testValue")));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				accumulatorsInfo = new JobAccumulatorsInfo(Collections.emptyList(), userTaskAccumulators, serializedUserTaskAccumulators);
			} else {
				accumulatorsInfo = new JobAccumulatorsInfo(Collections.emptyList(), userTaskAccumulators, Collections.emptyMap());
			}

			return CompletableFuture.completedFuture(accumulatorsInfo);
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
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				Collections.emptyMap(),
				headers);
		}
	}

	private TestRestServerEndpoint createRestServerEndpoint(
			final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws Exception {
		return TestRestServerEndpoint.createAndStartRestServerEndpoint(restServerEndpointConfiguration, abstractRestHandlers);
	}

	@FunctionalInterface
	private interface FailHttpRequestPredicate {

		boolean test(MessageHeaders<?, ?, ?> messageHeaders, MessageParameters messageParameters, RequestBody requestBody);

		static FailHttpRequestPredicate never() {
			return ((messageHeaders, messageParameters, requestBody) -> false);
		}
	}

}
