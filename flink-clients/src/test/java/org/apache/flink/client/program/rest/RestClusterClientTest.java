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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobStatusInfo;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
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
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetIdPathParameter;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetListHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetListResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobStatusInfoHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationHeaders;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationMessageParameters;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationRequestBody;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for the {@link RestClusterClient}.
 *
 * <p>These tests verify that the client uses the appropriate headers for each request, properly
 * constructs the request bodies/parameters and processes the responses correctly.
 */
class RestClusterClientTest {

    private final DispatcherGateway mockRestfulGateway =
            TestingDispatcherGateway.newBuilder().build();

    private GatewayRetriever<DispatcherGateway> mockGatewayRetriever;

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

    @BeforeEach
    void setUp() {
        mockGatewayRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);

        executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(RestClusterClientTest.class.getSimpleName()));

        jobGraph = JobGraphTestUtils.emptyJobGraph();
        jobId = jobGraph.getJobID();
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private RestClusterClient<StandaloneClusterId> createRestClusterClient(int port)
            throws Exception {
        return createRestClusterClient(port, new Configuration(restConfig));
    }

    private RestClusterClient<StandaloneClusterId> createRestClusterClient(
            int port, Configuration clientConfig) throws Exception {
        clientConfig.setInteger(RestOptions.PORT, port);
        return new RestClusterClient<>(
                clientConfig,
                createRestClient(),
                StandaloneClusterId.getInstance(),
                (attempt) -> 0);
    }

    @Nonnull
    private RestClient createRestClient() throws ConfigurationException {
        return new RestClient(restConfig, executor) {
            @Override
            public <
                            M extends MessageHeaders<R, P, U>,
                            U extends MessageParameters,
                            R extends RequestBody,
                            P extends ResponseBody>
                    CompletableFuture<P> sendRequest(
                            final String targetAddress,
                            final int targetPort,
                            final M messageHeaders,
                            final U messageParameters,
                            final R request,
                            final Collection<FileUpload> files)
                            throws IOException {
                if (failHttpRequest.test(messageHeaders, messageParameters, request)) {
                    return FutureUtils.completedExceptionally(new IOException("expected"));
                } else {
                    return super.sendRequest(
                            targetAddress,
                            targetPort,
                            messageHeaders,
                            messageParameters,
                            request,
                            files);
                }
            }
        };
    }

    @Test
    void testJobSubmitCancel() throws Exception {
        TestJobSubmitHandler submitHandler = new TestJobSubmitHandler();
        TestJobCancellationHandler terminationHandler = new TestJobCancellationHandler();

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(submitHandler, terminationHandler)) {

            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                assertThat(submitHandler.jobSubmitted).isFalse();
                restClusterClient.submitJob(jobGraph).get();
                assertThat(submitHandler.jobSubmitted).isTrue();

                assertThat(terminationHandler.jobCanceled).isFalse();
                restClusterClient.cancel(jobId).get();
                assertThat(terminationHandler.jobCanceled).isTrue();
            }
        }
    }

    private class TestJobSubmitHandler
            extends TestHandler<
                    JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {
        private volatile boolean jobSubmitted = false;

        private TestJobSubmitHandler() {
            super(JobSubmitHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<JobSubmitResponseBody> handleRequest(
                @Nonnull HandlerRequest<JobSubmitRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            jobSubmitted = true;
            return CompletableFuture.completedFuture(new JobSubmitResponseBody("/url"));
        }
    }

    private class TestJobCancellationHandler
            extends TestHandler<
                    EmptyRequestBody, EmptyResponseBody, JobCancellationMessageParameters> {
        private volatile boolean jobCanceled = false;

        private TestJobCancellationHandler() {
            super(JobCancellationHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            jobCanceled = true;
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }
    }

    @Test
    void testDisposeSavepoint() throws Exception {
        final String savepointPath = "foobar";
        final String exceptionMessage = "Test exception.";
        final FlinkException testException = new FlinkException(exceptionMessage);

        final TestSavepointDisposalHandlers testSavepointDisposalHandlers =
                new TestSavepointDisposalHandlers(savepointPath);
        final TestSavepointDisposalHandlers.TestSavepointDisposalTriggerHandler
                testSavepointDisposalTriggerHandler =
                        testSavepointDisposalHandlers.new TestSavepointDisposalTriggerHandler();
        final TestSavepointDisposalHandlers.TestSavepointDisposalStatusHandler
                testSavepointDisposalStatusHandler =
                        testSavepointDisposalHandlers
                        .new TestSavepointDisposalStatusHandler(
                                OptionalFailure.of(AsynchronousOperationInfo.complete()),
                                OptionalFailure.of(
                                        AsynchronousOperationInfo.completeExceptional(
                                                new SerializedThrowable(testException))),
                                OptionalFailure.ofFailure(testException));

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        testSavepointDisposalStatusHandler, testSavepointDisposalTriggerHandler)) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                {
                    final CompletableFuture<Acknowledge> disposeSavepointFuture =
                            restClusterClient.disposeSavepoint(savepointPath);
                    assertThat(disposeSavepointFuture.get()).isEqualTo(Acknowledge.get());
                }

                {
                    final CompletableFuture<Acknowledge> disposeSavepointFuture =
                            restClusterClient.disposeSavepoint(savepointPath);

                    try {
                        disposeSavepointFuture.get();
                        fail("Expected an exception");
                    } catch (ExecutionException ee) {
                        assertThat(ExceptionUtils.findThrowableWithMessage(ee, exceptionMessage))
                                .isPresent();
                    }
                }

                {
                    try {
                        restClusterClient.disposeSavepoint(savepointPath).get();
                        fail("Expected an exception.");
                    } catch (ExecutionException ee) {
                        assertThat(ExceptionUtils.findThrowable(ee, RestClientException.class))
                                .isPresent();
                    }
                }
            } finally {
                restClusterClient.close();
            }
        }
    }

    private class TestSavepointDisposalHandlers {

        private final TriggerId triggerId = new TriggerId();

        private final String savepointPath;

        private TestSavepointDisposalHandlers(String savepointPath) {
            this.savepointPath = Preconditions.checkNotNull(savepointPath);
        }

        private class TestSavepointDisposalTriggerHandler
                extends TestHandler<
                        SavepointDisposalRequest, TriggerResponse, EmptyMessageParameters> {
            private TestSavepointDisposalTriggerHandler() {
                super(SavepointDisposalTriggerHeaders.getInstance());
            }

            @Override
            protected CompletableFuture<TriggerResponse> handleRequest(
                    @Nonnull HandlerRequest<SavepointDisposalRequest> request,
                    @Nonnull DispatcherGateway gateway) {
                assertThat(request.getRequestBody().getSavepointPath()).isEqualTo(savepointPath);
                return CompletableFuture.completedFuture(new TriggerResponse(triggerId));
            }
        }

        private class TestSavepointDisposalStatusHandler
                extends TestHandler<
                        EmptyRequestBody,
                        AsynchronousOperationResult<AsynchronousOperationInfo>,
                        SavepointDisposalStatusMessageParameters> {

            private final Queue<OptionalFailure<AsynchronousOperationInfo>> responses;

            private TestSavepointDisposalStatusHandler(
                    OptionalFailure<AsynchronousOperationInfo>... responses) {
                super(SavepointDisposalStatusHeaders.getInstance());
                this.responses = new ArrayDeque<>(Arrays.asList(responses));
            }

            @Override
            protected CompletableFuture<AsynchronousOperationResult<AsynchronousOperationInfo>>
                    handleRequest(
                            @Nonnull HandlerRequest<EmptyRequestBody> request,
                            @Nonnull DispatcherGateway gateway)
                            throws RestHandlerException {
                final TriggerId actualTriggerId =
                        request.getPathParameter(TriggerIdPathParameter.class);

                if (actualTriggerId.equals(triggerId)) {
                    final OptionalFailure<AsynchronousOperationInfo> nextResponse =
                            responses.poll();

                    if (nextResponse != null) {
                        if (nextResponse.isFailure()) {
                            throw new RestHandlerException(
                                    "Failure",
                                    HttpResponseStatus.BAD_REQUEST,
                                    nextResponse.getFailureCause());
                        } else {
                            return CompletableFuture.completedFuture(
                                    AsynchronousOperationResult.completed(
                                            nextResponse.getUnchecked()));
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
    public void testListCompletedClusterDatasetIds() {
        Set<AbstractID> expectedCompletedClusterDatasetIds = new HashSet<>();
        expectedCompletedClusterDatasetIds.add(new AbstractID());
        expectedCompletedClusterDatasetIds.add(new AbstractID());

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        new TestListCompletedClusterDatasetHandler(
                                expectedCompletedClusterDatasetIds))) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                final Set<AbstractID> returnedIds =
                        restClusterClient.listCompletedClusterDatasetIds().get();
                assertThat(returnedIds).isEqualTo(expectedCompletedClusterDatasetIds);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class TestListCompletedClusterDatasetHandler
            extends TestHandler<
                    EmptyRequestBody, ClusterDataSetListResponseBody, EmptyMessageParameters> {

        private final Set<AbstractID> intermediateDataSetIds;

        private TestListCompletedClusterDatasetHandler(Set<AbstractID> intermediateDataSetIds) {
            super(ClusterDataSetListHeaders.INSTANCE);
            this.intermediateDataSetIds = intermediateDataSetIds;
        }

        @Override
        protected CompletableFuture<ClusterDataSetListResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {

            Map<IntermediateDataSetID, DataSetMetaInfo> datasets = new HashMap<>();
            intermediateDataSetIds.forEach(
                    id ->
                            datasets.put(
                                    new IntermediateDataSetID(id),
                                    DataSetMetaInfo.withNumRegisteredPartitions(1, 1)));
            return CompletableFuture.completedFuture(ClusterDataSetListResponseBody.from(datasets));
        }
    }

    @Test
    public void testInvalidateClusterDataset() throws Exception {
        final IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
        final String exceptionMessage = "Test exception.";
        final FlinkException testException = new FlinkException(exceptionMessage);

        final TestClusterDatasetDeleteHandlers testClusterDatasetDeleteHandlers =
                new TestClusterDatasetDeleteHandlers(intermediateDataSetID);
        final TestClusterDatasetDeleteHandlers.TestClusterDatasetDeleteTriggerHandler
                testClusterDatasetDeleteTriggerHandler =
                        testClusterDatasetDeleteHandlers
                        .new TestClusterDatasetDeleteTriggerHandler();
        final TestClusterDatasetDeleteHandlers.TestClusterDatasetDeleteStatusHandler
                testClusterDatasetDeleteStatusHandler =
                        testClusterDatasetDeleteHandlers
                        .new TestClusterDatasetDeleteStatusHandler(
                                OptionalFailure.of(AsynchronousOperationInfo.complete()),
                                OptionalFailure.of(
                                        AsynchronousOperationInfo.completeExceptional(
                                                new SerializedThrowable(testException))),
                                OptionalFailure.ofFailure(testException));

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        testClusterDatasetDeleteStatusHandler,
                        testClusterDatasetDeleteTriggerHandler)) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                {
                    final CompletableFuture<Void> invalidateCacheFuture =
                            restClusterClient.invalidateClusterDataset(intermediateDataSetID);
                    assertThat(invalidateCacheFuture.get()).isNull();
                }

                {
                    final CompletableFuture<Void> invalidateCacheFuture =
                            restClusterClient.invalidateClusterDataset(intermediateDataSetID);

                    try {
                        invalidateCacheFuture.get();
                        fail("Expected an exception");
                    } catch (ExecutionException ee) {
                        assertThat(
                                        ExceptionUtils.findThrowableWithMessage(
                                                        ee, exceptionMessage)
                                                .isPresent())
                                .isTrue();
                    }
                }

                {
                    try {
                        restClusterClient.invalidateClusterDataset(intermediateDataSetID).get();
                        fail("Expected an exception.");
                    } catch (ExecutionException ee) {
                        assertThat(
                                        ExceptionUtils.findThrowable(ee, RestClientException.class)
                                                .isPresent())
                                .isTrue();
                    }
                }
            } finally {
                restClusterClient.close();
            }
        }
    }

    private class TestClusterDatasetDeleteHandlers {

        private final TriggerId triggerId = new TriggerId();

        private final IntermediateDataSetID intermediateDataSetID;

        private TestClusterDatasetDeleteHandlers(IntermediateDataSetID intermediateDatasetId) {
            this.intermediateDataSetID = Preconditions.checkNotNull(intermediateDatasetId);
        }

        private class TestClusterDatasetDeleteTriggerHandler
                extends TestHandler<
                        EmptyRequestBody,
                        TriggerResponse,
                        ClusterDataSetDeleteTriggerMessageParameters> {
            private TestClusterDatasetDeleteTriggerHandler() {
                super(ClusterDataSetDeleteTriggerHeaders.INSTANCE);
            }

            @Override
            protected CompletableFuture<TriggerResponse> handleRequest(
                    HandlerRequest<EmptyRequestBody> request, DispatcherGateway gateway)
                    throws RestHandlerException {
                assertThat(request.getPathParameter(ClusterDataSetIdPathParameter.class))
                        .isEqualTo(intermediateDataSetID);
                return CompletableFuture.completedFuture(new TriggerResponse(triggerId));
            }
        }

        private class TestClusterDatasetDeleteStatusHandler
                extends TestHandler<
                        EmptyRequestBody,
                        AsynchronousOperationResult<AsynchronousOperationInfo>,
                        ClusterDataSetDeleteStatusMessageParameters> {

            private final Queue<OptionalFailure<AsynchronousOperationInfo>> responses;

            private TestClusterDatasetDeleteStatusHandler(
                    OptionalFailure<AsynchronousOperationInfo>... responses) {
                super(ClusterDataSetDeleteStatusHeaders.INSTANCE);
                this.responses = new ArrayDeque<>(Arrays.asList(responses));
            }

            @Override
            protected CompletableFuture<AsynchronousOperationResult<AsynchronousOperationInfo>>
                    handleRequest(
                            @Nonnull HandlerRequest<EmptyRequestBody> request,
                            @Nonnull DispatcherGateway gateway)
                            throws RestHandlerException {
                final TriggerId actualTriggerId =
                        request.getPathParameter(TriggerIdPathParameter.class);

                if (actualTriggerId.equals(triggerId)) {
                    final OptionalFailure<AsynchronousOperationInfo> nextResponse =
                            responses.poll();

                    if (nextResponse != null) {
                        if (nextResponse.isFailure()) {
                            throw new RestHandlerException(
                                    "Failure",
                                    HttpResponseStatus.BAD_REQUEST,
                                    nextResponse.getFailureCause());
                        } else {
                            return CompletableFuture.completedFuture(
                                    AsynchronousOperationResult.completed(
                                            nextResponse.getUnchecked()));
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
    void testListJobs() throws Exception {
        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new TestListJobsHandler())) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture =
                        restClusterClient.listJobs();
                Collection<JobStatusMessage> jobDetails = jobDetailsFuture.get();
                Iterator<JobStatusMessage> jobDetailsIterator = jobDetails.iterator();
                JobStatusMessage job1 = jobDetailsIterator.next();
                JobStatusMessage job2 = jobDetailsIterator.next();
                assertThat(job1.getJobState()).isNotEqualByComparingTo(job2.getJobState());
            } finally {
                restClusterClient.close();
            }
        }
    }

    @Test
    void testGetAccumulators() throws Exception {
        TestAccumulatorHandler accumulatorHandler = new TestAccumulatorHandler();

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(accumulatorHandler)) {

            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                JobID id = new JobID();
                Map<String, Object> accumulators = restClusterClient.getAccumulators(id).get();
                assertThat(accumulators).hasSize(1).containsEntry("testKey", "testValue");
            }
        }
    }

    /** Tests that command line options override the configuration settings. */
    @Test
    void testRESTManualConfigurationOverride() throws Exception {
        final String manualHostname = "123.123.123.123";
        final int manualPort = 4321;
        final String httpProtocol = "http";
        final String[] args = {"-m", manualHostname + ':' + manualPort};

        final RestClusterClient<?> clusterClient = getRestClusterClient(args);

        URL webMonitorBaseUrl = clusterClient.getWebMonitorBaseUrl().get();
        assertThat(webMonitorBaseUrl).hasHost(manualHostname).hasPort(manualPort);
        assertThat(clusterClient.getJobmanagerUrl())
                .hasHost(manualHostname)
                .hasPort(manualPort)
                .hasNoPath()
                .hasProtocol(httpProtocol);
        assertThat(clusterClient.getCustomHttpHeaders()).isEmpty();

        final String urlPath = "/some/path/here/index.html";
        final String httpsProtocol = "https";
        final String[] httpsUrlArgs = {
            "-m", httpsProtocol + "://" + manualHostname + ':' + manualPort + urlPath
        };

        final Map<String, String> envMap =
                Collections.singletonMap(
                        ConfigConstants.FLINK_REST_CLIENT_HEADERS,
                        "Cookie:authCookie=12:345\nCustomHeader:value1,value2\nMalformedHeaderSkipped");
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(envMap);

        final RestClusterClient<?> newClusterClient = getRestClusterClient(httpsUrlArgs);
        assertThat(newClusterClient.getWebMonitorBaseUrl().get())
                .hasHost(manualHostname)
                .hasPort(manualPort);

        final URL jobManagerUrl = newClusterClient.getJobmanagerUrl();
        assertThat(jobManagerUrl)
                .hasHost(manualHostname)
                .hasPort(manualPort)
                .hasPath(urlPath)
                .hasProtocol(httpsProtocol);

        final List<HttpHeader> customHttpHeaders =
                new ArrayList<>(newClusterClient.getCustomHttpHeaders());
        final HttpHeader expectedHeader1 = new HttpHeader("Cookie", "authCookie=12:345");
        final HttpHeader expectedHeader2 = new HttpHeader("CustomHeader", "value1,value2");
        assertThat(customHttpHeaders).hasSize(2);
        assertThat(customHttpHeaders.get(0)).isEqualTo(expectedHeader1);
        assertThat(customHttpHeaders.get(1)).isEqualTo(expectedHeader2);
    }

    private static RestClusterClient<?> getRestClusterClient(String[] args)
            throws CliArgsException, FlinkException {
        final DefaultCLI defaultCLI = new DefaultCLI();

        CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);

        final ClusterClientServiceLoader serviceLoader = new DefaultClusterClientServiceLoader();
        final Configuration executorConfig = defaultCLI.toConfiguration(commandLine);

        final ClusterClientFactory<StandaloneClusterId> clusterFactory =
                serviceLoader.getClusterClientFactory(executorConfig);
        checkState(clusterFactory != null);

        final ClusterDescriptor<StandaloneClusterId> clusterDescriptor =
                clusterFactory.createClusterDescriptor(executorConfig);
        final RestClusterClient<?> clusterClient =
                (RestClusterClient<?>)
                        clusterDescriptor
                                .retrieve(clusterFactory.getClusterId(executorConfig))
                                .getClusterClient();
        return clusterClient;
    }

    /** Tests that the send operation is being retried. */
    @Test
    void testRetriableSendOperationIfConnectionErrorOrServiceUnavailable() throws Exception {
        final PingRestHandler pingRestHandler =
                new PingRestHandler(
                        FutureUtils.completedExceptionally(
                                new RestHandlerException(
                                        "test exception", HttpResponseStatus.SERVICE_UNAVAILABLE)),
                        CompletableFuture.completedFuture(EmptyResponseBody.getInstance()));

        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(pingRestHandler)) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                final AtomicBoolean firstPollFailed = new AtomicBoolean();
                failHttpRequest =
                        (messageHeaders, messageParameters, requestBody) ->
                                messageHeaders instanceof PingRestHandlerHeaders
                                        && !firstPollFailed.getAndSet(true);

                restClusterClient.sendRequest(PingRestHandlerHeaders.INSTANCE).get();
            } finally {
                restClusterClient.close();
            }
        }
    }

    private class TestJobExecutionResultHandler
            extends TestHandler<
                    EmptyRequestBody, JobExecutionResultResponseBody, JobMessageParameters> {

        private final Iterator<Object> jobExecutionResults;

        private Object lastJobExecutionResult;

        private TestJobExecutionResultHandler(final Object... jobExecutionResults) {
            super(JobExecutionResultHeaders.getInstance());
            checkArgument(
                    Arrays.stream(jobExecutionResults)
                            .allMatch(
                                    object ->
                                            object instanceof JobExecutionResultResponseBody
                                                    || object instanceof RestHandlerException));
            this.jobExecutionResults = Arrays.asList(jobExecutionResults).iterator();
        }

        @Override
        protected CompletableFuture<JobExecutionResultResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway) {
            if (jobExecutionResults.hasNext()) {
                lastJobExecutionResult = jobExecutionResults.next();
            }
            checkState(lastJobExecutionResult != null);
            if (lastJobExecutionResult instanceof JobExecutionResultResponseBody) {
                return CompletableFuture.completedFuture(
                        (JobExecutionResultResponseBody) lastJobExecutionResult);
            } else if (lastJobExecutionResult instanceof RestHandlerException) {
                return FutureUtils.completedExceptionally(
                        (RestHandlerException) lastJobExecutionResult);
            } else {
                throw new AssertionError();
            }
        }
    }

    @Test
    void testJobSubmissionRespectsConfiguredRetryPolicy() throws Exception {
        final int maxRetryAttempts = 3;
        final AtomicInteger failedRequest = new AtomicInteger(0);
        failHttpRequest =
                (messageHeaders, messageParameters, requestBody) -> {
                    failedRequest.incrementAndGet();
                    // Fail all job submissions.
                    return true;
                };

        final Configuration clientConfig = new Configuration(restConfig);
        clientConfig.set(RestOptions.RETRY_MAX_ATTEMPTS, maxRetryAttempts);
        clientConfig.set(RestOptions.RETRY_DELAY, 10L);
        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new TestJobSubmitHandler())) {
            final InetSocketAddress serverAddress =
                    Objects.requireNonNull(restServerEndpoint.getServerAddress());
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(serverAddress.getPort(), clientConfig)) {
                assertThatThrownBy(() -> restClusterClient.submitJob(jobGraph).get())
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .cause()
                        .isInstanceOf(FutureUtils.RetryException.class);

                assertThat(failedRequest).hasValue(maxRetryAttempts + 1);
            }
        }
    }

    @Test
    void testSubmitJobAndWaitForExecutionResult() throws Exception {
        final TestJobExecutionResultHandler testJobExecutionResultHandler =
                new TestJobExecutionResultHandler(
                        new RestHandlerException(
                                "should trigger retry", HttpResponseStatus.SERVICE_UNAVAILABLE),
                        JobExecutionResultResponseBody.inProgress(),
                        // On an UNKNOWN JobResult it should be retried
                        JobExecutionResultResponseBody.created(
                                new JobResult.Builder()
                                        .applicationStatus(ApplicationStatus.UNKNOWN)
                                        .jobId(jobId)
                                        .netRuntime(Long.MAX_VALUE)
                                        .accumulatorResults(
                                                Collections.singletonMap(
                                                        "testName",
                                                        new SerializedValue<>(
                                                                OptionalFailure.of(1.0))))
                                        .build()),
                        JobExecutionResultResponseBody.created(
                                new JobResult.Builder()
                                        .applicationStatus(ApplicationStatus.SUCCEEDED)
                                        .jobId(jobId)
                                        .netRuntime(Long.MAX_VALUE)
                                        .accumulatorResults(
                                                Collections.singletonMap(
                                                        "testName",
                                                        new SerializedValue<>(
                                                                OptionalFailure.of(1.0))))
                                        .build()),
                        JobExecutionResultResponseBody.created(
                                new JobResult.Builder()
                                        .applicationStatus(ApplicationStatus.FAILED)
                                        .jobId(jobId)
                                        .netRuntime(Long.MAX_VALUE)
                                        .serializedThrowable(
                                                new SerializedThrowable(
                                                        new RuntimeException("expected")))
                                        .build()));

        // Fail the first JobExecutionResult HTTP polling attempt, which should not be a problem
        // because of the retries.
        final AtomicBoolean firstExecutionResultPollFailed = new AtomicBoolean(false);
        // Fail the first JobSubmit HTTP request, which should not be a problem because of the
        // retries.
        final AtomicBoolean firstSubmitRequestFailed = new AtomicBoolean(false);
        failHttpRequest =
                (messageHeaders, messageParameters, requestBody) -> {
                    if (messageHeaders instanceof JobExecutionResultHeaders) {
                        return !firstExecutionResultPollFailed.getAndSet(true);
                    }
                    if (messageHeaders instanceof JobSubmitHeaders) {
                        return !firstSubmitRequestFailed.getAndSet(true);
                    }
                    return false;
                };

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        testJobExecutionResultHandler, new TestJobSubmitHandler())) {

            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                final JobExecutionResult jobExecutionResult =
                        restClusterClient
                                .submitJob(jobGraph)
                                .thenCompose(restClusterClient::requestJobResult)
                                .get()
                                .toJobExecutionResult(ClassLoader.getSystemClassLoader());
                assertThat(firstExecutionResultPollFailed).isTrue();
                assertThat(firstSubmitRequestFailed).isTrue();
                assertThat(jobExecutionResult.getJobID()).isEqualTo(jobId);
                assertThat(jobExecutionResult.getNetRuntime()).isEqualTo(Long.MAX_VALUE);
                assertThat(jobExecutionResult.getAllAccumulatorResults())
                        .hasSize(1)
                        .containsEntry("testName", 1.0);

                try {
                    restClusterClient
                            .submitJob(jobGraph)
                            .thenCompose(restClusterClient::requestJobResult)
                            .get()
                            .toJobExecutionResult(ClassLoader.getSystemClassLoader());
                    fail("Expected exception not thrown.");
                } catch (final Exception e) {
                    final Optional<RuntimeException> cause =
                            ExceptionUtils.findThrowable(e, RuntimeException.class);
                    assertThat(cause).isPresent();
                    assertThat(cause.get().getMessage()).isEqualTo("expected");
                }
            }
        }
    }

    @Test
    @Timeout(value = 120_000, unit = TimeUnit.MILLISECONDS)
    void testJobSubmissionWithoutUserArtifact() throws Exception {
        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new TestJobSubmitHandler())) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {

                restClusterClient.submitJob(jobGraph).get();
            }
        }
    }

    @Test
    @Timeout(value = 120_000, unit = TimeUnit.MILLISECONDS)
    void testJobSubmissionWithUserArtifact(@TempDir java.nio.file.Path temporaryPath)
            throws Exception {
        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new TestJobSubmitHandler())) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {

                File file = temporaryPath.resolve("hello.txt").toFile();
                Files.write(file.toPath(), "hello world".getBytes(ConfigConstants.DEFAULT_CHARSET));

                // Add file path with scheme
                jobGraph.addUserArtifact(
                        "file",
                        new DistributedCache.DistributedCacheEntry(file.toURI().toString(), false));

                // Add file path without scheme
                jobGraph.addUserArtifact(
                        "file2",
                        new DistributedCache.DistributedCacheEntry(file.toURI().getPath(), false));

                restClusterClient.submitJob(jobGraph).get();
            }
        }
    }

    @Test
    void testJobSubmissionFailureCauseForwardedToClient() throws Exception {
        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new SubmissionFailingHandler())) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                restClusterClient
                        .submitJob(jobGraph)
                        .thenCompose(restClusterClient::requestJobResult)
                        .get()
                        .toJobExecutionResult(ClassLoader.getSystemClassLoader());
            } catch (final Exception e) {
                assertThat(
                                ExceptionUtils.findThrowableWithMessage(
                                        e, "RestHandlerException: expected"))
                        .isPresent();
                return;
            }
            fail("Should failed with exception");
        }
    }

    @Test
    void testJobGraphFileCleanedUpOnJobSubmissionFailure() throws Exception {
        final Path jobGraphFileDir = getTempDir();
        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(new SubmissionFailingHandler())) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                assertThatThrownBy(() -> restClusterClient.submitJob(jobGraph).join())
                        .hasCauseInstanceOf(JobSubmissionException.class);
                try (Stream<Path> files = Files.list(jobGraphFileDir)) {
                    assertThat(files)
                            .noneMatch(
                                    path ->
                                            path.toString()
                                                    .contains(jobGraph.getJobID().toString()));
                }
            }
        }
    }

    private static Path getTempDir() throws IOException {
        Path tempFile = Files.createTempFile("test", ".bin");
        Path tempDir = tempFile.getParent();
        Files.delete(tempFile);
        return tempDir;
    }

    private final class SubmissionFailingHandler
            extends TestHandler<
                    JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

        private SubmissionFailingHandler() {
            super(JobSubmitHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<JobSubmitResponseBody> handleRequest(
                @Nonnull HandlerRequest<JobSubmitRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            throw new RestHandlerException("expected", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Tests that the send operation is not being retried when receiving a NOT_FOUND return code.
     */
    @Test
    void testSendIsNotRetriableIfHttpNotFound() throws Exception {
        final String exceptionMessage = "test exception";
        final PingRestHandler pingRestHandler =
                new PingRestHandler(
                        FutureUtils.completedExceptionally(
                                new RestHandlerException(
                                        exceptionMessage, HttpResponseStatus.NOT_FOUND)));

        try (final TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(pingRestHandler)) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                restClusterClient.sendRequest(PingRestHandlerHeaders.INSTANCE).get();
                fail("The rest request should have failed.");
            } catch (Exception e) {
                assertThat(ExceptionUtils.findThrowableWithMessage(e, exceptionMessage))
                        .isPresent();
            } finally {
                restClusterClient.close();
            }
        }
    }

    private class PingRestHandler
            extends TestHandler<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        private final Queue<CompletableFuture<EmptyResponseBody>> responseQueue;

        private PingRestHandler(CompletableFuture<EmptyResponseBody>... responses) {
            super(PingRestHandlerHeaders.INSTANCE);
            responseQueue = new ArrayDeque<>(Arrays.asList(responses));
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            final CompletableFuture<EmptyResponseBody> result = responseQueue.poll();

            if (result != null) {
                return result;
            } else {
                return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
            }
        }
    }

    private static final class PingRestHandlerHeaders
            implements RuntimeMessageHeaders<
                    EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

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

    @Test
    void testSendCoordinationRequest() throws Exception {
        final TestClientCoordinationHandler handler = new TestClientCoordinationHandler();

        try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(handler)) {
            RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            String payload = "testing payload";
            TestCoordinationRequest<String> request = new TestCoordinationRequest<>(payload);
            try {
                CompletableFuture<CoordinationResponse> future =
                        restClusterClient.sendCoordinationRequest(jobId, new OperatorID(), request);
                TestCoordinationResponse response = (TestCoordinationResponse) future.get();

                assertThat(response.payload).isEqualTo(payload);
            } finally {
                restClusterClient.close();
            }
        }
    }

    @Test
    void testSendCoordinationRequestException() throws Exception {
        final TestClientCoordinationHandler handler =
                new TestClientCoordinationHandler(new FlinkJobNotFoundException(jobId));
        try (TestRestServerEndpoint restServerEndpoint = createRestServerEndpoint(handler)) {
            try (RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort())) {
                String payload = "testing payload";
                TestCoordinationRequest<String> request = new TestCoordinationRequest<>(payload);

                assertThatThrownBy(
                                () ->
                                        restClusterClient
                                                .sendCoordinationRequest(
                                                        jobId, new OperatorID(), request)
                                                .get())
                        .matches(
                                e ->
                                        ExceptionUtils.findThrowableWithMessage(
                                                        e,
                                                        FlinkJobNotFoundException.class.getName())
                                                .isPresent());
            }
        }
    }

    /**
     * The SUSPENDED job status should never be returned by the client thus client retries until it
     * either receives a different job status or the cluster is not reachable.
     */
    @Test
    void testNotShowSuspendedJobStatus() throws Exception {
        final List<JobStatusInfo> jobStatusInfo = new ArrayList<>();
        jobStatusInfo.add(new JobStatusInfo(JobStatus.SUSPENDED));
        jobStatusInfo.add(new JobStatusInfo(JobStatus.RUNNING));
        final TestJobStatusHandler jobStatusHandler =
                new TestJobStatusHandler(jobStatusInfo.iterator());

        try (TestRestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(jobStatusHandler)) {
            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());
            try {
                final CompletableFuture<JobStatus> future = restClusterClient.getJobStatus(jobId);
                assertThat(future.get()).isEqualTo(JobStatus.RUNNING);
            } finally {
                restClusterClient.close();
            }
        }
    }

    private class TestClientCoordinationHandler
            extends TestHandler<
                    ClientCoordinationRequestBody,
                    ClientCoordinationResponseBody,
                    ClientCoordinationMessageParameters> {
        @Nullable private final FlinkJobNotFoundException exception;

        private TestClientCoordinationHandler() {
            this(null);
        }

        private TestClientCoordinationHandler(@Nullable FlinkJobNotFoundException exception) {
            super(ClientCoordinationHeaders.getInstance());
            this.exception = exception;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected CompletableFuture<ClientCoordinationResponseBody> handleRequest(
                @Nonnull HandlerRequest<ClientCoordinationRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            try {
                if (exception != null) {
                    throw exception;
                }
                TestCoordinationRequest req =
                        (TestCoordinationRequest)
                                request.getRequestBody()
                                        .getSerializedCoordinationRequest()
                                        .deserializeValue(getClass().getClassLoader());
                TestCoordinationResponse resp = new TestCoordinationResponse(req.payload);
                return CompletableFuture.completedFuture(
                        new ClientCoordinationResponseBody(new SerializedValue<>(resp)));
            } catch (Exception e) {
                return FutureUtils.completedExceptionally(e);
            }
        }
    }

    private static class TestCoordinationRequest<T> implements CoordinationRequest {

        private static final long serialVersionUID = 1L;

        private final T payload;

        private TestCoordinationRequest(T payload) {
            this.payload = payload;
        }
    }

    private static class TestCoordinationResponse<T> implements CoordinationResponse {

        private static final long serialVersionUID = 1L;

        private final T payload;

        private TestCoordinationResponse(T payload) {
            this.payload = payload;
        }
    }

    private class TestAccumulatorHandler
            extends TestHandler<
                    EmptyRequestBody, JobAccumulatorsInfo, JobAccumulatorsMessageParameters> {

        public TestAccumulatorHandler() {
            super(JobAccumulatorsHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<JobAccumulatorsInfo> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            JobAccumulatorsInfo accumulatorsInfo;
            List<Boolean> queryParams =
                    request.getQueryParameter(
                            AccumulatorsIncludeSerializedValueQueryParameter.class);

            final boolean includeSerializedValue;
            if (!queryParams.isEmpty()) {
                includeSerializedValue = queryParams.get(0);
            } else {
                includeSerializedValue = false;
            }

            List<JobAccumulatorsInfo.UserTaskAccumulator> userTaskAccumulators = new ArrayList<>(1);

            userTaskAccumulators.add(
                    new JobAccumulatorsInfo.UserTaskAccumulator(
                            "testName", "testType", "testValue"));

            if (includeSerializedValue) {
                Map<String, SerializedValue<OptionalFailure<Object>>>
                        serializedUserTaskAccumulators = new HashMap<>(1);
                try {
                    serializedUserTaskAccumulators.put(
                            "testKey", new SerializedValue<>(OptionalFailure.of("testValue")));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                accumulatorsInfo =
                        new JobAccumulatorsInfo(
                                Collections.emptyList(),
                                userTaskAccumulators,
                                serializedUserTaskAccumulators);
            } else {
                accumulatorsInfo =
                        new JobAccumulatorsInfo(
                                Collections.emptyList(),
                                userTaskAccumulators,
                                Collections.emptyMap());
            }

            return CompletableFuture.completedFuture(accumulatorsInfo);
        }
    }

    private class TestListJobsHandler
            extends TestHandler<EmptyRequestBody, MultipleJobsDetails, EmptyMessageParameters> {

        private TestListJobsHandler() {
            super(JobsOverviewHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<MultipleJobsDetails> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            JobDetails running =
                    new JobDetails(
                            new JobID(), "job1", 0, 0, 0, JobStatus.RUNNING, 0, new int[10], 0);
            JobDetails finished =
                    new JobDetails(
                            new JobID(), "job2", 0, 0, 0, JobStatus.FINISHED, 0, new int[10], 0);
            return CompletableFuture.completedFuture(
                    new MultipleJobsDetails(Arrays.asList(running, finished)));
        }
    }

    private class TestJobStatusHandler
            extends TestHandler<EmptyRequestBody, JobStatusInfo, JobMessageParameters> {

        private final Iterator<JobStatusInfo> jobStatusInfo;

        private TestJobStatusHandler(@Nonnull Iterator<JobStatusInfo> jobStatusInfo) {
            super(JobStatusInfoHeaders.getInstance());
            checkState(jobStatusInfo.hasNext(), "Job status are empty");
            this.jobStatusInfo = checkNotNull(jobStatusInfo);
        }

        @Override
        protected CompletableFuture<JobStatusInfo> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {
            if (!jobStatusInfo.hasNext()) {
                throw new IllegalStateException("More job status were requested than configured");
            }
            return CompletableFuture.completedFuture(jobStatusInfo.next());
        }
    }

    private abstract class TestHandler<
                    R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
            extends AbstractRestHandler<DispatcherGateway, R, P, M> {

        private TestHandler(MessageHeaders<R, P, M> headers) {
            super(mockGatewayRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), headers);
        }
    }

    private TestRestServerEndpoint createRestServerEndpoint(
            final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws Exception {
        TestRestServerEndpoint.Builder builder = TestRestServerEndpoint.builder(restConfig);
        Arrays.stream(abstractRestHandlers).forEach(builder::withHandler);

        return builder.buildAndStart();
    }

    @FunctionalInterface
    private interface FailHttpRequestPredicate {

        boolean test(
                MessageHeaders<?, ?, ?> messageHeaders,
                MessageParameters messageParameters,
                RequestBody requestBody);

        static FailHttpRequestPredicate never() {
            return ((messageHeaders, messageParameters, requestBody) -> false);
        }
    }
}
