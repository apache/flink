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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.EmbeddedApplicationResultStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.StandaloneApplicationStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingExecutionPlanStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.JobMdcRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how the {@link Dispatcher} resolves {@link PipelineOptions#PARALLELISM_OVERRIDES} for a
 * submitted {@link StreamGraph}: the cluster and job maps are merged once at submission (the job
 * map wins), the merged map is attached to the plan that is persisted and handed to the job master,
 * entries that are no job vertex id are dropped, a bad value only fails the job that contains the
 * vertex, and a recovered plan is not resolved again.
 *
 * <p>The {@link org.apache.flink.runtime.jobgraph.JobGraph} path is covered by {@link
 * DispatcherTest#testOverridingJobVertexParallelisms()}.
 */
class DispatcherParallelismOverridesTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static final TestingRpcServiceExtension RPC_SERVICE_EXTENSION =
            new TestingRpcServiceExtension();

    @RegisterExtension
    private static final AllCallbackWrapper<TestingRpcServiceExtension> RPC_SERVICE_WRAPPER =
            new AllCallbackWrapper<>(RPC_SERVICE_EXTENSION);

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension fatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @TempDir private File tempDir;

    private final JobID jobId = new JobID();
    private final ApplicationID applicationId = ApplicationID.fromHexString(jobId.toHexString());

    private Configuration configuration;
    private TestingHighAvailabilityServices haServices;
    private HeartbeatServices heartbeatServices;
    private BlobServer blobServer;
    private BlockingQueue<ExecutionPlan> persistedPlans;
    private CapturingJobManagerRunnerFactory runnerFactory;
    private TestingDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        configuration = new Configuration();
        heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);
        persistedPlans = new ArrayBlockingQueue<>(4);
        runnerFactory = new CapturingJobManagerRunnerFactory();

        haServices = new TestingHighAvailabilityServices();
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
        haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        final TestingExecutionPlanStore executionPlanStore =
                TestingExecutionPlanStore.newBuilder()
                        .setPutExecutionPlanConsumer(persistedPlans::add)
                        .build();
        executionPlanStore.start(null);
        haServices.setExecutionPlanStore(executionPlanStore);
        haServices.setJobResultStore(new EmbeddedJobResultStore());
        haServices.setApplicationStore(new StandaloneApplicationStore());
        haServices.setApplicationResultStore(new EmbeddedApplicationResultStore());

        blobServer = new BlobServer(configuration, tempDir, new VoidBlobStore());
    }

    @AfterEach
    void tearDown() throws Exception {
        JobMdcRegistry.clear();
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
        if (haServices != null) {
            haServices.closeWithOptionalClean(true);
        }
        if (blobServer != null) {
            blobServer.close();
        }
    }

    @Test
    void testStreamGraphSubmissionAttachesMergedOverrides() throws Exception {
        final String overriddenByCluster = new JobVertexID().toHexString();
        final String overriddenByBoth = new JobVertexID().toHexString();
        final String unknownInCluster = new JobVertexID().toHexString();
        final String unknownInJob = new JobVertexID().toHexString();

        final Map<String, String> clusterOverrides = new HashMap<>();
        clusterOverrides.put(overriddenByCluster, "10");
        clusterOverrides.put(overriddenByBoth, "21");
        clusterOverrides.put(unknownInCluster, "23");
        configuration.set(PipelineOptions.PARALLELISM_OVERRIDES, clusterOverrides);

        final Map<String, String> jobOverrides = new HashMap<>();
        jobOverrides.put(overriddenByBoth, "42");
        jobOverrides.put(unknownInJob, "25");
        final StreamGraph streamGraph = createStreamGraph(jobOverrides);

        startDispatcherAndSubmitApplication();
        dispatcher.getSelfGateway(DispatcherGateway.class).submitJob(streamGraph, TIMEOUT).get();

        final Map<String, String> expected = new HashMap<>();
        expected.put(overriddenByCluster, "10");
        expected.put(overriddenByBoth, "42");
        expected.put(unknownInCluster, "23");
        expected.put(unknownInJob, "25");

        final ExecutionPlan persisted =
                persistedPlans.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(persisted).isNotNull();
        assertThat(persisted.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES))
                .as("merged overrides in the persisted plan")
                .isEqualTo(expected);

        final ExecutionPlan started = runnerFactory.takePlan();
        assertThat(started.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES))
                .as("merged overrides in the plan handed to the job master")
                .isEqualTo(expected);
    }

    @Test
    void testStreamGraphSubmissionWithoutOverridesLeavesJobConfigurationUntouched()
            throws Exception {
        final StreamGraph streamGraph = createStreamGraph(Collections.emptyMap());

        startDispatcherAndSubmitApplication();
        dispatcher.getSelfGateway(DispatcherGateway.class).submitJob(streamGraph, TIMEOUT).get();

        assertThat(
                        runnerFactory
                                .takePlan()
                                .getJobConfiguration()
                                .getOptional(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEmpty();
    }

    @Test
    void testEntriesThatAreNoJobVertexIdAreDroppedFromTheAttachedOverrides() throws Exception {
        final String valid = new JobVertexID().toHexString();
        final Map<String, String> clusterOverrides = new HashMap<>();
        clusterOverrides.put(valid, "3");
        clusterOverrides.put("v1", "3");
        clusterOverrides.put(valid + "0", "3");
        clusterOverrides.put(valid.toUpperCase(), "3");
        configuration.set(PipelineOptions.PARALLELISM_OVERRIDES, clusterOverrides);
        final StreamGraph streamGraph = createStreamGraph(Collections.emptyMap());

        startDispatcherAndSubmitApplication();
        dispatcher.getSelfGateway(DispatcherGateway.class).submitJob(streamGraph, TIMEOUT).get();

        assertThat(
                        runnerFactory
                                .takePlan()
                                .getJobConfiguration()
                                .get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(Collections.singletonMap(valid, "3"));
    }

    @Test
    void testValueThatIsNoParallelismIsKeptForTheJobMasterToReject() throws Exception {
        // The Dispatcher does not know the vertices of a StreamGraph, so a bad value can only
        // fail the job whose vertex it is meant for, which happens when the JobGraph is generated.
        final String key = new JobVertexID().toHexString();
        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES, Collections.singletonMap(key, "abc"));
        final StreamGraph streamGraph = createStreamGraph(Collections.emptyMap());

        startDispatcherAndSubmitApplication();
        dispatcher.getSelfGateway(DispatcherGateway.class).submitJob(streamGraph, TIMEOUT).get();

        assertThat(
                        runnerFactory
                                .takePlan()
                                .getJobConfiguration()
                                .get(PipelineOptions.PARALLELISM_OVERRIDES))
                .isEqualTo(Collections.singletonMap(key, "abc"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidValues")
    void testValueThatIsNoParallelismFailsTheJobGraphSubmissionOfThatVertex(
            String description, String value) throws Exception {
        final JobVertex vertex = new JobVertex("vertex");
        vertex.setParallelism(1);
        vertex.setInvokableClass(NoOpInvokable.class);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);
        jobGraph.setJobID(jobId);
        jobGraph.setApplicationId(applicationId);
        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(vertex.getID().toHexString(), value));

        startDispatcherAndSubmitApplication();

        FlinkAssertions.assertThatFuture(
                        dispatcher
                                .getSelfGateway(DispatcherGateway.class)
                                .submitJob(jobGraph, TIMEOUT))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(JobSubmissionException.class)
                .withMessageContaining(vertex.getID().toHexString());
        assertThat(runnerFactory.isEmpty()).as("no job master started").isTrue();
    }

    private static Stream<Arguments> invalidValues() {
        return Stream.of(
                Arguments.of("non-numeric value", "abc"), Arguments.of("non-positive value", "0"));
    }

    @Test
    void testRecoveredStreamGraphIsNotResolvedAgain() throws Exception {
        final String overriddenByJob = new JobVertexID().toHexString();
        final String overriddenByCluster = new JobVertexID().toHexString();
        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(overriddenByCluster, "10"));
        final StreamGraph recovered =
                createStreamGraph(Collections.singletonMap(overriddenByJob, "42"));

        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(Collections.singleton(recovered))
                        .build(RPC_SERVICE_EXTENSION.getTestingRpcService());
        dispatcher.start();

        final ExecutionPlan started = runnerFactory.takePlan();
        assertThat(started.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES))
                .as("a recovered plan keeps the overrides resolved at its original submission")
                .isEqualTo(Collections.singletonMap(overriddenByJob, "42"));
    }

    private StreamGraph createStreamGraph(Map<String, String> jobOverrides) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(1, 2, 3).map(x -> x).print();
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobId(jobId);
        streamGraph.setApplicationId(applicationId);
        if (!jobOverrides.isEmpty()) {
            streamGraph
                    .getJobConfiguration()
                    .set(PipelineOptions.PARALLELISM_OVERRIDES, jobOverrides);
        }
        return streamGraph;
    }

    private void startDispatcherAndSubmitApplication() throws Exception {
        dispatcher =
                createTestingDispatcherBuilder()
                        .build(RPC_SERVICE_EXTENSION.getTestingRpcService());
        dispatcher.start();
        dispatcher
                .submitApplication(
                        TestingApplication.builder().setApplicationId(applicationId).build(),
                        TIMEOUT)
                .get();
    }

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        return TestingDispatcher.builder()
                .setConfiguration(configuration)
                .setHeartbeatServices(heartbeatServices)
                .setHighAvailabilityServices(haServices)
                .setExecutionPlanWriter(haServices.getExecutionPlanStore())
                .setJobResultStore(haServices.getJobResultStore())
                .setApplicationWriter(haServices.getApplicationStore())
                .setApplicationResultStore(haServices.getApplicationResultStore())
                .setJobManagerRunnerFactory(runnerFactory)
                .setCleanupRunnerFactory(CheckpointResourcesCleanupRunnerFactory.INSTANCE)
                .setFatalErrorHandler(fatalErrorHandlerExtension.getTestingFatalErrorHandler())
                .setBlobServer(blobServer);
    }

    /** Records the plan of every job master it is asked to start. */
    private static final class CapturingJobManagerRunnerFactory implements JobManagerRunnerFactory {

        private final BlockingQueue<ExecutionPlan> plans = new ArrayBlockingQueue<>(4);

        @Override
        public JobManagerRunner createJobManagerRunner(
                ExecutionPlan executionPlan,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                JobStatusListener jobStatusListener,
                long initializationTimestamp) {
            plans.add(executionPlan);
            return TestingJobManagerRunner.newBuilder().setJobId(executionPlan.getJobID()).build();
        }

        ExecutionPlan takePlan() throws InterruptedException {
            final ExecutionPlan plan = plans.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(plan).as("job master started within the timeout").isNotNull();
            return plan;
        }

        boolean isEmpty() {
            return plans.isEmpty();
        }
    }
}
