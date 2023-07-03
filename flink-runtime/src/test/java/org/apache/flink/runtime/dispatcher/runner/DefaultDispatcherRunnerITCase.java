/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrapFactory;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobPersistenceComponents;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.dispatcher.TestingJobMasterServiceLeadershipRunnerFactory;
import org.apache.flink.runtime.dispatcher.TestingPartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.TestingJobPersistenceComponentFactory;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link DefaultDispatcherRunner}. */
class DefaultDispatcherRunnerITCase {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDispatcherRunnerITCase.class);

    private static final Time TIMEOUT = Time.seconds(10L);

    @RegisterExtension
    public static AllCallbackWrapper<TestingRpcServiceExtension> rpcServiceExtensionWrapper =
            new AllCallbackWrapper<>(new TestingRpcServiceExtension());

    @RegisterExtension
    public static AllCallbackWrapper<BlobServerExtension> blobServerExtensionWrapper =
            new AllCallbackWrapper<>(new BlobServerExtension());

    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private JobGraph jobGraph;

    private TestingLeaderElection dispatcherLeaderElection;

    private TestingFatalErrorHandler fatalErrorHandler;

    private JobGraphStore jobGraphStore;

    private JobResultStore jobResultStore;

    private PartialDispatcherServices partialDispatcherServices;

    private DefaultDispatcherRunnerFactory dispatcherRunnerFactory;

    @BeforeEach
    void setup() {
        dispatcherRunnerFactory =
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE);
        jobGraph = createJobGraph();
        dispatcherLeaderElection = new TestingLeaderElection();
        fatalErrorHandler = new TestingFatalErrorHandler();
        jobGraphStore = TestingJobGraphStore.newBuilder().build();
        jobResultStore = new EmbeddedJobResultStore();

        partialDispatcherServices =
                TestingPartialDispatcherServices.builder()
                        .withFatalErrorHandler(fatalErrorHandler)
                        .build(
                                blobServerExtensionWrapper.getCustomExtension().getBlobServer(),
                                new Configuration());
    }

    @AfterEach
    void teardown() throws Exception {
        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
        }
    }

    @Test
    void leaderChange_afterJobSubmission_recoversSubmittedJob() throws Exception {
        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            final UUID firstLeaderSessionId = UUID.randomUUID();

            final DispatcherGateway firstDispatcherGateway =
                    electLeaderAndRetrieveGateway(firstLeaderSessionId);

            firstDispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

            dispatcherLeaderElection.notLeader();

            final UUID secondLeaderSessionId = UUID.randomUUID();
            final DispatcherGateway secondDispatcherGateway =
                    electLeaderAndRetrieveGateway(secondLeaderSessionId);

            final Collection<JobID> jobIds = secondDispatcherGateway.listJobs(TIMEOUT).get();

            assertThat(jobIds).containsExactly(jobGraph.getJobID());
        }
    }

    private DispatcherGateway electLeaderAndRetrieveGateway(UUID firstLeaderSessionId)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        return dispatcherLeaderElection
                .isLeader(firstLeaderSessionId)
                .thenCompose(
                        leaderInformation ->
                                rpcServiceExtensionWrapper
                                        .getCustomExtension()
                                        .getTestingRpcService()
                                        .connect(
                                                leaderInformation.getLeaderAddress(),
                                                DispatcherId.fromUuid(
                                                        leaderInformation.getLeaderSessionID()),
                                                DispatcherGateway.class))
                .get();
    }

    /**
     * See FLINK-11843. This is a probabilistic test which needs to be executed several times to
     * fail.
     */
    @Test
    void leaderChange_withBlockingJobManagerTermination_doesNotAffectNewLeader() throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory(1);
        final TestingCleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();
        dispatcherRunnerFactory =
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        new TestingDispatcherFactory(
                                jobManagerRunnerFactory, cleanupRunnerFactory));
        jobGraphStore = new SingleJobJobGraphStore(jobGraph);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {

            // initial run
            dispatcherLeaderElection.isLeader(UUID.randomUUID()).get();
            try (final TestingJobManagerRunner testingJobManagerRunner =
                    jobManagerRunnerFactory.takeCreatedJobManagerRunner()) {

                dispatcherLeaderElection.notLeader();

                LOG.info("Re-grant leadership first time.");
                dispatcherLeaderElection.isLeader(UUID.randomUUID());

                // give the Dispatcher some time to recover jobs
                Thread.sleep(1L);

                dispatcherLeaderElection.notLeader();

                LOG.info("Re-grant leadership second time.");
                final UUID leaderSessionId = UUID.randomUUID();
                final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                        dispatcherLeaderElection.isLeader(leaderSessionId);
                assertThat(confirmedLeaderInformation).isNotDone();

                LOG.info("Complete the termination of the first job manager runner.");
                testingJobManagerRunner.completeTerminationFuture();

                final LeaderInformation actualConfirmedLeaderInformation =
                        confirmedLeaderInformation.join();
                assertThat(actualConfirmedLeaderInformation.getLeaderSessionID())
                        .isEqualTo(leaderSessionId);

                // Wait for job to recover...
                final DispatcherGateway leaderGateway =
                        rpcServiceExtensionWrapper
                                .getCustomExtension()
                                .getTestingRpcService()
                                .connect(
                                        actualConfirmedLeaderInformation.getLeaderAddress(),
                                        DispatcherId.fromUuid(
                                                actualConfirmedLeaderInformation
                                                        .getLeaderSessionID()),
                                        DispatcherGateway.class)
                                .get();
                assertThatFuture(leaderGateway.listJobs(TIMEOUT))
                        .eventuallySucceeds()
                        .isEqualTo(Collections.singleton(jobGraph.getJobID()));
            }
        }
    }

    private static class TestingDispatcherFactory implements DispatcherFactory {
        private final JobManagerRunnerFactory jobManagerRunnerFactory;
        private final CleanupRunnerFactory cleanupRunnerFactory;

        private TestingDispatcherFactory(
                JobManagerRunnerFactory jobManagerRunnerFactory,
                CleanupRunnerFactory cleanupRunnerFactory) {
            this.jobManagerRunnerFactory = jobManagerRunnerFactory;
            this.cleanupRunnerFactory = cleanupRunnerFactory;
        }

        @Override
        public Dispatcher createDispatcher(
                RpcService rpcService,
                DispatcherId fencingToken,
                Collection<JobGraph> recoveredJobs,
                Collection<JobResult> recoveredDirtyJobResults,
                DispatcherBootstrapFactory dispatcherBootstrapFactory,
                PartialDispatcherServicesWithJobPersistenceComponents
                        partialDispatcherServicesWithJobPersistenceComponents)
                throws Exception {
            return new StandaloneDispatcher(
                    rpcService,
                    fencingToken,
                    recoveredJobs,
                    recoveredDirtyJobResults,
                    dispatcherBootstrapFactory,
                    DispatcherServices.from(
                            partialDispatcherServicesWithJobPersistenceComponents,
                            jobManagerRunnerFactory,
                            cleanupRunnerFactory));
        }
    }

    private static JobGraph createJobGraph() {
        return JobGraphTestUtils.singleNoOpJobGraph();
    }

    private DispatcherRunner createDispatcherRunner() throws Exception {
        return dispatcherRunnerFactory.createDispatcherRunner(
                dispatcherLeaderElection,
                fatalErrorHandler,
                new TestingJobPersistenceComponentFactory(jobGraphStore, jobResultStore),
                EXECUTOR_RESOURCE.getExecutor(),
                rpcServiceExtensionWrapper.getCustomExtension().getTestingRpcService(),
                partialDispatcherServices);
    }
}
