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
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrapFactory;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.dispatcher.TestingJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Integration tests for the {@link DefaultDispatcherRunner}. */
public class DefaultDispatcherRunnerITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDispatcherRunnerITCase.class);

    private static final Time TIMEOUT = Time.seconds(10L);

    @ClassRule
    public static TestingRpcServiceResource rpcServiceResource = new TestingRpcServiceResource();

    @ClassRule public static BlobServerResource blobServerResource = new BlobServerResource();

    private JobGraph jobGraph;

    private TestingLeaderElectionService dispatcherLeaderElectionService;

    private TestingFatalErrorHandler fatalErrorHandler;

    private JobGraphStore jobGraphStore;

    private PartialDispatcherServices partialDispatcherServices;

    private DefaultDispatcherRunnerFactory dispatcherRunnerFactory;

    @Before
    public void setup() {
        dispatcherRunnerFactory =
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE);
        jobGraph = createJobGraph();
        dispatcherLeaderElectionService = new TestingLeaderElectionService();
        fatalErrorHandler = new TestingFatalErrorHandler();
        jobGraphStore = TestingJobGraphStore.newBuilder().build();

        partialDispatcherServices =
                new PartialDispatcherServices(
                        new Configuration(),
                        new TestingHighAvailabilityServicesBuilder().build(),
                        CompletableFuture::new,
                        blobServerResource.getBlobServer(),
                        new TestingHeartbeatServices(),
                        UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup,
                        new MemoryExecutionGraphInfoStore(),
                        fatalErrorHandler,
                        VoidHistoryServerArchivist.INSTANCE,
                        null,
                        ForkJoinPool.commonPool());
    }

    @After
    public void teardown() throws Exception {
        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
        }
    }

    @Test
    public void leaderChange_afterJobSubmission_recoversSubmittedJob() throws Exception {
        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            final UUID firstLeaderSessionId = UUID.randomUUID();

            final DispatcherGateway firstDispatcherGateway =
                    electLeaderAndRetrieveGateway(firstLeaderSessionId);

            firstDispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

            dispatcherLeaderElectionService.notLeader();

            final UUID secondLeaderSessionId = UUID.randomUUID();
            final DispatcherGateway secondDispatcherGateway =
                    electLeaderAndRetrieveGateway(secondLeaderSessionId);

            final Collection<JobID> jobIds = secondDispatcherGateway.listJobs(TIMEOUT).get();

            assertThat(jobIds, contains(jobGraph.getJobID()));
        }
    }

    private DispatcherGateway electLeaderAndRetrieveGateway(UUID firstLeaderSessionId)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        dispatcherLeaderElectionService.isLeader(firstLeaderSessionId);
        final LeaderConnectionInfo leaderConnectionInfo =
                dispatcherLeaderElectionService.getConfirmationFuture().get();

        return rpcServiceResource
                .getTestingRpcService()
                .connect(
                        leaderConnectionInfo.getAddress(),
                        DispatcherId.fromUuid(leaderConnectionInfo.getLeaderSessionId()),
                        DispatcherGateway.class)
                .get();
    }

    /**
     * See FLINK-11843. This is a probabilistic test which needs to be executed several times to
     * fail.
     */
    @Test
    public void leaderChange_withBlockingJobManagerTermination_doesNotAffectNewLeader()
            throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                new TestingJobManagerRunnerFactory(1);
        dispatcherRunnerFactory =
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        new TestingDispatcherFactory(jobManagerRunnerFactory));
        jobGraphStore = new SingleJobJobGraphStore(jobGraph);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {

            // initial run
            dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();
            final TestingJobManagerRunner testingJobManagerRunner =
                    jobManagerRunnerFactory.takeCreatedJobManagerRunner();

            dispatcherLeaderElectionService.notLeader();

            LOG.info("Re-grant leadership first time.");
            dispatcherLeaderElectionService.isLeader(UUID.randomUUID());

            // give the Dispatcher some time to recover jobs
            Thread.sleep(1L);

            dispatcherLeaderElectionService.notLeader();

            LOG.info("Re-grant leadership second time.");
            final UUID leaderSessionId = UUID.randomUUID();
            final CompletableFuture<UUID> leaderFuture =
                    dispatcherLeaderElectionService.isLeader(leaderSessionId);
            assertThat(leaderFuture.isDone(), is(false));

            LOG.info("Complete the termination of the first job manager runner.");
            testingJobManagerRunner.completeTerminationFuture();

            assertThat(
                    leaderFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS),
                    is(equalTo(leaderSessionId)));

            // Wait for job to recover...
            final DispatcherGateway leaderGateway =
                    rpcServiceResource
                            .getTestingRpcService()
                            .connect(
                                    dispatcherLeaderElectionService.getAddress(),
                                    DispatcherId.fromUuid(leaderSessionId),
                                    DispatcherGateway.class)
                            .get();
            assertEquals(
                    jobGraph.getJobID(),
                    Iterables.getOnlyElement(leaderGateway.listJobs(TIMEOUT).get()));
        }
    }

    private static class TestingDispatcherFactory implements DispatcherFactory {
        private final JobManagerRunnerFactory jobManagerRunnerFactory;

        private TestingDispatcherFactory(JobManagerRunnerFactory jobManagerRunnerFactory) {
            this.jobManagerRunnerFactory = jobManagerRunnerFactory;
        }

        @Override
        public Dispatcher createDispatcher(
                RpcService rpcService,
                DispatcherId fencingToken,
                Collection<JobGraph> recoveredJobs,
                DispatcherBootstrapFactory dispatcherBootstrapFactory,
                PartialDispatcherServicesWithJobGraphStore
                        partialDispatcherServicesWithJobGraphStore)
                throws Exception {
            return new StandaloneDispatcher(
                    rpcService,
                    fencingToken,
                    recoveredJobs,
                    dispatcherBootstrapFactory,
                    DispatcherServices.from(
                            partialDispatcherServicesWithJobGraphStore, jobManagerRunnerFactory));
        }
    }

    private static JobGraph createJobGraph() {
        return JobGraphTestUtils.singleNoOpJobGraph();
    }

    private DispatcherRunner createDispatcherRunner() throws Exception {
        return dispatcherRunnerFactory.createDispatcherRunner(
                dispatcherLeaderElectionService,
                fatalErrorHandler,
                () -> jobGraphStore,
                TestingUtils.defaultExecutor(),
                rpcServiceResource.getTestingRpcService(),
                partialDispatcherServices);
    }
}
