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

package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcessFactory;
import org.apache.flink.runtime.dispatcher.runner.TestingDispatcherGatewayService;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.TestingPersistenceComponentFactory;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.resourcemanager.TestingResourceManagerFactory;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.TestingApplicationResultStore;
import org.apache.flink.runtime.testutils.TestingApplicationStore;
import org.apache.flink.runtime.testutils.TestingExecutionPlanStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the delegation tokens of JobManagers which take over the leadership from each other. Every
 * simulated JobManager process has the real {@link ResourceManagerServiceImpl}, Dispatcher runner
 * and {@link DefaultDelegationTokenManager}. The leader election, the token service and the storage
 * are fakes, and the storage fails like the real one when it is used with an expired token.
 */
class DelegationTokenHaTakeoverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static TestingRpcService rpcService;

    private final MockTokenService tokenService = new MockTokenService();

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardownClass() throws Exception {
        RpcUtils.terminateRpcService(rpcService);
    }

    @Test
    void standbyKeepsTheTokensOfItsReceiverValid() throws Exception {
        try (JobManagerProcess standby = new JobManagerProcess("standby")) {
            standby.start();

            tokenService.expireEverythingIssuedSoFar();
            standby.tokens.obtainIsDue();

            assertThat(standby.tokens.receiver().heldGeneration()).isEqualTo(2);
            assertThat(tokenService.isValid(standby.tokens.receiver().heldGeneration())).isTrue();
        }
    }

    @Test
    void formerLeaderKeepsTheTokensOfItsReceiverValid() throws Exception {
        try (JobManagerProcess jobManager = new JobManagerProcess("former leader")) {
            jobManager.start();
            jobManager.becomeLeader();
            jobManager.loseLeadership();

            tokenService.expireEverythingIssuedSoFar();
            jobManager.tokens.obtainIsDue();

            assertThat(jobManager.tokens.receiver().heldGeneration()).isEqualTo(2);
            assertThat(tokenService.isValid(jobManager.tokens.receiver().heldGeneration()))
                    .isTrue();
        }
    }

    @Test
    void standbyTakesOverWhileTheTokenServiceHangs() throws Exception {
        try (JobManagerProcess leader = new JobManagerProcess("leader");
                JobManagerProcess standby = new JobManagerProcess("standby")) {
            leader.start();
            standby.start();
            leader.becomeLeader();

            // The time passes and both obtain new tokens, then the leader dies.
            tokenService.expireEverythingIssuedSoFar();
            leader.tokens.obtainIsDue();
            standby.tokens.obtainIsDue();
            leader.loseLeadership();

            standby.hangTokenService();
            final CompletableFuture<Void> takeover = standby.grantLeadership(UUID.randomUUID());
            CompletableFuture.anyOf(
                            standby.firstDispatcherCreated,
                            standby.fatalErrorHandler.getErrorFuture())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(standby.fatalErrorHandler.getException())
                    .as("fatal error of the standby which took over")
                    .isNull();
            assertThat(standby.firstDispatcherCreated).isDone();
            assertThat(takeover).succeedsWithin(TIMEOUT);
        }
    }

    @Test
    void standbyTakesOverWhileItsScheduledObtainHangs() throws Exception {
        final ExecutorService obtainer = Executors.newSingleThreadExecutor();
        try (JobManagerProcess standby = new JobManagerProcess("standby")) {
            standby.start();
            standby.hangTokenService();
            // The scheduled obtain is due and the token service does not answer it.
            final CompletableFuture<Void> hangingObtain =
                    CompletableFuture.runAsync(standby.tokens::obtainIsDue, obtainer);
            waitUtil(
                    () -> standby.tokens.provider().obtainCalls() == 2,
                    TIMEOUT,
                    "The scheduled obtain did not reach the token service");

            final CompletableFuture<Void> takeover = standby.grantLeadership(UUID.randomUUID());
            CompletableFuture.anyOf(
                            standby.firstDispatcherCreated,
                            standby.fatalErrorHandler.getErrorFuture())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(standby.fatalErrorHandler.getException())
                    .as("fatal error of the standby which took over")
                    .isNull();
            assertThat(standby.firstDispatcherCreated).isDone();
            assertThat(takeover).succeedsWithin(TIMEOUT);
            assertThat(hangingObtain)
                    .as("scheduled obtain which still waits for the token service")
                    .isNotDone();
        } finally {
            obtainer.shutdownNow();
        }
    }

    @Test
    void resourceManagerLeadershipIsConfirmedWhileTheTokenServiceHangs() throws Exception {
        try (JobManagerProcess jobManager = new JobManagerProcess("jobmanager")) {
            jobManager.start();
            jobManager.hangTokenService();
            final UUID leaderSessionId = UUID.randomUUID();

            final LeaderInformation confirmed =
                    jobManager
                            .resourceManagerElection
                            .isLeader(leaderSessionId)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(confirmed.getLeaderSessionID()).isEqualTo(leaderSessionId);
            assertThat(jobManager.tokens.provider().obtainCalls()).isEqualTo(1);
        }
    }

    @Test
    void alternatingLeadersKeepOneChainOfObtainsPerProcess() throws Exception {
        final int terms = 6;
        try (JobManagerProcess first = new JobManagerProcess("first");
                JobManagerProcess second = new JobManagerProcess("second")) {
            first.start();
            second.start();
            JobManagerProcess leader = first;
            JobManagerProcess standby = second;

            for (int term = 1; term <= terms; term++) {
                final int dispatchersBefore = leader.dispatcherCreations.get();
                leader.becomeLeader();
                assertThat(leader.fatalErrorHandler.getException())
                        .as("fatal error of %s in term %s", leader.name, term)
                        .isNull();
                assertThat(leader.dispatcherCreations)
                        .as("dispatchers created by %s in term %s", leader.name, term)
                        .hasValue(dispatchersBefore + 1);

                // The time passes: every token issued so far expires and every process obtains new
                // ones.
                tokenService.expireEverythingIssuedSoFar();
                first.tokens.obtainIsDue();
                second.tokens.obtainIsDue();
                assertThat(first.tokens.pendingObtainDelaysMillis())
                        .as("obtains waiting in %s in term %s", first.name, term)
                        .hasSize(1);
                assertThat(second.tokens.pendingObtainDelaysMillis())
                        .as("obtains waiting in %s in term %s", second.name, term)
                        .hasSize(1);

                leader.loseLeadership();
                final JobManagerProcess nextLeader = standby;
                standby = leader;
                leader = nextLeader;
            }

            assertThat(first.fatalErrorHandler.getException()).isNull();
            assertThat(second.fatalErrorHandler.getException()).isNull();
            // The start and the scheduled obtain of every term, the leader sessions obtain nothing.
            assertThat(first.tokens.provider().obtainCalls()).isEqualTo(1 + terms);
            assertThat(second.tokens.provider().obtainCalls()).isEqualTo(1 + terms);
        }
    }

    /**
     * One simulated JobManager process. The storage of the Dispatcher authenticates with the token
     * which the receiver of the process holds, as a file system does with its credentials.
     */
    private final class JobManagerProcess implements AutoCloseable {

        private final String name;
        private final DelegationTokenManagerTestHarness tokens =
                new DelegationTokenManagerTestHarness(tokenService);
        private final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        private final TestingLeaderElection resourceManagerElection = new TestingLeaderElection();
        private final TestingLeaderElection dispatcherElection = new TestingLeaderElection();
        private final CompletableFuture<Void> firstDispatcherCreated = new CompletableFuture<>();
        private final AtomicInteger dispatcherCreations = new AtomicInteger();
        private final CountDownLatch tokenServiceHangs = new CountDownLatch(1);
        private final ResourceManagerServiceImpl resourceManagerService;
        private final DispatcherRunner dispatcherRunner;

        private JobManagerProcess(String name) throws Exception {
            this.name = name;

            final TestingHighAvailabilityServices haServices =
                    new TestingHighAvailabilityServices();
            haServices.setResourceManagerLeaderElection(resourceManagerElection);
            resourceManagerService =
                    ResourceManagerServiceImpl.create(
                            new TestingResourceManagerFactory.Builder().build(),
                            new Configuration(),
                            ResourceID.generate(),
                            rpcService,
                            haServices,
                            new TestingHeartbeatServices(),
                            tokens.manager(),
                            fatalErrorHandler,
                            new ClusterInformation("localhost", 1234),
                            null,
                            TestingMetricRegistry.builder().build(),
                            "localhost",
                            ForkJoinPool.commonPool());

            dispatcherRunner =
                    DefaultDispatcherRunner.create(
                            dispatcherElection,
                            fatalErrorHandler,
                            new SessionDispatcherLeaderProcessFactory(
                                    (dispatcherId,
                                            recoveredJobs,
                                            recoveredDirtyJobResults,
                                            recoveredApplications,
                                            recoveredDirtyApplicationResults,
                                            executionPlanWriter,
                                            jobResultStore,
                                            applicationStore,
                                            applicationResultStore) -> {
                                        dispatcherCreations.incrementAndGet();
                                        firstDispatcherCreated.complete(null);
                                        return TestingDispatcherGatewayService.newBuilder().build();
                                    },
                                    new TestingPersistenceComponentFactory(
                                            TestingExecutionPlanStore.newBuilder().build(),
                                            new TokenCheckedJobResultStore(
                                                    tokens.receiver(), tokenService),
                                            TestingApplicationStore.newBuilder().build(),
                                            TestingApplicationResultStore.builder().build()),
                                    null,
                                    ForkJoinPool.commonPool(),
                                    fatalErrorHandler));
            resourceManagerService.start();
        }

        /** What the process does when it starts, whether it becomes the leader or not. */
        private void start() throws Exception {
            tokens.manager().start();
        }

        /** Whatever the process asks the token service for from now on does not return. */
        private void hangTokenService() {
            tokens.provider().duringObtain(tokenServiceHangs::await);
        }

        /** Grants the leadership of the Dispatcher and the ResourceManager. */
        private CompletableFuture<Void> grantLeadership(UUID leaderSessionId) {
            return CompletableFuture.allOf(
                    dispatcherElection.isLeader(leaderSessionId),
                    resourceManagerElection.isLeader(leaderSessionId));
        }

        private void becomeLeader() throws Exception {
            // The leadership must be confirmed before it is revoked, the testing leader election
            // calls the contender while it holds its monitor.
            grantLeadership(UUID.randomUUID()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void loseLeadership() throws Exception {
            final ResourceManager<?> resourceManager =
                    resourceManagerService.getLeaderResourceManager();
            resourceManagerElection.notLeader();
            dispatcherElection.notLeader();
            resourceManager.getTerminationFuture().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() throws Exception {
            tokenServiceHangs.countDown();
            dispatcherRunner.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            resourceManagerService.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** Fails to list the results the way the real storage does with an expired token. */
    private static final class TokenCheckedJobResultStore extends EmbeddedJobResultStore {

        private final MockDelegationTokenReceiver receiver;
        private final MockTokenService tokenService;

        private TokenCheckedJobResultStore(
                MockDelegationTokenReceiver receiver, MockTokenService tokenService) {
            this.receiver = receiver;
            this.tokenService = tokenService;
        }

        @Override
        public Set<JobResult> getDirtyResults() throws IOException {
            tokenService.requireValid(receiver.heldGeneration());
            return super.getDirtyResults();
        }
    }
}
