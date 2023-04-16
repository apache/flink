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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.security.token.NoOpDelegationTokenManager;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link ResourceManagerServiceImpl}. */
public class ResourceManagerServiceImplTest extends TestLogger {

    private static final HeartbeatServices heartbeatServices = new TestingHeartbeatServices();
    private static final DelegationTokenManager delegationTokenManager =
            new NoOpDelegationTokenManager();
    private static final ClusterInformation clusterInformation =
            new ClusterInformation("localhost", 1234);
    private static final MetricRegistry metricRegistry = TestingMetricRegistry.builder().build();

    private static TestingRpcService rpcService;
    private static TestingHighAvailabilityServices haService;
    private static TestingFatalErrorHandler fatalErrorHandler;

    private TestingResourceManagerFactory.Builder rmFactoryBuilder;
    private TestingLeaderElection leaderElection;

    private ResourceManagerServiceImpl resourceManagerService;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
        haService = new TestingHighAvailabilityServices();
        fatalErrorHandler = new TestingFatalErrorHandler();
    }

    @Before
    public void setup() throws Exception {

        fatalErrorHandler.clearError();

        rmFactoryBuilder = new TestingResourceManagerFactory.Builder();

        leaderElection = new TestingLeaderElection();
        haService.setResourceManagerLeaderElection(leaderElection);
    }

    @After
    public void teardown() throws Exception {
        leaderElection.close();

        if (resourceManagerService != null) {
            resourceManagerService.close();
        }

        if (fatalErrorHandler.hasExceptionOccurred()) {
            fatalErrorHandler.rethrowError();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    private void createAndStartResourceManager() throws Exception {
        createResourceManager();
        resourceManagerService.start();
    }

    private void createResourceManager() throws Exception {
        final TestingResourceManagerFactory rmFactory = rmFactoryBuilder.build();
        resourceManagerService =
                ResourceManagerServiceImpl.create(
                        rmFactory,
                        new Configuration(),
                        ResourceID.generate(),
                        rpcService,
                        haService,
                        heartbeatServices,
                        delegationTokenManager,
                        fatalErrorHandler,
                        clusterInformation,
                        null,
                        metricRegistry,
                        "localhost",
                        ForkJoinPool.commonPool());
    }

    @Test
    public void grantLeadership_startRmAndConfirmLeaderSession() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<UUID> startRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(startRmFuture::complete);

        createAndStartResourceManager();

        // grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(leaderSessionId);

        // should start new RM and confirm leader session
        assertThat(startRmFuture.get(), is(leaderSessionId));
        assertThat(confirmedLeaderInformation.get().getLeaderSessionID(), is(leaderSessionId));
    }

    @Test
    public void grantLeadership_confirmLeaderSessionAfterRmStarted() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<Void> finishRmInitializationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(
                (ignore) -> blockOnFuture(finishRmInitializationFuture));

        createAndStartResourceManager();

        // grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(leaderSessionId);

        // RM initialization not finished, should not confirm leader session
        assertNotComplete(confirmedLeaderInformation);

        // finish RM initialization
        finishRmInitializationFuture.complete(null);

        // should confirm leader session
        assertThat(confirmedLeaderInformation.get().getLeaderSessionID(), is(leaderSessionId));
    }

    @Test
    public void grantLeadership_withExistingLeader_stopExistLeader() throws Exception {
        final UUID leaderSessionId1 = UUID.randomUUID();
        final UUID leaderSessionId2 = UUID.randomUUID();
        final CompletableFuture<UUID> startRmFuture1 = new CompletableFuture<>();
        final CompletableFuture<UUID> startRmFuture2 = new CompletableFuture<>();
        final CompletableFuture<UUID> terminateRmFuture = new CompletableFuture<>();

        rmFactoryBuilder
                .setInitializeConsumer(
                        uuid -> {
                            if (!startRmFuture1.isDone()) {
                                startRmFuture1.complete(uuid);
                            } else {
                                startRmFuture2.complete(uuid);
                            }
                        })
                .setTerminateConsumer(terminateRmFuture::complete);

        createAndStartResourceManager();

        // first time grant leadership
        leaderElection.isLeader(leaderSessionId1).join();

        // second time grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(leaderSessionId2);

        // should terminate first RM, start a new RM and confirm leader session
        assertThat(terminateRmFuture.get(), is(leaderSessionId1));
        assertThat(startRmFuture2.get(), is(leaderSessionId2));
        assertThat(confirmedLeaderInformation.get().getLeaderSessionID(), is(leaderSessionId2));
    }

    @Test
    public void grantLeadership_withExistingLeader_waitTerminationOfExistingLeader()
            throws Exception {
        final UUID leaderSessionId1 = UUID.randomUUID();
        final UUID leaderSessionId2 = UUID.randomUUID();
        final CompletableFuture<UUID> startRmFuture1 = new CompletableFuture<>();
        final CompletableFuture<UUID> startRmFuture2 = new CompletableFuture<>();
        final CompletableFuture<Void> finishRmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder
                .setInitializeConsumer(
                        uuid -> {
                            if (!startRmFuture1.isDone()) {
                                startRmFuture1.complete(uuid);
                            } else {
                                startRmFuture2.complete(uuid);
                            }
                        })
                .setTerminateConsumer((ignore) -> blockOnFuture(finishRmTerminationFuture));

        createAndStartResourceManager();

        // first time grant leadership
        leaderElection.isLeader(leaderSessionId1).join();

        // second time grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(leaderSessionId2);

        // first RM termination not finished, should not start new RM
        assertNotComplete(startRmFuture2);

        // finish first RM termination
        finishRmTerminationFuture.complete(null);

        // should start new RM and confirm leader session
        assertThat(startRmFuture2.get(), is(leaderSessionId2));
        assertThat(confirmedLeaderInformation.get().getLeaderSessionID(), is(leaderSessionId2));
    }

    @Test
    public void grantLeadership_notStarted_doesNotStartNewRm() throws Exception {
        final CompletableFuture<UUID> startRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(startRmFuture::complete);

        createResourceManager();

        // grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(UUID.randomUUID());

        // service not started, should not start new RM
        assertNotComplete(startRmFuture);
        assertNotComplete(confirmedLeaderInformation);
    }

    @Test
    public void grantLeadership_stopped_doesNotStartNewRm() throws Exception {
        final CompletableFuture<UUID> startRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(startRmFuture::complete);

        createAndStartResourceManager();
        resourceManagerService.close();

        // grant leadership
        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                leaderElection.isLeader(UUID.randomUUID());

        // service stopped, should not start new RM
        assertNotComplete(startRmFuture);
        assertNotComplete(confirmedLeaderInformation);
    }

    @Test
    public void revokeLeadership_stopExistLeader() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<UUID> terminateRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setTerminateConsumer(terminateRmFuture::complete);

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(leaderSessionId).join();

        // revoke leadership
        leaderElection.notLeader();

        // should terminate RM
        assertThat(terminateRmFuture.get(), is(leaderSessionId));
    }

    @Test
    public void revokeLeadership_terminateService_multiLeaderSessionNotSupported()
            throws Exception {
        rmFactoryBuilder.setSupportMultiLeaderSession(false);

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(UUID.randomUUID()).join();

        // revoke leadership
        leaderElection.notLeader();

        // should terminate service
        resourceManagerService.getTerminationFuture().get();
    }

    @Test
    public void leaderRmTerminated_terminateService() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<Void> rmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setGetTerminationFutureFunction((ignore1, ignore2) -> rmTerminationFuture);

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(leaderSessionId).join();

        // terminate RM
        rmTerminationFuture.complete(null);

        // should terminate service
        resourceManagerService.getTerminationFuture().get();
    }

    @Test
    public void nonLeaderRmTerminated_doseNotTerminateService() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<UUID> terminateRmFuture = new CompletableFuture<>();
        final CompletableFuture<Void> rmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder
                .setTerminateConsumer(terminateRmFuture::complete)
                .setGetTerminationFutureFunction((ignore1, ignore2) -> rmTerminationFuture);

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(leaderSessionId).join();

        // revoke leadership
        leaderElection.notLeader();
        assertThat(terminateRmFuture.get(), is(leaderSessionId));

        // terminate RM
        rmTerminationFuture.complete(null);

        // should not terminate service
        assertNotComplete(resourceManagerService.getTerminationFuture());
    }

    @Test
    public void closeService_stopRmAndLeaderElection() throws Exception {
        final CompletableFuture<UUID> terminateRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setTerminateConsumer(terminateRmFuture::complete);

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(UUID.randomUUID()).join();

        assertFalse(leaderElection.isStopped());

        // close service
        resourceManagerService.close();

        // should stop RM and leader election
        assertTrue(terminateRmFuture.isDone());
        assertTrue(leaderElection.isStopped());
    }

    @Test
    public void closeService_futureCompleteAfterRmTerminated() throws Exception {
        final CompletableFuture<Void> finishRmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setTerminateConsumer((ignore) -> blockOnFuture(finishRmTerminationFuture));

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(UUID.randomUUID()).join();

        // close service
        final CompletableFuture<Void> closeServiceFuture = resourceManagerService.closeAsync();

        // RM termination not finished, future should not complete
        assertNotComplete(closeServiceFuture);

        // finish RM termination
        finishRmTerminationFuture.complete(null);

        closeServiceFuture.get();
    }

    @Test
    public void deregisterApplication_leaderRmNotStarted() throws Exception {
        final CompletableFuture<Void> startRmInitializationFuture = new CompletableFuture<>();
        final CompletableFuture<Void> finishRmInitializationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(
                (ignore) -> {
                    startRmInitializationFuture.complete(null);
                    blockOnFuture(finishRmInitializationFuture);
                });

        createAndStartResourceManager();

        // grant leadership
        leaderElection.isLeader(UUID.randomUUID());

        // make sure leader RM is created
        startRmInitializationFuture.get();

        // deregister application
        final CompletableFuture<Void> deregisterApplicationFuture =
                resourceManagerService.deregisterApplication(ApplicationStatus.CANCELED, null);

        // RM not fully started, future should not complete
        assertNotComplete(deregisterApplicationFuture);

        // finish starting RM
        finishRmInitializationFuture.complete(null);

        // should perform deregistration
        deregisterApplicationFuture.get();
    }

    @Test
    public void deregisterApplication_noLeaderRm() throws Exception {
        createAndStartResourceManager();
        final CompletableFuture<Void> deregisterApplicationFuture =
                resourceManagerService.deregisterApplication(ApplicationStatus.CANCELED, null);

        // should not report error
        deregisterApplicationFuture.get();
    }

    @Test
    public void grantAndRevokeLeadership_verifyMetrics() throws Exception {
        final Set<String> registeredMetrics = Collections.newSetFromMap(new ConcurrentHashMap<>());
        TestingMetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer((a, b, c) -> registeredMetrics.add(b))
                        .setUnregisterConsumer((a, b, c) -> registeredMetrics.remove(b))
                        .build();

        final TestingResourceManagerFactory rmFactory = rmFactoryBuilder.build();
        resourceManagerService =
                ResourceManagerServiceImpl.create(
                        rmFactory,
                        new Configuration(),
                        ResourceID.generate(),
                        rpcService,
                        haService,
                        heartbeatServices,
                        delegationTokenManager,
                        fatalErrorHandler,
                        clusterInformation,
                        null,
                        metricRegistry,
                        "localhost",
                        ForkJoinPool.commonPool());
        resourceManagerService.start();

        Assert.assertEquals(0, registeredMetrics.size());
        // grant leadership
        leaderElection.isLeader(UUID.randomUUID()).join();

        Set<String> expectedMetrics =
                Sets.set(
                        MetricNames.NUM_REGISTERED_TASK_MANAGERS,
                        MetricNames.TASK_SLOTS_TOTAL,
                        MetricNames.TASK_SLOTS_AVAILABLE);
        Assert.assertTrue(
                "Expected RM to register leader metrics",
                registeredMetrics.containsAll(expectedMetrics));

        // revoke leadership, block until old rm is terminated
        revokeLeadership();

        Set<String> intersection = new HashSet<>(registeredMetrics);
        intersection.retainAll(expectedMetrics);
        Assert.assertTrue("Expected RM to unregister leader metrics", intersection.isEmpty());

        leaderElection.isLeader(UUID.randomUUID()).join();

        Assert.assertTrue(
                "Expected RM to re-register leader metrics",
                registeredMetrics.containsAll(expectedMetrics));
    }

    private static void blockOnFuture(CompletableFuture<?> future) {
        try {
            future.get();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    private static void assertNotComplete(CompletableFuture<?> future) throws Exception {
        try {
            future.get(50, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
    }

    private void revokeLeadership() {
        ResourceManager<?> leaderResourceManager =
                resourceManagerService.getLeaderResourceManager();
        leaderElection.notLeader();
        blockOnFuture(leaderResourceManager.getTerminationFuture());
    }
}
