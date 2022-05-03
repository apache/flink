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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link ResourceManagerServiceImpl}. */
public class ResourceManagerServiceImplTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10L);
    private static final Time FAST_TIMEOUT = Time.milliseconds(50L);

    private static final HeartbeatServices heartbeatServices = new TestingHeartbeatServices();
    private static final ClusterInformation clusterInformation =
            new ClusterInformation("localhost", 1234);
    private static final MetricRegistry metricRegistry = TestingMetricRegistry.builder().build();

    private static TestingRpcService rpcService;
    private static TestingHighAvailabilityServices haService;
    private static TestingFatalErrorHandler fatalErrorHandler;

    private TestingResourceManagerFactory.Builder rmFactoryBuilder;
    private TestingLeaderElectionService leaderElectionService;
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

        leaderElectionService = new TestingLeaderElectionService();
        haService.setResourceManagerLeaderElectionService(leaderElectionService);
    }

    @After
    public void teardown() throws Exception {
        if (resourceManagerService != null) {
            resourceManagerService.close();
        }

        if (leaderElectionService != null) {
            leaderElectionService.stop();
        }

        if (fatalErrorHandler.hasExceptionOccurred()) {
            fatalErrorHandler.rethrowError();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, TIMEOUT);
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
                        rpcService,
                        haService,
                        heartbeatServices,
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
        leaderElectionService.isLeader(leaderSessionId);

        // should start new RM and confirm leader session
        assertThat(startRmFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId));
        assertThat(
                leaderElectionService
                        .getConfirmationFuture()
                        .get(TIMEOUT.getSize(), TIMEOUT.getUnit())
                        .getLeaderSessionId(),
                is(leaderSessionId));
    }

    @Test
    public void grantLeadership_confirmLeaderSessionAfterRmStarted() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<Void> finishRmInitializationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(
                (ignore) -> blockOnFuture(finishRmInitializationFuture));

        createAndStartResourceManager();

        // grant leadership
        leaderElectionService.isLeader(leaderSessionId);

        // RM initialization not finished, should not confirm leader session
        assertNotComplete(leaderElectionService.getConfirmationFuture());

        // finish RM initialization
        finishRmInitializationFuture.complete(null);

        // should confirm leader session
        assertThat(
                leaderElectionService
                        .getConfirmationFuture()
                        .get(TIMEOUT.getSize(), TIMEOUT.getUnit())
                        .getLeaderSessionId(),
                is(leaderSessionId));
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
        leaderElectionService.isLeader(leaderSessionId1);

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // second time grant leadership
        leaderElectionService.isLeader(leaderSessionId2);

        // should terminate first RM, start a new RM and confirm leader session
        assertThat(
                terminateRmFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId1));
        assertThat(startRmFuture2.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId2));
        assertThat(
                leaderElectionService
                        .getConfirmationFuture()
                        .get(TIMEOUT.getSize(), TIMEOUT.getUnit())
                        .getLeaderSessionId(),
                is(leaderSessionId2));
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
        leaderElectionService.isLeader(leaderSessionId1);

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // second time grant leadership
        leaderElectionService.isLeader(leaderSessionId2);

        // first RM termination not finished, should not start new RM
        assertNotComplete(startRmFuture2);

        // finish first RM termination
        finishRmTerminationFuture.complete(null);

        // should start new RM and confirm leader session
        assertThat(startRmFuture2.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId2));
        assertThat(
                leaderElectionService
                        .getConfirmationFuture()
                        .get(TIMEOUT.getSize(), TIMEOUT.getUnit())
                        .getLeaderSessionId(),
                is(leaderSessionId2));
    }

    @Test
    public void grantLeadership_notStarted_doesNotStartNewRm() throws Exception {
        final CompletableFuture<UUID> startRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(startRmFuture::complete);

        createResourceManager();

        // grant leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // service not started, should not start new RM
        assertNotComplete(startRmFuture);
        assertNotComplete(leaderElectionService.getConfirmationFuture());
    }

    @Test
    public void grantLeadership_stopped_doesNotStartNewRm() throws Exception {
        final CompletableFuture<UUID> startRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setInitializeConsumer(startRmFuture::complete);

        createAndStartResourceManager();
        resourceManagerService.close();

        // grant leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // service stopped, should not start new RM
        assertNotComplete(startRmFuture);
        assertNotComplete(leaderElectionService.getConfirmationFuture());
    }

    @Test
    public void revokeLeadership_stopExistLeader() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<UUID> terminateRmFuture = new CompletableFuture<>();

        rmFactoryBuilder.setTerminateConsumer(terminateRmFuture::complete);

        createAndStartResourceManager();

        // grant leadership
        leaderElectionService.isLeader(leaderSessionId);

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // revoke leadership
        leaderElectionService.notLeader();

        // should terminate RM
        assertThat(
                terminateRmFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId));
    }

    @Test
    public void revokeLeadership_terminateService_multiLeaderSessionNotSupported()
            throws Exception {
        rmFactoryBuilder.setSupportMultiLeaderSession(false);

        createAndStartResourceManager();

        // grant leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // revoke leadership
        leaderElectionService.notLeader();

        // should terminate service
        resourceManagerService.getTerminationFuture().get(TIMEOUT.getSize(), TIMEOUT.getUnit());
    }

    @Test
    public void leaderRmTerminated_terminateService() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<Void> rmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setGetTerminationFutureFunction((ignore1, ignore2) -> rmTerminationFuture);

        createAndStartResourceManager();

        // grant leadership
        leaderElectionService.isLeader(leaderSessionId);

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // terminate RM
        rmTerminationFuture.complete(null);

        // should terminate service
        resourceManagerService.getTerminationFuture().get(TIMEOUT.getSize(), TIMEOUT.getUnit());
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
        leaderElectionService.isLeader(leaderSessionId);

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // revoke leadership
        leaderElectionService.notLeader();
        assertThat(
                terminateRmFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit()), is(leaderSessionId));

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
        leaderElectionService.isLeader(UUID.randomUUID());

        // make sure RM started, before proceeding the next step
        assertRmStarted();
        assertFalse(leaderElectionService.isStopped());

        // close service
        resourceManagerService.close();

        // should stop RM and leader election
        assertTrue(terminateRmFuture.isDone());
        assertTrue(leaderElectionService.isStopped());
    }

    @Test
    public void closeService_futureCompleteAfterRmTerminated() throws Exception {
        final CompletableFuture<Void> finishRmTerminationFuture = new CompletableFuture<>();

        rmFactoryBuilder.setTerminateConsumer((ignore) -> blockOnFuture(finishRmTerminationFuture));

        createAndStartResourceManager();

        // grant leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // make sure RM started, before proceeding the next step
        assertRmStarted();

        // close service
        final CompletableFuture<Void> closeServiceFuture = resourceManagerService.closeAsync();

        // RM termination not finished, future should not complete
        assertNotComplete(closeServiceFuture);

        // finish RM termination
        finishRmTerminationFuture.complete(null);

        closeServiceFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
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
        leaderElectionService.isLeader(UUID.randomUUID());

        // make sure leader RM is created
        startRmInitializationFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());

        // deregister application
        final CompletableFuture<Void> deregisterApplicationFuture =
                resourceManagerService.deregisterApplication(ApplicationStatus.CANCELED, null);

        // RM not fully started, future should not complete
        assertNotComplete(deregisterApplicationFuture);

        // finish starting RM
        finishRmInitializationFuture.complete(null);

        // should perform deregistration
        deregisterApplicationFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
    }

    @Test
    public void deregisterApplication_noLeaderRm() throws Exception {
        createAndStartResourceManager();
        final CompletableFuture<Void> deregisterApplicationFuture =
                resourceManagerService.deregisterApplication(ApplicationStatus.CANCELED, null);

        // should not report error
        deregisterApplicationFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
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
            future.get(FAST_TIMEOUT.getSize(), FAST_TIMEOUT.getUnit());
            fail();
        } catch (TimeoutException e) {
            // expected
        }
    }

    private void assertRmStarted() throws Exception {
        leaderElectionService.getConfirmationFuture().get(TIMEOUT.getSize(), TIMEOUT.getUnit());
    }
}
