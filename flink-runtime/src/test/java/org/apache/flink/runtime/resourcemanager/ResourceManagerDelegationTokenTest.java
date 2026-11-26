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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManagerTestHarness;
import org.apache.flink.runtime.security.token.MockTokenService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how the {@link ResourceManager} of a leader session takes part in the distribution of the
 * delegation tokens, which the process obtains and keeps obtaining between leader sessions.
 */
class ResourceManagerDelegationTokenTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static TestingRpcService rpcService;

    private final DelegationTokenManagerTestHarness tokens =
            new DelegationTokenManagerTestHarness(new MockTokenService());
    private final TestingLeaderElection leaderElection = new TestingLeaderElection();
    private final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

    private ResourceManagerServiceImpl resourceManagerService;

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardownClass() throws Exception {
        RpcUtils.terminateRpcService(rpcService);
    }

    @BeforeEach
    void setup() throws Exception {
        final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
        haServices.setResourceManagerLeaderElection(leaderElection);
        // The factory of the testing ResourceManager cannot accept registrations of TaskManagers.
        resourceManagerService =
                ResourceManagerServiceImpl.create(
                        StandaloneResourceManagerFactory.getInstance(),
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
        resourceManagerService.start();
    }

    @AfterEach
    void teardown() throws Exception {
        leaderElection.close();
        resourceManagerService.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (fatalErrorHandler.hasExceptionOccurred()) {
            fatalErrorHandler.rethrowError();
        }
    }

    @Test
    void taskExecutorRegisteringInTheLeaderSessionGetsTheTokensTheProcessObtained()
            throws Exception {
        tokens.manager().start();
        final ResourceManagerGateway resourceManager = becomeLeader();

        final RegistrationResponse response =
                register(resourceManager, taskExecutor(new CopyOnWriteArrayList<>()));

        assertThat(response).isInstanceOf(TaskExecutorRegistrationSuccess.class);
        final byte[] initialTokens =
                ((TaskExecutorRegistrationSuccess) response).getInitialTokens();
        assertThat(initialTokens).isNotNull();
        assertThat(MockTokenService.generationIn(initialTokens)).isEqualTo(1);
        assertThat(tokens.provider().obtainCalls()).isEqualTo(1);
    }

    @Test
    void obtainedTokensArePushedToTheRegisteredTaskExecutorWithTheFencingTokenOfTheLeader()
            throws Exception {
        tokens.manager().start();
        final ResourceManagerGateway resourceManager = becomeLeader();
        final List<TokenUpdate> updates = new CopyOnWriteArrayList<>();
        register(resourceManager, taskExecutor(updates));

        tokens.obtainIsDue();

        assertThat(updates).hasSize(1);
        assertThat(updates.get(0).fencingToken).isEqualTo(resourceManager.getFencingToken());
        assertThat(MockTokenService.generationIn(updates.get(0).tokens)).isEqualTo(2);
    }

    @Test
    void nextLeaderSessionServesTheTokensObtainedWhileNobodyWasLeader() throws Exception {
        tokens.manager().start();
        becomeLeader();
        loseLeadership();
        tokens.obtainIsDue();

        final ResourceManagerGateway resourceManager = becomeLeader();
        final RegistrationResponse response =
                register(resourceManager, taskExecutor(new CopyOnWriteArrayList<>()));

        final byte[] initialTokens =
                ((TaskExecutorRegistrationSuccess) response).getInitialTokens();
        assertThat(MockTokenService.generationIn(initialTokens)).isEqualTo(2);
        assertThat(tokens.provider().obtainCalls()).isEqualTo(2);
    }

    /** Grants leadership and returns the gateway of the ResourceManager of the new session. */
    private ResourceManagerGateway becomeLeader() throws Exception {
        leaderElection.isLeader(UUID.randomUUID()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return resourceManagerService
                .getLeaderResourceManager()
                .getSelfGateway(ResourceManagerGateway.class);
    }

    private void loseLeadership() throws Exception {
        final ResourceManager<?> resourceManager =
                resourceManagerService.getLeaderResourceManager();
        leaderElection.notLeader();
        resourceManager.getTerminationFuture().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** A TaskExecutor the ResourceManager can connect to, it records the token updates it gets. */
    private static TestingTaskExecutorGateway taskExecutor(List<TokenUpdate> updates) {
        final TestingTaskExecutorGateway taskExecutor =
                new TestingTaskExecutorGatewayBuilder()
                        // every gateway of the shared RPC service needs its own address
                        .setAddress("task-executor-" + UUID.randomUUID())
                        .setUpdateDelegationTokensFunction(
                                (fencingToken, tokens) -> {
                                    updates.add(new TokenUpdate(fencingToken, tokens));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutor.getAddress(), taskExecutor);
        return taskExecutor;
    }

    private static RegistrationResponse register(
            ResourceManagerGateway resourceManager, TestingTaskExecutorGateway taskExecutor)
            throws Exception {
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1234);
        return resourceManager
                .registerTaskExecutor(
                        new TaskExecutorRegistration(
                                taskExecutor.getAddress(),
                                ResourceID.generate(),
                                1234,
                                23456,
                                new HardwareDescription(1, 2L, 3L, 4L),
                                new TaskExecutorMemoryConfiguration(
                                        1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                                resourceProfile,
                                resourceProfile,
                                taskExecutor.getAddress(),
                                1),
                        TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static final class TokenUpdate {
        private final ResourceManagerId fencingToken;
        private final byte[] tokens;

        private TokenUpdate(ResourceManagerId fencingToken, byte[] tokens) {
            this.fencingToken = fencingToken;
            this.tokens = tokens;
        }
    }
}
