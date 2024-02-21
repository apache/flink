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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.function.BiConsumerWithException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test behaviors of {@link ZooKeeperLeaderElectionDriver} when losing the connection to ZooKeeper.
 */
class ZooKeeperLeaderElectionConnectionHandlingTest {

    @RegisterExtension
    private static final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private final Configuration configuration = new Configuration();

    @BeforeEach
    void setup() {
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperResource.getCustomExtension().getConnectString());
    }

    @Test
    void testLoseLeadershipOnConnectionSuspended() throws Exception {
        runTestWithBrieflySuspendedZooKeeperConnection(
                configuration,
                (connectionStateListener, contender) -> {
                    connectionStateListener.awaitSuspendedConnection();
                    contender.awaitRevokeLeadership();
                });
    }

    @Test
    void testKeepLeadershipOnSuspendedConnectionIfTolerateSuspendedConnectionsIsEnabled()
            throws Exception {
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS, true);
        runTestWithBrieflySuspendedZooKeeperConnection(
                configuration,
                (connectionStateListener, contender) -> {
                    connectionStateListener.awaitSuspendedConnection();
                    connectionStateListener.awaitReconnectedConnection();
                    assertThat(contender.hasRevokeLeadershipBeenTriggered()).isFalse();
                });
    }

    @Test
    void testLoseLeadershipOnLostConnectionIfTolerateSuspendedConnectionsIsEnabled()
            throws Exception {
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 1000);
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS, true);
        runTestWithLostZooKeeperConnection(
                configuration,
                (connectionStateListener, contender) -> {
                    connectionStateListener.awaitLostConnection();
                    contender.awaitRevokeLeadership();
                });
    }

    private void runTestWithLostZooKeeperConnection(
            Configuration configuration,
            BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
                    validationLogic)
            throws Exception {
        runTestWithZooKeeperConnectionProblem(
                configuration, validationLogic, Problem.LOST_CONNECTION);
    }

    private void runTestWithBrieflySuspendedZooKeeperConnection(
            Configuration configuration,
            BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
                    validationLogic)
            throws Exception {
        runTestWithZooKeeperConnectionProblem(
                configuration, validationLogic, Problem.SUSPENDED_CONNECTION);
    }

    private enum Problem {
        LOST_CONNECTION,
        SUSPENDED_CONNECTION
    }

    private void runTestWithZooKeeperConnectionProblem(
            Configuration configuration,
            BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
                    validationLogic,
            Problem problem)
            throws Exception {
        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration,
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
        CuratorFramework client = curatorFrameworkWrapper.asCuratorFramework();
        LeaderElectionDriverFactory leaderElectionDriverFactory =
                new ZooKeeperLeaderElectionDriverFactory(client);
        DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        leaderElectionDriverFactory,
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler());

        final TestingConnectionStateListener connectionStateListener =
                new TestingConnectionStateListener();
        client.getConnectionStateListenable().addListener(connectionStateListener);

        final TestingContender contender = new TestingContender();
        try (LeaderElection leaderElection =
                leaderElectionService.createLeaderElection("random-component-id")) {
            leaderElection.startLeaderElection(contender);

            contender.awaitGrantLeadership();

            switch (problem) {
                case SUSPENDED_CONNECTION:
                    zooKeeperResource.getCustomExtension().restart();
                    break;
                case LOST_CONNECTION:
                    zooKeeperResource.getCustomExtension().stop();
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown problem type %s.", problem));
            }

            validationLogic.accept(connectionStateListener, contender);
        } finally {
            leaderElectionService.close();
            curatorFrameworkWrapper.close();

            if (problem == Problem.LOST_CONNECTION) {
                // in case of lost connections we accept that some unhandled error can occur
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler().clearError();
            }
        }
    }

    private final class TestingContender implements LeaderContender {

        private final OneShotLatch grantLeadershipLatch;
        private final OneShotLatch revokeLeadershipLatch;

        private TestingContender() {
            this.grantLeadershipLatch = new OneShotLatch();
            this.revokeLeadershipLatch = new OneShotLatch();
        }

        @Override
        public void grantLeadership(UUID leaderSessionID) {
            grantLeadershipLatch.trigger();
        }

        @Override
        public void revokeLeadership() {
            revokeLeadershipLatch.trigger();
        }

        @Override
        public void handleError(Exception exception) {
            ZooKeeperLeaderElectionConnectionHandlingTest.this
                    .testingFatalErrorHandlerResource
                    .getTestingFatalErrorHandler()
                    .onFatalError(exception);
        }

        public void awaitGrantLeadership() throws InterruptedException {
            grantLeadershipLatch.await();
        }

        public boolean hasRevokeLeadershipBeenTriggered() {
            return revokeLeadershipLatch.isTriggered();
        }

        public void awaitRevokeLeadership() throws InterruptedException {
            revokeLeadershipLatch.await();
        }
    }

    private static final class TestingConnectionStateListener implements ConnectionStateListener {
        private final OneShotLatch connectionSuspendedLatch;
        private final OneShotLatch reconnectedLatch;
        private final OneShotLatch connectionLostLatch;

        public TestingConnectionStateListener() {
            this.connectionSuspendedLatch = new OneShotLatch();
            this.reconnectedLatch = new OneShotLatch();
            this.connectionLostLatch = new OneShotLatch();
        }

        @Override
        public void stateChanged(
                CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState == ConnectionState.SUSPENDED) {
                connectionSuspendedLatch.trigger();
            }

            if (connectionState == ConnectionState.RECONNECTED) {
                reconnectedLatch.trigger();
            }

            if (connectionState == ConnectionState.LOST) {
                connectionLostLatch.trigger();
            }
        }

        public void awaitSuspendedConnection() throws InterruptedException {
            connectionSuspendedLatch.await();
        }

        public void awaitReconnectedConnection() throws InterruptedException {
            reconnectedLatch.await();
        }

        public void awaitLostConnection() throws InterruptedException {
            connectionLostLatch.await();
        }
    }
}
