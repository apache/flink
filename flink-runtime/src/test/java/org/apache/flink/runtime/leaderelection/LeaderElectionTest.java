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
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedLeaderService;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for leader election. */
@ExtendWith(ParameterizedTestExtension.class)
public class LeaderElectionTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    @Parameters(name = "Leader election: {0}")
    public static Collection<ServiceClass> parameters() {
        return Arrays.asList(
                new ZooKeeperServiceClass(),
                new EmbeddedServiceClass(),
                new StandaloneServiceClass());
    }

    @Parameter public ServiceClass serviceClass;

    @BeforeEach
    void setup() throws Exception {
        serviceClass.setup(testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    @AfterEach
    void teardown() throws Exception {
        serviceClass.teardown();
    }

    @TestTemplate
    void testHasLeadership() throws Exception {
        final ManualLeaderContender manualLeaderContender = new ManualLeaderContender();

        try {
            final LeaderElection leaderElection = serviceClass.createLeaderElection();
            leaderElection.startLeaderElection(manualLeaderContender);

            final UUID leaderSessionId = manualLeaderContender.waitForLeaderSessionId();

            assertThat(leaderElection.hasLeadership(leaderSessionId)).isTrue();
            assertThat(leaderElection.hasLeadership(UUID.randomUUID())).isFalse();

            leaderElection.confirmLeadership(leaderSessionId, "foobar");

            assertThat(leaderElection.hasLeadership(leaderSessionId)).isTrue();

            leaderElection.close();

            assertThat(leaderElection.hasLeadership(leaderSessionId)).isFalse();

            assertThat(manualLeaderContender.waitForLeaderSessionId())
                    .as("The leadership has been revoked from the contender.")
                    .isEqualTo(ManualLeaderContender.NULL_LEADER_SESSION_ID);
        } finally {
            manualLeaderContender.rethrowError();
        }
    }

    private static final class ManualLeaderContender implements LeaderContender {

        private static final UUID NULL_LEADER_SESSION_ID = new UUID(0L, 0L);

        private final ArrayBlockingQueue<UUID> leaderSessionIds = new ArrayBlockingQueue<>(10);

        private volatile Exception exception;

        @Override
        public void grantLeadership(UUID leaderSessionID) {
            leaderSessionIds.offer(leaderSessionID);
        }

        @Override
        public void revokeLeadership() {
            leaderSessionIds.offer(NULL_LEADER_SESSION_ID);
        }

        @Override
        public void handleError(Exception exception) {
            this.exception = exception;
        }

        void rethrowError() throws Exception {
            if (exception != null) {
                throw exception;
            }
        }

        UUID waitForLeaderSessionId() throws InterruptedException {
            return leaderSessionIds.take();
        }
    }

    private interface ServiceClass {
        void setup(FatalErrorHandler fatalErrorHandler) throws Exception;

        void teardown() throws Exception;

        LeaderElection createLeaderElection() throws Exception;
    }

    private static final class ZooKeeperServiceClass implements ServiceClass {

        private TestingServer testingServer;

        private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

        private DefaultLeaderElectionService leaderElectionService;

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) throws Exception {
            try {
                testingServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
            } catch (Exception e) {
                throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
            }

            final Configuration configuration = new Configuration();

            configuration.setString(
                    HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
            configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

            curatorFrameworkWrapper =
                    ZooKeeperUtils.startCuratorFramework(configuration, fatalErrorHandler);

            final LeaderElectionDriverFactory driverFactory =
                    new ZooKeeperLeaderElectionDriverFactory(
                            curatorFrameworkWrapper.asCuratorFramework());
            leaderElectionService = new DefaultLeaderElectionService(driverFactory);
        }

        @Override
        public void teardown() throws Exception {
            if (leaderElectionService != null) {
                leaderElectionService.close();
            }

            if (curatorFrameworkWrapper != null) {
                curatorFrameworkWrapper.close();
                curatorFrameworkWrapper = null;
            }

            if (testingServer != null) {
                testingServer.close();
                testingServer = null;
            }
        }

        @Override
        public LeaderElection createLeaderElection() {
            return leaderElectionService.createLeaderElection("random-component-id");
        }
    }

    private static final class EmbeddedServiceClass implements ServiceClass {
        private EmbeddedLeaderService embeddedLeaderService;

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) {
            embeddedLeaderService = new EmbeddedLeaderService(EXECUTOR_RESOURCE.getExecutor());
        }

        @Override
        public void teardown() {
            if (embeddedLeaderService != null) {
                embeddedLeaderService.shutdown();
                embeddedLeaderService = null;
            }
        }

        @Override
        public LeaderElection createLeaderElection() {
            return embeddedLeaderService.createLeaderElectionService("embedded_leader_election");
        }
    }

    private static final class StandaloneServiceClass implements ServiceClass {

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) {
            // noop
        }

        @Override
        public void teardown() {
            // noop
        }

        @Override
        public LeaderElection createLeaderElection() {
            return new StandaloneLeaderElection(UUID.randomUUID());
        }
    }
}
