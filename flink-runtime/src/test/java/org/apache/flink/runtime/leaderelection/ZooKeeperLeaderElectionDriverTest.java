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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ZooKeeperLeaderElectionDriver}. */
class ZooKeeperLeaderElectionDriverTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> eachWrapper =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    @Test
    void testElectionDriverGainsLeadershipAtStartup() throws Exception {
        new Context() {
            {
                runTest(
                        () ->
                                leaderElectionListener.await(
                                        LeaderElectionEvent.IsLeaderEvent.class));
            }
        };
    }

    @Test
    void testElectionDriverLosesLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);
                            zooKeeperExtension.stop();
                            leaderElectionListener.await(LeaderElectionEvent.NotLeaderEvent.class);
                        });
            }
        };
    }

    @Test
    void testPublishLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);

                            final String componentId = "retrieved-component";
                            final DefaultLeaderRetrievalService defaultLeaderRetrievalService =
                                    new DefaultLeaderRetrievalService(
                                            new ZooKeeperLeaderRetrievalDriverFactory(
                                                    curatorFramework.asCuratorFramework(),
                                                    componentId,
                                                    ZooKeeperLeaderRetrievalDriver
                                                            .LeaderInformationClearancePolicy
                                                            .ON_LOST_CONNECTION));

                            final TestingListener leaderRetrievalListener = new TestingListener();
                            defaultLeaderRetrievalService.start(leaderRetrievalListener);

                            final LeaderInformation leaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "foobar");
                            leaderElectionDriver.publishLeaderInformation(
                                    componentId, leaderInformation);

                            leaderRetrievalListener.waitForNewLeader();

                            assertThat(leaderRetrievalListener.getLeader())
                                    .isEqualTo(leaderInformation);
                        });
            }
        };
    }

    @Test
    void testPublishEmptyLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);

                            final String componentId = "retrieved-component";
                            final DefaultLeaderRetrievalService defaultLeaderRetrievalService =
                                    new DefaultLeaderRetrievalService(
                                            new ZooKeeperLeaderRetrievalDriverFactory(
                                                    curatorFramework.asCuratorFramework(),
                                                    componentId,
                                                    ZooKeeperLeaderRetrievalDriver
                                                            .LeaderInformationClearancePolicy
                                                            .ON_LOST_CONNECTION));

                            final TestingListener leaderRetrievalListener = new TestingListener();
                            defaultLeaderRetrievalService.start(leaderRetrievalListener);

                            leaderElectionDriver.publishLeaderInformation(
                                    componentId,
                                    LeaderInformation.known(UUID.randomUUID(), "foobar"));

                            leaderRetrievalListener.waitForNewLeader();

                            leaderElectionDriver.publishLeaderInformation(
                                    componentId, LeaderInformation.empty());

                            leaderRetrievalListener.waitForEmptyLeaderInformation();

                            assertThat(leaderRetrievalListener.getLeader())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testToStringContainingLeaderLatchPath() throws Exception {
        new Context() {
            {
                runTest(
                        () ->
                                assertThat(leaderElectionDriver.toString())
                                        .as(
                                                "toString() should contain the leader latch path for human-readable representation of the driver instance.")
                                        .contains(
                                                ZooKeeperUtils.generateLeaderLatchPath(
                                                        curatorFramework
                                                                .asCuratorFramework()
                                                                .getNamespace())));
            }
        };
    }

    @Test
    void testNonLeaderCannotPublishLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            ElectionDriver otherLeaderElectionDriver = null;
                            try {
                                leaderElectionListener.await(
                                        LeaderElectionEvent.IsLeaderEvent.class);

                                otherLeaderElectionDriver =
                                        createLeaderElectionDriver(
                                                curatorFramework.asCuratorFramework(),
                                                testingFatalErrorHandlerResource
                                                        .getTestingFatalErrorHandler());

                                assertThat(otherLeaderElectionDriver.hasLeadership()).isFalse();

                                otherLeaderElectionDriver.publishLeaderInformation(
                                        "component-id",
                                        LeaderInformation.known(UUID.randomUUID(), "localhost"));

                                assertThat(
                                                leaderElectionListener.await(
                                                        LeaderElectionEvent
                                                                .LeaderInformationChangeEvent.class,
                                                        Duration.ofMillis(50L)))
                                        .isEmpty();
                            } finally {
                                if (otherLeaderElectionDriver != null) {
                                    otherLeaderElectionDriver.close();
                                }
                            }
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChange() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);

                            final LeaderInformation leaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "foobar");
                            final String componentId = "componentId";
                            final String path =
                                    ZooKeeperUtils.generateConnectionInformationPath(componentId);

                            ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                                    leaderInformation,
                                    curatorFramework.asCuratorFramework(),
                                    () -> true,
                                    path);

                            final LeaderElectionEvent.LeaderInformationChangeEvent
                                    leaderInformationChangeEvent =
                                            leaderElectionListener.await(
                                                    LeaderElectionEvent.LeaderInformationChangeEvent
                                                            .class);

                            assertThat(leaderInformationChangeEvent.getComponentId())
                                    .isEqualTo(componentId);
                            assertThat(leaderInformationChangeEvent.getLeaderInformation())
                                    .isEqualTo(leaderInformation);
                        });
            }
        };
    }

    @Test
    void testLeaderElectionWithMultipleDrivers() throws Exception {
        final CuratorFrameworkWithUnhandledErrorListener curatorFramework = startCuratorFramework();

        try {
            Set<ElectionDriver> electionDrivers =
                    Stream.generate(
                                    () ->
                                            createLeaderElectionDriver(
                                                    curatorFramework.asCuratorFramework(),
                                                    testingFatalErrorHandlerResource
                                                            .getTestingFatalErrorHandler()))
                            .limit(3)
                            .collect(Collectors.toSet());

            while (!electionDrivers.isEmpty()) {
                final CompletableFuture<Object> anyLeader =
                        CompletableFuture.anyOf(
                                electionDrivers.stream()
                                        .map(ElectionDriver::getLeadershipFuture)
                                        .toArray(CompletableFuture[]::new));

                // wait for any leader
                anyLeader.join();

                final Map<Boolean, Set<ElectionDriver>> leaderAndRest =
                        electionDrivers.stream()
                                .collect(
                                        Collectors.partitioningBy(
                                                ElectionDriver::hasLeadership, Collectors.toSet()));

                assertThat(leaderAndRest.get(true)).hasSize(1);

                // TODO: remove this line after CURATOR-645 is resolved
                // CURATOR-645 covers a bug in the LeaderLatch implementation that causes a race
                // condition if a child node, participating in the leader election, is removed too
                // fast. This results in a different code branch being executed which triggers a
                // reset of the LeaderLatch instead of re-collecting the children to determine the
                // next leader.
                // The issue occurs because LeaderLatch#checkLeadership is not executed
                // transactionally, i.e. retrieving the children and setting up the watcher for the
                // predecessor is not done atomically. This leads to the race condition where a
                // children (the previous leader's node) is removed before setting up the watcher
                // which results in an invalid handling of the situation using reset.
                // Adding some sleep here (simulating the leader actually doing something) will
                // reduce the risk of falling into the race condition because it will give the
                // concurrently running LeaderLatch instances more time to set up the watchers
                // properly.
                Thread.sleep(100);

                Iterables.getOnlyElement(leaderAndRest.get(true)).close();

                electionDrivers = leaderAndRest.get(false);
            }
        } finally {
            curatorFramework.close();
        }
    }

    @Test
    void testLeaderInformationNodeRemovalLeadsToLeaderChangeWithEmptyLeaderInformation()
            throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);

                            final LeaderInformation leaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "foobar");
                            final String componentId = "componentId";
                            final String path =
                                    ZooKeeperUtils.generateConnectionInformationPath(componentId);

                            ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                                    leaderInformation,
                                    curatorFramework.asCuratorFramework(),
                                    () -> true,
                                    path);

                            // wait for the publishing of the leader information
                            leaderElectionListener.await(
                                    LeaderElectionEvent.LeaderInformationChangeEvent.class);

                            curatorFramework.asCuratorFramework().delete().forPath(path);

                            // wait for the removal of the leader information
                            final LeaderElectionEvent.LeaderInformationChangeEvent
                                    leaderInformationChangeEvent =
                                            leaderElectionListener.await(
                                                    LeaderElectionEvent.LeaderInformationChangeEvent
                                                            .class);
                            assertThat(leaderInformationChangeEvent.getComponentId())
                                    .isEqualTo(componentId);
                            assertThat(leaderInformationChangeEvent.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    private static ElectionDriver createLeaderElectionDriver(
            CuratorFramework curatorFramework, FatalErrorHandler fatalErrorHandler) {
        final SimpleLeaderElectionListener leaderElectionListener =
                new SimpleLeaderElectionListener(fatalErrorHandler);

        try {
            final ZooKeeperLeaderElectionDriver leaderElectionDriver =
                    new ZooKeeperLeaderElectionDriver(curatorFramework, leaderElectionListener);
            return new ElectionDriver(leaderElectionDriver, leaderElectionListener);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }

    private static final class ElectionDriver {
        private final ZooKeeperLeaderElectionDriver leaderElectionDriver;
        private final SimpleLeaderElectionListener leaderElectionListener;

        private ElectionDriver(
                ZooKeeperLeaderElectionDriver leaderElectionDriver,
                SimpleLeaderElectionListener leaderElectionListener) {
            this.leaderElectionDriver = leaderElectionDriver;
            this.leaderElectionListener = leaderElectionListener;
        }

        void close() throws Exception {
            leaderElectionDriver.close();
        }

        boolean hasLeadership() {
            return leaderElectionDriver.hasLeadership();
        }

        CompletableFuture<Void> getLeadershipFuture() {
            return leaderElectionListener.getLeadershipFuture();
        }

        void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
                throws Exception {
            leaderElectionDriver.publishLeaderInformation(componentId, leaderInformation);
        }
    }

    private static final class SimpleLeaderElectionListener
            implements LeaderElectionDriver.Listener {

        private final CompletableFuture<Void> leadershipFuture = new CompletableFuture<>();
        private final FatalErrorHandler fatalErrorHandler;

        public SimpleLeaderElectionListener(FatalErrorHandler fatalErrorHandler) {
            this.fatalErrorHandler = fatalErrorHandler;
        }

        CompletableFuture<Void> getLeadershipFuture() {
            return leadershipFuture;
        }

        @Override
        public void onGrantLeadership(UUID ignoredSessionID) {
            leadershipFuture.complete(null);
        }

        @Override
        public void onRevokeLeadership() {}

        @Override
        public void onLeaderInformationChange(
                String componentId, LeaderInformation leaderInformation) {}

        @Override
        public void onLeaderInformationChange(
                LeaderInformationRegister leaderInformationRegister) {}

        @Override
        public void onError(Throwable t) {
            fatalErrorHandler.onFatalError(t);
        }
    }

    private CuratorFrameworkWithUnhandledErrorListener startCuratorFramework() {
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        return ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
    }

    private class Context {
        protected final TestingLeaderElectionListener leaderElectionListener;
        protected final CuratorFrameworkWithUnhandledErrorListener curatorFramework;
        protected final ZooKeeperLeaderElectionDriver leaderElectionDriver;

        private Context() throws Exception {
            this.leaderElectionListener = new TestingLeaderElectionListener();
            this.curatorFramework = startCuratorFramework();
            this.leaderElectionDriver =
                    new ZooKeeperLeaderElectionDriver(
                            curatorFramework.asCuratorFramework(), leaderElectionListener);
        }

        protected final void runTest(RunnableWithException test) throws Exception {
            try {
                test.run();
            } finally {
                close();

                leaderElectionListener.failIfErrorEventHappened();
            }
        }

        private void close() throws Exception {
            this.leaderElectionDriver.close();
            this.curatorFramework.close();
        }
    }
}
