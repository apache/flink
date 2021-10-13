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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.curator4.com.google.common.collect.Iterables;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ZooKeeperMultipleComponentLeaderElectionDriver}. */
@ExtendWith(TestLoggerExtension.class)
class ZooKeeperMultipleComponentLeaderElectionDriverTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> eachWrapper =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @Test
    public void testElectionDriverGainsLeadershipAtStartup() throws Exception {
        new Context() {
            {
                runTest(() -> leaderElectionListener.await(IsLeaderEvent.class));
            }
        };
    }

    @Test
    public void testElectionDriverLosesLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(IsLeaderEvent.class);
                            zooKeeperExtension.stop();
                            leaderElectionListener.await(NotLeaderEvent.class);
                        });
            }
        };
    }

    @Test
    public void testPublishLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(IsLeaderEvent.class);

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

                            leaderRetrievalListener.waitForNewLeader(10_000L);

                            assertThat(leaderRetrievalListener.getLeader())
                                    .isEqualTo(leaderInformation);
                        });
            }
        };
    }

    @Test
    public void testPublishEmptyLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(IsLeaderEvent.class);

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

                            leaderRetrievalListener.waitForNewLeader(10_000L);

                            leaderElectionDriver.publishLeaderInformation(
                                    componentId, LeaderInformation.empty());

                            leaderRetrievalListener.waitForEmptyLeaderInformation(10_000L);

                            assertThat(leaderRetrievalListener.getLeader())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    public void testNonLeaderCannotPublishLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            ElectionDriver otherLeaderElectionDriver = null;
                            try {
                                leaderElectionListener.await(IsLeaderEvent.class);

                                otherLeaderElectionDriver =
                                        createLeaderElectionDriver(
                                                curatorFramework.asCuratorFramework());

                                assertThat(otherLeaderElectionDriver.hasLeadership()).isFalse();

                                otherLeaderElectionDriver.publishLeaderInformation(
                                        "componentId",
                                        LeaderInformation.known(UUID.randomUUID(), "localhost"));

                                assertThat(
                                                leaderElectionListener.await(
                                                        LeaderInformationChangeEvent.class,
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
    public void testLeaderInformationChange() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(IsLeaderEvent.class);

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

                            final LeaderInformationChangeEvent leaderInformationChangeEvent =
                                    leaderElectionListener.await(
                                            LeaderInformationChangeEvent.class);

                            assertThat(leaderInformationChangeEvent.getComponentId())
                                    .isEqualTo(componentId);
                            assertThat(leaderInformationChangeEvent.getLeaderInformation())
                                    .isEqualTo(leaderInformation);
                        });
            }
        };
    }

    @Test
    public void testLeaderElectionWithMultipleDrivers() throws Exception {
        final CuratorFrameworkWithUnhandledErrorListener curatorFramework = startCuratorFramework();

        try {
            Set<ElectionDriver> electionDrivers =
                    Stream.generate(
                                    () ->
                                            createLeaderElectionDriver(
                                                    curatorFramework.asCuratorFramework()))
                            .limit(3)
                            .collect(Collectors.toSet());

            while (!electionDrivers.isEmpty()) {
                final CompletableFuture<Object> anyLeader =
                        CompletableFuture.anyOf(
                                electionDrivers.stream()
                                        .map(ElectionDriver::getLeadershipFuture)
                                        .collect(Collectors.toList())
                                        .toArray(new CompletableFuture[0]));

                // wait for any leader
                anyLeader.join();

                final Map<Boolean, Set<ElectionDriver>> leaderAndRest =
                        electionDrivers.stream()
                                .collect(
                                        Collectors.partitioningBy(
                                                ElectionDriver::hasLeadership, Collectors.toSet()));

                assertThat(leaderAndRest.get(true)).hasSize(1);
                Iterables.getOnlyElement(leaderAndRest.get(true)).close();

                electionDrivers = leaderAndRest.get(false);
            }
        } finally {
            curatorFramework.close();
        }
    }

    @Test
    public void testLeaderConnectionInfoNodeRemovalLeadsToLeaderChangeWithEmptyLeaderInformation()
            throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionListener.await(IsLeaderEvent.class);

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
                            leaderElectionListener.await(LeaderInformationChangeEvent.class);

                            curatorFramework.asCuratorFramework().delete().forPath(path);

                            // wait for the removal of the leader information
                            final LeaderInformationChangeEvent leaderInformationChangeEvent =
                                    leaderElectionListener.await(
                                            LeaderInformationChangeEvent.class);
                            assertThat(leaderInformationChangeEvent.getComponentId())
                                    .isEqualTo(componentId);
                            assertThat(leaderInformationChangeEvent.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    private static ElectionDriver createLeaderElectionDriver(CuratorFramework curatorFramework) {
        final SimpleLeaderElectionListener leaderElectionListener =
                new SimpleLeaderElectionListener();

        try {
            final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver =
                    createLeaderElectionDriver(leaderElectionListener, curatorFramework);
            return new ElectionDriver(leaderElectionDriver, leaderElectionListener);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }

    private static final class ElectionDriver {
        private final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver;
        private final SimpleLeaderElectionListener leaderElectionListener;

        private ElectionDriver(
                ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver,
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
            implements MultipleComponentLeaderElectionDriver.Listener {

        private final CompletableFuture<Void> leadershipFuture = new CompletableFuture<>();

        CompletableFuture<Void> getLeadershipFuture() {
            return leadershipFuture;
        }

        @Override
        public void isLeader() {
            leadershipFuture.complete(null);
        }

        @Override
        public void notLeader() {}

        @Override
        public void notifyLeaderInformationChange(
                String componentId, LeaderInformation leaderInformation) {}

        @Override
        public void notifyAllKnownLeaderInformation(
                Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {}
    }

    private static ZooKeeperMultipleComponentLeaderElectionDriver createLeaderElectionDriver(
            MultipleComponentLeaderElectionDriver.Listener leaderElectionListener,
            CuratorFramework curatorFramework)
            throws Exception {
        return new ZooKeeperMultipleComponentLeaderElectionDriver(
                curatorFramework, leaderElectionListener);
    }

    private CuratorFrameworkWithUnhandledErrorListener startCuratorFramework() {
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        return ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
    }

    private static final class TestingLeaderElectionListener
            implements MultipleComponentLeaderElectionDriver.Listener {
        private final BlockingQueue<LeaderElectionEvent> leaderElectionEvents =
                new ArrayBlockingQueue<>(10);

        @Override
        public void isLeader() {
            put(new IsLeaderEvent());
        }

        @Override
        public void notLeader() {
            put(new NotLeaderEvent());
        }

        @Override
        public void notifyLeaderInformationChange(
                String componentId, LeaderInformation leaderInformation) {
            put(new LeaderInformationChangeEvent(componentId, leaderInformation));
        }

        @Override
        public void notifyAllKnownLeaderInformation(
                Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
            put(new AllKnownLeaderInformationEvent(leaderInformationWithComponentIds));
        }

        private void put(LeaderElectionEvent leaderElectionEvent) {
            try {
                leaderElectionEvents.put(leaderElectionEvent);
            } catch (InterruptedException e) {
                ExceptionUtils.rethrow(e);
            }
        }

        public <T> T await(Class<T> clazz) throws InterruptedException {
            while (true) {
                final LeaderElectionEvent leaderElectionEvent = leaderElectionEvents.take();

                if (clazz.isAssignableFrom(leaderElectionEvent.getClass())) {
                    return clazz.cast(leaderElectionEvent);
                }
            }
        }

        public <T> Optional<T> await(Class<T> clazz, Duration timeout) throws InterruptedException {
            final Deadline deadline = Deadline.fromNow(timeout);

            while (true) {
                final Duration timeLeft = deadline.timeLeft();

                if (timeLeft.isNegative()) {
                    return Optional.empty();
                } else {
                    final Optional<LeaderElectionEvent> optLeaderElectionEvent =
                            Optional.ofNullable(
                                    leaderElectionEvents.poll(
                                            timeLeft.toMillis(), TimeUnit.MILLISECONDS));

                    if (optLeaderElectionEvent.isPresent()) {
                        final LeaderElectionEvent leaderElectionEvent =
                                optLeaderElectionEvent.get();

                        if (clazz.isAssignableFrom(leaderElectionEvent.getClass())) {
                            return Optional.of(clazz.cast(optLeaderElectionEvent));
                        }
                    } else {
                        return Optional.empty();
                    }
                }
            }
        }
    }

    private abstract static class LeaderElectionEvent {
        boolean isIsLeaderEvent() {
            return false;
        }

        boolean isNotLeaderEvent() {
            return false;
        }

        boolean isLeaderInformationChangeEvent() {
            return false;
        }

        boolean isAllKnownLeaderInformationEvent() {
            return false;
        }

        IsLeaderEvent asIsLeaderEvent() {
            return as(IsLeaderEvent.class);
        }

        NotLeaderEvent asNotLeaderEvent() {
            return as(NotLeaderEvent.class);
        }

        LeaderInformationChangeEvent asLeaderInformationChangeEvent() {
            return as(LeaderInformationChangeEvent.class);
        }

        AllKnownLeaderInformationEvent asAllKnownLeaderInformationEvent() {
            return as(AllKnownLeaderInformationEvent.class);
        }

        <T> T as(Class<T> clazz) {
            if (clazz.isAssignableFrom(getClass())) {
                return clazz.cast(this);
            } else {
                throw new IllegalStateException("Cannot cast object.");
            }
        }
    }

    private static class IsLeaderEvent extends LeaderElectionEvent {
        @Override
        boolean isIsLeaderEvent() {
            return true;
        }
    }

    private static class NotLeaderEvent extends LeaderElectionEvent {
        @Override
        boolean isNotLeaderEvent() {
            return true;
        }
    }

    private static class LeaderInformationChangeEvent extends LeaderElectionEvent {
        private final String componentId;
        private final LeaderInformation leaderInformation;

        private LeaderInformationChangeEvent(
                String componentId, LeaderInformation leaderInformation) {
            this.componentId = componentId;
            this.leaderInformation = leaderInformation;
        }

        @Override
        boolean isLeaderInformationChangeEvent() {
            return true;
        }

        public String getComponentId() {
            return componentId;
        }

        public LeaderInformation getLeaderInformation() {
            return leaderInformation;
        }
    }

    private static class AllKnownLeaderInformationEvent extends LeaderElectionEvent {
        private final Collection<LeaderInformationWithComponentId>
                leaderInformationWithComponentIds;

        private AllKnownLeaderInformationEvent(
                Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
            this.leaderInformationWithComponentIds = leaderInformationWithComponentIds;
        }

        @Override
        boolean isAllKnownLeaderInformationEvent() {
            return true;
        }
    }

    private class Context {
        protected final TestingLeaderElectionListener leaderElectionListener;
        protected final CuratorFrameworkWithUnhandledErrorListener curatorFramework;
        protected final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver;

        private Context() throws Exception {
            this.leaderElectionListener = new TestingLeaderElectionListener();
            this.curatorFramework = startCuratorFramework();
            this.leaderElectionDriver =
                    createLeaderElectionDriver(
                            leaderElectionListener, curatorFramework.asCuratorFramework());
        }

        protected final void runTest(RunnableWithException test) throws Exception {
            try {
                test.run();
            } finally {
                close();
            }
        }

        private void close() throws Exception {
            this.leaderElectionDriver.close();
            this.curatorFramework.close();
        }
    }
}
