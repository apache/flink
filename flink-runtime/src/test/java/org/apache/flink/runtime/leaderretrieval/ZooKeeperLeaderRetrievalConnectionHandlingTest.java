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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the error handling in case of a suspended connection to the ZooKeeper instance when
 * retrieving the leader information.
 */
@ExtendWith(TestLoggerExtension.class)
class ZooKeeperLeaderRetrievalConnectionHandlingTest {

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension fatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> zooKeeperExtension =
            new EachCallbackWrapper<>(new ZooKeeperExtension());

    @Nullable private CuratorFramework zooKeeperClient;

    @BeforeEach
    public void before() throws Exception {
        zooKeeperClient =
                zooKeeperExtension
                        .getCustomExtension()
                        .getZooKeeperClient(
                                fatalErrorHandlerResource.getTestingFatalErrorHandler());
        zooKeeperClient.blockUntilConnected();
    }

    private ZooKeeperExtension getZooKeeper() {
        return zooKeeperExtension.getCustomExtension();
    }

    @Test
    public void testConnectionSuspendedHandlingDuringInitialization() throws Exception {
        testWithQueueLeaderElectionListener(
                queueLeaderElectionListener ->
                        ZooKeeperUtils.createLeaderRetrievalDriverFactory(zooKeeperClient)
                                .createLeaderRetrievalDriver(
                                        queueLeaderElectionListener,
                                        fatalErrorHandlerResource.getTestingFatalErrorHandler()),
                (leaderRetrievalDriver, queueLeaderElectionListener) -> {
                    // do the testing
                    assertThat(queueLeaderElectionListener.next(Duration.ofMillis(50)))
                            .as("No results are expected, yet, since no leader was elected.")
                            .isNotPresent();

                    getZooKeeper().restart();

                    // QueueLeaderElectionListener will be notified with an empty leader when ZK
                    // connection is suspended
                    final String secondAddress =
                            queueLeaderElectionListener.next().getLeaderAddress();
                    assertThat(secondAddress)
                            .as("The next result is expected to be null.")
                            .isNull();
                });
    }

    @Test
    public void testConnectionSuspendedHandling() throws Exception {
        testWithQueueLeaderElectionListener(
                queueLeaderElectionListener ->
                        new ZooKeeperLeaderRetrievalDriver(
                                zooKeeperClient,
                                "/testConnectionSuspendedHandling",
                                queueLeaderElectionListener,
                                ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                                        .ON_SUSPENDED_CONNECTION,
                                fatalErrorHandlerResource.getTestingFatalErrorHandler()),
                (leaderRetrievalDriver, queueLeaderElectionListener) -> {
                    final String leaderAddress = "localhost";
                    writeLeaderInformationToZooKeeper(
                            leaderRetrievalDriver.getConnectionInformationPath(),
                            leaderAddress,
                            UUID.randomUUID());

                    // do the testing
                    String firstAddress = queueLeaderElectionListener.next().getLeaderAddress();
                    assertThat(firstAddress)
                            .as(
                                    "The first result is expected to be the initially set leader address.")
                            .isEqualTo(leaderAddress);

                    getZooKeeper().restart();

                    String secondAddress = queueLeaderElectionListener.next().getLeaderAddress();
                    assertThat(secondAddress)
                            .as("The next result is expected to be null.")
                            .isNull();
                });
    }

    @Test
    public void testSuspendedConnectionDoesNotClearLeaderInformationIfClearanceOnLostConnection()
            throws Exception {
        testWithQueueLeaderElectionListener(
                queueLeaderElectionListener ->
                        new ZooKeeperLeaderRetrievalDriver(
                                zooKeeperClient,
                                "/testConnectionSuspendedHandling",
                                queueLeaderElectionListener,
                                ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                                        .ON_LOST_CONNECTION,
                                fatalErrorHandlerResource.getTestingFatalErrorHandler()),
                (leaderRetrievalDriver, queueLeaderElectionListener) -> {
                    final String leaderAddress = "localhost";

                    writeLeaderInformationToZooKeeper(
                            leaderRetrievalDriver.getConnectionInformationPath(),
                            leaderAddress,
                            UUID.randomUUID());

                    // do the testing
                    String firstAddress = queueLeaderElectionListener.next().getLeaderAddress();
                    assertThat(firstAddress)
                            .as(
                                    "The first result is expected to be the initially set leader address.")
                            .isEqualTo(leaderAddress);

                    getZooKeeper().close();

                    // make sure that no new leader information is published
                    assertThat(queueLeaderElectionListener.next(Duration.ofMillis(100L)))
                            .isNotPresent();
                });
    }

    @Test
    public void testSameLeaderAfterReconnectTriggersListenerNotification() throws Exception {
        testWithQueueLeaderElectionListener(
                queueLeaderElectionListener ->
                        new ZooKeeperLeaderRetrievalDriver(
                                zooKeeperClient,
                                "/testSameLeaderAfterReconnectTriggersListenerNotification",
                                queueLeaderElectionListener,
                                ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                                        .ON_SUSPENDED_CONNECTION,
                                fatalErrorHandlerResource.getTestingFatalErrorHandler()),
                (leaderRetrievalDriver, queueLeaderElectionListener) -> {
                    final String leaderAddress = "foobar";
                    final UUID sessionId = UUID.randomUUID();
                    writeLeaderInformationToZooKeeper(
                            leaderRetrievalDriver.getConnectionInformationPath(),
                            leaderAddress,
                            sessionId);

                    // pop new leader
                    queueLeaderElectionListener.next();

                    getZooKeeper().stop();

                    queueLeaderElectionListener.next();

                    getZooKeeper().restart();

                    // new old leader information should be announced
                    final LeaderInformation connectionReconnect =
                            queueLeaderElectionListener.next();
                    assertThat(connectionReconnect.getLeaderAddress()).isEqualTo(leaderAddress);
                });
    }

    private void writeLeaderInformationToZooKeeper(
            String retrievalPath, String leaderAddress, UUID sessionId) throws Exception {
        final byte[] data = createLeaderInformation(leaderAddress, sessionId);
        if (zooKeeperClient.checkExists().forPath(retrievalPath) != null) {
            zooKeeperClient.setData().forPath(retrievalPath, data);
        } else {
            zooKeeperClient.create().creatingParentsIfNeeded().forPath(retrievalPath, data);
        }
    }

    private byte[] createLeaderInformation(String leaderAddress, UUID sessionId)
            throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeUTF(leaderAddress);
            oos.writeObject(sessionId);

            oos.flush();

            return baos.toByteArray();
        }
    }

    @Test
    public void testNewLeaderAfterReconnectTriggersListenerNotification() throws Exception {
        testWithQueueLeaderElectionListener(
                queueLeaderElectionListener ->
                        new ZooKeeperLeaderRetrievalDriver(
                                zooKeeperClient,
                                "/testNewLeaderAfterReconnectTriggersListenerNotification",
                                queueLeaderElectionListener,
                                ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                                        .ON_SUSPENDED_CONNECTION,
                                fatalErrorHandlerResource.getTestingFatalErrorHandler()),
                (leaderRetrievalDriver, queueLeaderElectionListener) -> {
                    final String leaderAddress = "foobar";
                    final UUID sessionId = UUID.randomUUID();
                    writeLeaderInformationToZooKeeper(
                            leaderRetrievalDriver.getConnectionInformationPath(),
                            leaderAddress,
                            sessionId);

                    // pop new leader
                    queueLeaderElectionListener.next();

                    getZooKeeper().stop();

                    queueLeaderElectionListener.next();

                    getZooKeeper().restart();

                    final String newLeaderAddress = "barfoo";
                    final UUID newSessionId = UUID.randomUUID();
                    writeLeaderInformationToZooKeeper(
                            leaderRetrievalDriver.getConnectionInformationPath(),
                            newLeaderAddress,
                            newSessionId);

                    // check that we find the new leader information eventually
                    CommonTestUtils.waitUntilCondition(
                            () -> {
                                final LeaderInformation afterConnectionReconnect =
                                        queueLeaderElectionListener.next();
                                return afterConnectionReconnect.getLeaderAddress() != null
                                        && afterConnectionReconnect
                                                .getLeaderAddress()
                                                .equals(newLeaderAddress);
                            },
                            Deadline.fromNow(Duration.ofSeconds(30L)));
                });
    }

    private void testWithQueueLeaderElectionListener(
            FunctionWithException<
                            QueueLeaderElectionListener, ZooKeeperLeaderRetrievalDriver, Exception>
                    driverFactoryMethod,
            BiConsumerWithException<
                            ZooKeeperLeaderRetrievalDriver, QueueLeaderElectionListener, Exception>
                    testCallback)
            throws Exception {
        final QueueLeaderElectionListener queueLeaderElectionListener =
                new QueueLeaderElectionListener(1);

        ZooKeeperLeaderRetrievalDriver leaderRetrievalDriver = null;
        try {
            leaderRetrievalDriver = driverFactoryMethod.apply(queueLeaderElectionListener);

            testCallback.accept(leaderRetrievalDriver, queueLeaderElectionListener);
        } finally {
            queueLeaderElectionListener.clearUnhandledEvents();
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }
        }
    }

    private static class QueueLeaderElectionListener implements LeaderRetrievalEventHandler {

        private final BlockingQueue<LeaderInformation> queue;

        public QueueLeaderElectionListener(int expectedCalls) {
            this.queue = new ArrayBlockingQueue<>(expectedCalls);
        }

        @Override
        public void notifyLeaderAddress(LeaderInformation leaderInformation) {
            try {
                queue.put(leaderInformation);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        public LeaderInformation next() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        public Optional<LeaderInformation> next(Duration timeout) {
            try {
                return Optional.ofNullable(
                        this.queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        /**
         * Clears any unhandled events. This is useful in cases where there was an unplanned
         * reconnect after the test passed to prepare the shutdown. Outstanding events might cause
         * an {@link InterruptedException} during shutdown, otherwise.
         */
        public void clearUnhandledEvents() {
            while (!queue.isEmpty()) {
                queue.poll();
            }
        }
    }
}
