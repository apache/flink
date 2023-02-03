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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultLeaderElectionService}. */
class DefaultLeaderElectionServiceTest {

    private static final String TEST_URL = "akka//user/jobmanager";

    @Test
    void testOnGrantAndRevokeLeadership() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            // grant leadership
                            testingLeaderElectionDriver.isLeader();

                            testingContender.waitForLeader();
                            assertThat(testingContender.getDescription()).isEqualTo(TEST_URL);
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(leaderElectionService.getLeaderSessionID());

                            final LeaderInformation expectedLeaderInformationInHaBackend =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(), TEST_URL);
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .as(
                                            "The HA backend should have its leader information updated.")
                                    .isEqualTo(expectedLeaderInformationInHaBackend);

                            // revoke leadership
                            testingLeaderElectionDriver.notLeader();
                            testingContender.waitForRevokeLeader();
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                            assertThat(leaderElectionService.getLeaderSessionID()).isNull();
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .as(
                                            "External storage is not touched by the leader session because the leadership is already lost.")
                                    .isEqualTo(expectedLeaderInformationInHaBackend);
                        });
            }
        };
    }

    @Test
    void testDelayedGrantCallAfterContenderRegistration() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            // we need to stop to deregister the contender that was already
                            // registered to the service
                            leaderElectionService.stop();

                            final UUID expectedSessionID = UUID.randomUUID();
                            testingLeaderElectionDriver.isLeader(expectedSessionID);

                            leaderElectionService.start(testingContender);

                            assertThat(testingContender.getLeaderSessionID())
                                    .as("Leadership grant was not forwarded to the contender, yet.")
                                    .isNull();

                            executorService.trigger();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as("Leadership grant is actually forwarded to the service.")
                                    .isEqualTo(expectedSessionID);

                            testingContender.waitForLeader();
                        });
            }
        };
    }

    @Test
    void testContenderRegistrationWithoutDriverBeingInstantiatedFails() throws Exception {
        try (final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory())) {
            assertThatThrownBy(
                            () ->
                                    leaderElectionService.start(
                                            new TestingContender(
                                                    "unused-address", leaderElectionService)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testDriverShutdownFailsWithContenderStillBeingRegistered() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () ->
                                assertThatThrownBy(leaderElectionService::close)
                                        .as(
                                                "The LeaderContender needs to be deregistered before closing the driver.")
                                        .isInstanceOf(IllegalStateException.class));
            }
        };
    }

    @Test
    void testProperCleanupOnStopWhenHoldingTheLeadership() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            testingContender.waitForLeader();

                            assertThat(testingContender.getLeaderSessionID()).isNotNull();
                            assertThat(leaderElectionService.getLeaderSessionID()).isNotNull();
                            assertThat(testingLeaderElectionDriver.getLeaderInformation().isEmpty())
                                    .isFalse();

                            leaderElectionService.stop();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "The LeaderContender should have been informed about the leadership loss.")
                                    .isNull();
                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .as(
                                            "The LeaderElectionService should have its internal state cleaned.")
                                    .isNull();
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .as("The HA backend's data should have been cleaned.")
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangedAndShouldBeCorrected() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();

                            final LeaderInformation expectedLeader =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(), TEST_URL);

                            // Leader information changed on external storage. It should be
                            // corrected.
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);

                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address"));
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWithLeadershipButNoGrantEventProcessed() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();

                            testingLeaderElectionDriver.isLeader(expectedSessionID);

                            assertThat(leaderElectionService.hasLeadership(expectedSessionID))
                                    .isFalse();
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWithLeadershipAndGrantEventProcessed() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();

                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();

                            assertThat(leaderElectionService.hasLeadership(expectedSessionID))
                                    .isTrue();
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWithLeadershipLostButNoRevokeEventProcessed() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();

                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();
                            testingLeaderElectionDriver.notLeader();

                            assertThat(leaderElectionService.hasLeadership(expectedSessionID))
                                    .as(
                                            "No operation should be handled anymore after the HA backend "
                                                    + "indicated leadership loss even if the onRevokeLeadership wasn't "
                                                    + "processed, yet, because some other process could have picked up "
                                                    + "the leadership in the meantime already based on the HA "
                                                    + "backend's decision.")
                                    .isFalse();
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWithLeadershipLostAndRevokeEventProcessed() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();

                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();

                            testingLeaderElectionDriver.notLeader();
                            executorService.trigger();

                            assertThat(leaderElectionService.hasLeadership(expectedSessionID))
                                    .isFalse();
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testHasLeadershipAfterStop() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();

                            leaderElectionService.stop();

                            assertThat(leaderElectionService.hasLeadership(expectedSessionID))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangedIfNotBeingLeader() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final LeaderInformation faultyLeader =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            testingLeaderElectionDriver.leaderInformationChanged(faultyLeader);
                            // External storage should keep the wrong value.
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(faultyLeader);
                        });
            }
        };
    }

    @Test
    void testOnGrantLeadershipIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.isLeader();

                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .as(
                                            "The grant event shouldn't have been processed by the LeaderElectionService.")
                                    .isNull();
                            // leader contender is not granted leadership
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                        });
            }
        };
    }

    @Test
    void testOnLeaderInformationChangeIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();

                            leaderElectionService.stop();
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());

                            // External storage should not be corrected
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testOnRevokeLeadershipIsTriggeredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID oldSessionId = leaderElectionService.getLeaderSessionID();
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);

                            leaderElectionService.stop();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "LeaderContender should have been revoked as part of the stop call.")
                                    .isNull();
                        });
            }
        };
    }

    @Test
    void testOldConfirmLeaderInformation() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isNotNull();

                            // Old confirm call should be ignored.
                            leaderElectionService.confirmLeadership(UUID.randomUUID(), TEST_URL);
                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .isEqualTo(currentLeaderSessionId);
                        });
            }
        };
    }

    @Test
    void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final Exception testException = new Exception("test leader exception");

                            testingLeaderElectionDriver.onFatalError(testException);

                            testingContender.waitForError();
                            assertThat(testingContender.getError())
                                    .isNotNull()
                                    .hasCause(testException);
                        });
            }
        };
    }

    @Test
    void testErrorIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final Exception testException = new Exception("test leader exception");

                            leaderElectionService.stop();
                            testingLeaderElectionDriver.onFatalError(testException);
                            assertThat(testingContender.getError()).isNull();
                        });
            }
        };
    }

    /**
     * Tests that we can shut down the DefaultLeaderElectionService if the used LeaderElectionDriver
     * holds an internal lock. See FLINK-20008 for more details.
     */
    @Test
    void testServiceShutDownWithSynchronizedDriver() throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);

        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);

        leaderElectionService.startLeaderElectionBackend();
        leaderElectionService.start(testingContender);

        final TestingLeaderElectionDriver currentLeaderDriver =
                Preconditions.checkNotNull(
                        testingLeaderElectionDriverFactory.getCurrentLeaderDriver());

        currentLeaderDriver.isLeader();

        leaderElectionService.stop();
    }

    @Test
    void testOnLeadershipChangeDoesNotBlock() throws Exception {
        final CompletableFuture<LeaderInformation> initialLeaderInformation =
                new CompletableFuture<>();
        final OneShotLatch latch = new OneShotLatch();

        final TestingGenericLeaderElectionDriver driver =
                TestingGenericLeaderElectionDriver.newBuilder()
                        .setWriteLeaderInformationConsumer(
                                leaderInformation -> {
                                    // the first call saves the confirmed LeaderInformation
                                    if (!initialLeaderInformation.isDone()) {
                                        initialLeaderInformation.complete(leaderInformation);
                                    } else {
                                        latch.awaitQuietly();
                                    }
                                })
                        .setHasLeadershipSupplier(() -> true)
                        .build();

        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        (leaderElectionEventHandler, errorHandler) -> driver);
        testInstance.startLeaderElectionBackend();

        final String address = "leader-address";
        testInstance.start(
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(
                                sessionID -> testInstance.confirmLeadership(sessionID, address))
                        .build());

        // initial messages to initialize usedLeaderSessionID and confirmedLeaderInformation
        final UUID sessionID = UUID.randomUUID();
        testInstance.onGrantLeadership(sessionID);

        FlinkAssertions.assertThatFuture(initialLeaderInformation)
                .eventuallySucceeds()
                .as("The LeaderInformation should have been forwarded to the driver.")
                .isEqualTo(LeaderInformation.known(sessionID, address));

        // this call shouldn't block the test execution
        testInstance.onLeaderInformationChange(LeaderInformation.empty());

        latch.trigger();

        testInstance.stop();
        testInstance.close();
    }

    @Test
    void testOnGrantLeadershipAsyncDoesNotBlock() throws Exception {
        testNonBlockingCall(
                latch ->
                        TestingGenericLeaderContender.newBuilder()
                                .setGrantLeadershipConsumer(
                                        ignoredSessionID -> latch.awaitQuietly())
                                .build(),
                TestingLeaderElectionDriver::isLeader);
    }

    @Test
    void testOnRevokeLeadershipDoesNotBlock() throws Exception {
        testNonBlockingCall(
                latch ->
                        TestingGenericLeaderContender.newBuilder()
                                .setRevokeLeadershipRunnable(latch::awaitQuietly)
                                .build(),
                driver -> {
                    driver.isLeader();
                    // this call should not block the test execution
                    driver.notLeader();
                });
    }

    private static void testNonBlockingCall(
            Function<OneShotLatch, TestingGenericLeaderContender> contenderCreator,
            Consumer<TestingLeaderElectionDriver> driverAction)
            throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final TestingGenericLeaderContender contender = contenderCreator.apply(latch);

        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();

        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(driverFactory);
        testInstance.startLeaderElectionBackend();
        testInstance.start(contender);

        final TestingLeaderElectionDriver driver = driverFactory.getCurrentLeaderDriver();
        assertThat(driver).isNotNull();

        driverAction.accept(driver);

        latch.trigger();

        testInstance.stop();
        testInstance.close();
    }

    private static class Context {

        DefaultLeaderElectionService leaderElectionService;
        TestingContender testingContender;

        TestingLeaderElectionDriver testingLeaderElectionDriver;

        void runTestWithSynchronousEventHandling(RunnableWithException testMethod)
                throws Exception {
            runTest(testMethod, Executors.newDirectExecutorService());
        }

        void runTestWithManuallyTriggeredEvents(
                ThrowingConsumer<ManuallyTriggeredScheduledExecutorService, Exception> testMethod)
                throws Exception {
            final ManuallyTriggeredScheduledExecutorService executorService =
                    new ManuallyTriggeredScheduledExecutorService();
            runTest(() -> testMethod.accept(executorService), executorService);
        }

        void runTest(RunnableWithException testMethod, ExecutorService leaderEventOperationExecutor)
                throws Exception {
            final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                    new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
            leaderElectionService =
                    new DefaultLeaderElectionService(driverFactory, leaderEventOperationExecutor);
            leaderElectionService.startLeaderElectionBackend();
            testingContender = new TestingContender(TEST_URL, leaderElectionService);

            leaderElectionService.start(testingContender);
            testingLeaderElectionDriver = driverFactory.getCurrentLeaderDriver();

            assertThat(testingLeaderElectionDriver).isNotNull();
            testMethod.run();

            leaderElectionService.stop();
            leaderElectionService.close();
        }
    }
}
