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
                                    .isEqualTo(
                                            leaderElectionService.getLeaderSessionID(contenderID));

                            final LeaderInformation expectedLeaderInformationInHaBackend =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(contenderID),
                                            TEST_URL);
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .as(
                                            "The HA backend should have its leader information updated.")
                                    .isEqualTo(expectedLeaderInformationInHaBackend);

                            // revoke leadership
                            testingLeaderElectionDriver.notLeader();
                            testingContender.waitForRevokeLeader();
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isNull();
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .as(
                                            "External storage is not touched by the leader session because the leadership is already lost.")
                                    .isEqualTo(expectedLeaderInformationInHaBackend);
                        });
            }
        };
    }

    @Test
    void testCloseGrantDeadlock() throws Exception {
        final OneShotLatch closeReachedLatch = new OneShotLatch();
        final OneShotLatch closeContinueLatch = new OneShotLatch();
        final OneShotLatch grantReachedLatch = new OneShotLatch();
        final OneShotLatch grantContinueLatch = new OneShotLatch();

        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory(
                        eventHandler -> {},
                        eventHandler -> {
                            closeReachedLatch.trigger();
                            closeContinueLatch.await();
                        },
                        leaderElectionEventHandler -> {
                            grantReachedLatch.trigger();
                            grantContinueLatch.awaitQuietly();
                        });

        final ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(driverFactory, executorService);
        testInstance.startLeaderElectionBackend();
        final TestingLeaderElectionDriver driver = driverFactory.getCurrentLeaderDriver();
        assertThat(driver).isNotNull();

        final Thread closeThread =
                new Thread(
                        () -> {
                            try {
                                testInstance.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        "CloseThread");

        // triggers close that acquires the DefaultLeaderElectionService lock
        closeThread.start();
        closeReachedLatch.await();

        final Thread grantThread = new Thread(driver::isLeader, "GrantThread");

        // triggers the service acquiring the leadership and, as a consequence, acquiring the
        // driver's lock
        grantThread.start();
        grantReachedLatch.await();

        // continue both processes which shouldn't result in a deadlock
        grantContinueLatch.trigger();
        closeContinueLatch.trigger();

        closeThread.join();
        grantThread.join();
    }

    /**
     * With {@link MultipleComponentLeaderElectionDriverAdapter} and {@link
     * DefaultMultipleComponentLeaderElectionService} it happens that {@link
     * LeaderElectionEventHandler#onGrantLeadership(UUID)} happens while instantiating the {@link
     * LeaderElectionDriver} (i.e. {@code MultipleComponentLeaderElectionDriverAdapter}). This test
     * verifies that the grant event is handled properly.
     */
    @Test
    void testGrantCallWhileInstantiatingDriver() throws Exception {
        final UUID expectedLeaderSessionID = UUID.randomUUID();
        try (final TestingGenericLeaderElectionDriver driver =
                        TestingGenericLeaderElectionDriver.newBuilder().build();
                final DefaultLeaderElectionService testInstance =
                        new DefaultLeaderElectionService(
                                (eventHandler, errorHandler) -> {
                                    eventHandler.onGrantLeadership(expectedLeaderSessionID);
                                    return driver;
                                },
                                Executors.newDirectExecutorService())) {
            testInstance.startLeaderElectionBackend();

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomContenderID());
            final TestingContender testingContender =
                    new TestingContender("unused-address", leaderElection);
            testingContender.startLeaderElection();

            assertThat(testingContender.getLeaderSessionID()).isEqualTo(expectedLeaderSessionID);

            leaderElection.close();
        }
    }

    @Test
    void testDelayedGrantCallAfterContenderRegistration() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            // we need to stop to deregister the contender that was already
                            // registered to the service
                            leaderElection.close();

                            final UUID expectedSessionID = UUID.randomUUID();
                            testingLeaderElectionDriver.isLeader(expectedSessionID);

                            try (LeaderElection anotherLeaderElection =
                                    leaderElectionService.createLeaderElection(contenderID)) {
                                final TestingContender testingContender =
                                        new TestingContender(TEST_URL, anotherLeaderElection);
                                testingContender.startLeaderElection();

                                assertThat(testingContender.getLeaderSessionID())
                                        .as(
                                                "Leadership grant was not forwarded to the contender, yet.")
                                        .isNull();

                                executorService.trigger();

                                assertThat(testingContender.getLeaderSessionID())
                                        .as(
                                                "Leadership grant is actually forwarded to the service.")
                                        .isEqualTo(expectedSessionID);

                                testingContender.waitForLeader();
                            }
                        });
            }
        };
    }

    @Test
    void testDelayedGrantCallAfterContenderBeingDeregisteredAgain() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            // we need to close the LeaderElection to deregister the contender that
                            // was already registered to the service
                            leaderElection.close();

                            final UUID expectedSessionID = UUID.randomUUID();
                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();

                            leaderElection =
                                    leaderElectionService.createLeaderElection(contenderID);
                            final TestingContender contender =
                                    new TestingContender("unused-address", leaderElection);
                            contender.startLeaderElection();

                            leaderElection.close();

                            executorService.trigger();
                        });
            }
        };
    }

    /**
     * Test to cover the issue described in FLINK-31814. This test could be removed after
     * FLINK-31814 is resolved.
     */
    @Test
    void testOnRevokeCallWhileClosingService() throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory(
                        LeaderElectionEventHandler::onRevokeLeadership);

        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(driverFactory)) {
            testInstance.startLeaderElectionBackend();

            final TestingLeaderElectionDriver driver = driverFactory.getCurrentLeaderDriver();
            assertThat(driver).isNotNull();

            driver.isLeader();

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomContenderID());
            final TestingContender contender =
                    new TestingContender("unused-address", leaderElection);
            contender.startLeaderElection();

            contender.waitForLeader();

            leaderElection.close();

            contender.throwErrorIfPresent();
        }
    }

    @Test
    void testStopWhileHavingLeadership() throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();

        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(driverFactory)) {
            testInstance.startLeaderElectionBackend();

            final TestingLeaderElectionDriver driver = driverFactory.getCurrentLeaderDriver();
            assertThat(driver).isNotNull();

            driver.isLeader();

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomContenderID());
            leaderElection.startLeaderElection(TestingGenericLeaderContender.newBuilder().build());

            leaderElection.close();
        }
    }

    @Test
    void testContenderRegistrationWithoutDriverBeingInstantiatedFails() throws Exception {
        try (final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory())) {
            final LeaderElection leaderElection =
                    leaderElectionService.createLeaderElection(createRandomContenderID());
            assertThatThrownBy(
                            () ->
                                    new TestingContender("unused-address", leaderElection)
                                            .startLeaderElection())
                    .isInstanceOf(IllegalStateException.class);

            // starting the backend because the close method expects it to be initialized
            leaderElectionService.startLeaderElectionBackend();
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
    void testProperCleanupOnLeaderElectionCloseWhenHoldingTheLeadership() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            testingContender.waitForLeader();

                            assertThat(testingContender.getLeaderSessionID()).isNotNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isNotNull();
                            assertThat(testingLeaderElectionDriver.getLeaderInformation().isEmpty())
                                    .isFalse();

                            leaderElection.close();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "The LeaderContender should have been informed about the leadership loss.")
                                    .isNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
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
                                            leaderElectionService.getLeaderSessionID(contenderID),
                                            TEST_URL);

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

                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, expectedSessionID))
                                    .isFalse();
                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, UUID.randomUUID()))
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

                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, expectedSessionID))
                                    .isTrue();
                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, UUID.randomUUID()))
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

                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, expectedSessionID))
                                    .as(
                                            "No operation should be handled anymore after the HA backend "
                                                    + "indicated leadership loss even if the onRevokeLeadership wasn't "
                                                    + "processed, yet, because some other process could have picked up "
                                                    + "the leadership in the meantime already based on the HA "
                                                    + "backend's decision.")
                                    .isFalse();
                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, UUID.randomUUID()))
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

                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, expectedSessionID))
                                    .isFalse();
                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, UUID.randomUUID()))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testHasLeadershipAfterLeaderElectionClose() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            testingLeaderElectionDriver.isLeader(expectedSessionID);
                            executorService.trigger();

                            leaderElection.close();

                            assertThat(
                                            leaderElectionService.hasLeadership(
                                                    contenderID, expectedSessionID))
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
    void testOnGrantLeadershipIsIgnoredAfterLeaderElectionBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            leaderElection.close();
                            testingLeaderElectionDriver.isLeader();

                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
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
    void testOnLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();

                            leaderElection.close();
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
    void testOnRevokeLeadershipIsTriggeredAfterLeaderElectionBeingStop() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID oldSessionId =
                                    leaderElectionService.getLeaderSessionID(contenderID);
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);

                            leaderElection.close();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "LeaderContender should have been revoked as part of the stop call.")
                                    .isNull();
                        });
            }
        };
    }

    @Test
    void testOldConfirmLeaderInformationWhileHavingNewLeadership() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID(contenderID);
                            assertThat(currentLeaderSessionId).isNotNull();

                            // Old confirm call should be ignored.
                            leaderElection.confirmLeadership(UUID.randomUUID(), TEST_URL);
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isEqualTo(currentLeaderSessionId);
                        });
            }
        };
    }

    @Test
    void testOldConfirmationWhileHavingLeadershipLost() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID(contenderID);
                            assertThat(currentLeaderSessionId).isNotNull();

                            testingLeaderElectionDriver.notLeader();

                            // Old confirm call should be ignored.
                            leaderElection.confirmLeadership(currentLeaderSessionId, TEST_URL);

                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isNull();
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

                            testingContender.clearError();
                        });
            }
        };
    }

    @Test
    void testErrorIsIgnoredAfterLeaderElectionBeingClosed() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final Exception testException = new Exception("test leader exception");

                            leaderElection.close();

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
        leaderElectionService.startLeaderElectionBackend();

        final LeaderElection leaderElection =
                leaderElectionService.createLeaderElection(createRandomContenderID());
        final TestingContender testingContender = new TestingContender(TEST_URL, leaderElection);
        testingContender.startLeaderElection();

        final TestingLeaderElectionDriver currentLeaderDriver =
                Preconditions.checkNotNull(
                        testingLeaderElectionDriverFactory.getCurrentLeaderDriver());

        currentLeaderDriver.isLeader();

        leaderElection.close();
        leaderElectionService.close();

        testingContender.throwErrorIfPresent();
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

        final String contenderID = "contender-id";
        final String address = "leader-address";
        final LeaderElection leaderElection = testInstance.createLeaderElection(contenderID);
        leaderElection.startLeaderElection(
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(
                                sessionID ->
                                        testInstance.confirmLeadership(
                                                contenderID, sessionID, address))
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

        leaderElection.close();
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
        final LeaderElection leaderElection =
                testInstance.createLeaderElection(createRandomContenderID());
        leaderElection.startLeaderElection(contender);

        final TestingLeaderElectionDriver driver = driverFactory.getCurrentLeaderDriver();
        assertThat(driver).isNotNull();

        driverAction.accept(driver);

        latch.trigger();

        leaderElection.close();
        testInstance.close();
    }

    private static String createRandomContenderID() {
        return String.format("contender-id-%s", UUID.randomUUID());
    }

    private static class Context {

        final String contenderID = createRandomContenderID();

        DefaultLeaderElectionService leaderElectionService;
        TestingContender testingContender;

        TestingLeaderElectionDriver testingLeaderElectionDriver;

        LeaderElection leaderElection;

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
            try {
                final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory driverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
                leaderElectionService =
                        new DefaultLeaderElectionService(
                                driverFactory, leaderEventOperationExecutor);
                leaderElectionService.startLeaderElectionBackend();

                leaderElection = leaderElectionService.createLeaderElection(contenderID);
                testingContender = new TestingContender(TEST_URL, leaderElection);
                testingContender.startLeaderElection();

                testingLeaderElectionDriver = driverFactory.getCurrentLeaderDriver();

                assertThat(testingLeaderElectionDriver).isNotNull();
                testMethod.run();
            } finally {
                if (leaderElection != null) {
                    leaderElection.close();
                }

                if (leaderElectionService != null) {
                    leaderElectionService.close();
                }

                if (testingContender != null) {
                    testingContender.throwErrorIfPresent();
                }

                if (testingLeaderElectionDriver != null) {
                    testingLeaderElectionDriver.close();
                }
            }
        }
    }
}
