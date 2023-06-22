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
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultLeaderElectionService}. */
class DefaultLeaderElectionServiceTest {

    @RegisterExtension
    public final TestingFatalErrorHandlerExtension fatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    private static final String TEST_URL = "akka//user/jobmanager";

    @Test
    void testOnGrantAndRevokeLeadership() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>(LeaderInformationRegister.empty());
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            // grant leadership
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            testingContender.waitForLeader();
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(
                                            leaderElectionService.getLeaderSessionID(contenderID))
                                    .isEqualTo(leaderSessionID);

                            final LeaderInformation expectedLeaderInformationInHaBackend =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(contenderID),
                                            TEST_URL);
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as(
                                            "The HA backend should have its leader information updated.")
                                    .hasValue(expectedLeaderInformationInHaBackend);

                            revokeLeadership();

                            testingContender.waitForRevokeLeader();
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isNull();
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as(
                                            "External storage is not touched by the leader session because the leadership is already lost.")
                                    .hasValue(expectedLeaderInformationInHaBackend);
                        });
            }
        };
    }

    /**
     * Tests that we can shut down the DefaultLeaderElectionService if the used LeaderElectionDriver
     * holds an internal lock. See FLINK-20008 for more details.
     */
    @Test
    void testCloseGrantDeadlock() throws Exception {
        final OneShotLatch closeReachedLatch = new OneShotLatch();
        final OneShotLatch closeContinueLatch = new OneShotLatch();
        final OneShotLatch grantReachedLatch = new OneShotLatch();
        final OneShotLatch grantContinueLatch = new OneShotLatch();

        final CompletableFuture<Void> driverCloseTriggered = new CompletableFuture<>();

        final AtomicBoolean leadershipGranted = new AtomicBoolean();
        final TestingLeaderElectionDriver.Builder driverBuilder =
                TestingLeaderElectionDriver.newBuilder(leadershipGranted)
                        .setCloseConsumer(
                                lock -> {
                                    closeReachedLatch.trigger();
                                    closeContinueLatch.await();
                                    try {
                                        lock.lock();
                                        driverCloseTriggered.complete(null);
                                    } finally {
                                        lock.unlock();
                                    }
                                });

        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(driverBuilder);
        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory, fatalErrorHandlerExtension.getTestingFatalErrorHandler());
        testInstance.startLeaderElectionBackend();
        final TestingLeaderElectionDriver driver = driverFactory.assertAndGetOnlyCreatedDriver();

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

        final Thread grantThread =
                new Thread(
                        () -> {
                            try {
                                // simulates the grant process being triggered from the HA backend's
                                // side where the same lock that is acquired during the driver's
                                // process is also acquired while handling a leadership event
                                // processing
                                driver.getLock().lock();
                                grantReachedLatch.trigger();
                                grantContinueLatch.awaitQuietly();

                                // grants leadership
                                leadershipGranted.set(true);
                                testInstance.isLeader(UUID.randomUUID());
                            } finally {
                                driver.getLock().unlock();
                            }
                        },
                        "GrantThread");

        // triggers the service acquiring the leadership and, as a consequence, acquiring the
        // driver's lock
        grantThread.start();
        grantReachedLatch.await();

        // continue both processes which shouldn't result in a deadlock
        grantContinueLatch.trigger();
        closeContinueLatch.trigger();

        closeThread.join();
        grantThread.join();

        FlinkAssertions.assertThatFuture(driverCloseTriggered).eventuallySucceeds();
    }

    @Test
    void testGrantCallWhileInstantiatingDriver() throws Exception {
        final UUID expectedLeaderSessionID = UUID.randomUUID();
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        (listener, errorHandler) -> {
                            listener.isLeader(expectedLeaderSessionID);
                            return TestingLeaderElectionDriver.newNoOpBuilder()
                                    .build(listener, errorHandler);
                        },
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
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
                            grantLeadership(expectedSessionID);

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

                            grantLeadership();
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
        final AtomicBoolean leadershipGranted = new AtomicBoolean();
        final TestingLeaderElectionDriver.Builder driverBuilder =
                TestingLeaderElectionDriver.newBuilder(leadershipGranted);

        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(driverBuilder);
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory, fatalErrorHandlerExtension.getTestingFatalErrorHandler())) {
            driverBuilder.setCloseConsumer(lock -> testInstance.onRevokeLeadership());
            testInstance.startLeaderElectionBackend();

            leadershipGranted.set(true);
            testInstance.isLeader(UUID.randomUUID());

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
    void testContenderRegistrationWithoutDriverBeingInstantiatedFails() throws Exception {
        try (final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        TestingLeaderElectionDriver.Factory.createFactoryWithNoOpDriver(),
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler())) {
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
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            assertThat(testingContender.getLeaderSessionID()).isNotNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isEqualTo(leaderSessionID);
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .hasValue(LeaderInformation.known(leaderSessionID, TEST_URL));

                            leaderElection.close();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "The LeaderContender should have been informed about the leadership loss.")
                                    .isNull();
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .as(
                                            "The LeaderElectionService should have its internal state cleaned.")
                                    .isNull();
                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
                                    .as("The HA backend's data should have been cleaned.")
                                    .isEmpty();
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangedAndShouldBeCorrected() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            final LeaderInformation expectedLeaderInformation =
                                    LeaderInformation.known(leaderSessionID, TEST_URL);

                            // Leader information changed on external storage. It should be
                            // corrected.
                            storedLeaderInformation.set(LeaderInformationRegister.empty());
                            leaderElectionService.notifyLeaderInformationChange(
                                    contenderID, LeaderInformation.empty());
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as("Removed leader information should have been reset.")
                                    .hasValue(expectedLeaderInformation);

                            final LeaderInformation faultyLeaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            storedLeaderInformation.set(
                                    LeaderInformationRegister.of(
                                            contenderID, faultyLeaderInformation));
                            leaderElectionService.notifyLeaderInformationChange(
                                    contenderID, faultyLeaderInformation);
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as("Overwritten leader information should have been reset.")
                                    .hasValue(expectedLeaderInformation);
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
                            grantLeadership(expectedSessionID);

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
                            grantLeadership(expectedSessionID);

                            assertThat(testingContender.getLeaderSessionID()).isNull();

                            executorService.trigger();

                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(expectedSessionID);

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
                            grantLeadership();
                            executorService.trigger();

                            final UUID expectedSessionID = testingContender.getLeaderSessionID();
                            revokeLeadership();

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
                            grantLeadership();
                            executorService.trigger();

                            final UUID expectedSessionID = testingContender.getLeaderSessionID();

                            revokeLeadership();
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
                            grantLeadership();
                            executorService.trigger();

                            final UUID expectedSessionID = testingContender.getLeaderSessionID();
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
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final LeaderInformation differentLeaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "different-address");
                            storedLeaderInformation.set(
                                    LeaderInformationRegister.of(
                                            contenderID, differentLeaderInformation));
                            leaderElectionService.notifyLeaderInformationChange(
                                    contenderID, differentLeaderInformation);

                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as("The external storage shouldn't have been changed.")
                                    .hasValue(differentLeaderInformation);
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
                            grantLeadership();

                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .as(
                                            "The grant event shouldn't have been processed by the LeaderElectionService.")
                                    .isNull();
                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "The grant event shouldn't have been forwarded to the contender.")
                                    .isNull();
                        });
            }
        };
    }

    @Test
    void testOnLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingStop() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            grantLeadership();

                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .isPresent();

                            leaderElection.close();

                            storedLeaderInformation.set(LeaderInformationRegister.empty());
                            leaderElectionService.notifyLeaderInformationChange(
                                    contenderID, LeaderInformation.empty());

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
                                    .as("The external storage shouldn't have been corrected.")
                                    .isEmpty();
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
                            grantLeadership();
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
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            grantLeadership();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID(contenderID);
                            assertThat(currentLeaderSessionId).isNotNull();
                            final LeaderInformation expectedLeaderInformation =
                                    LeaderInformation.known(currentLeaderSessionId, TEST_URL);
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .hasValue(expectedLeaderInformation);

                            // Old confirm call should be ignored.
                            leaderElection.confirmLeadership(UUID.randomUUID(), TEST_URL);
                            assertThat(leaderElectionService.getLeaderSessionID(contenderID))
                                    .isEqualTo(currentLeaderSessionId);
                            assertThat(storedLeaderInformation.get().forContenderID(contenderID))
                                    .as(
                                            "The leader information in the external storage shouldn't have been updated.")
                                    .hasValue(expectedLeaderInformation);
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
                            grantLeadership();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID(contenderID);
                            assertThat(currentLeaderSessionId).isNotNull();

                            revokeLeadership();

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

                            testingLeaderElectionDriver.triggerFatalError(testException);

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

                            testingLeaderElectionDriver.triggerFatalError(testException);
                            assertThat(testingContender.getError()).isNull();

                            assertThat(
                                            fatalErrorHandlerExtension
                                                    .getTestingFatalErrorHandler()
                                                    .getException())
                                    .as(
                                            "The fallback error handler should have caught the error in this case.")
                                    .isEqualTo(testException);

                            fatalErrorHandlerExtension.getTestingFatalErrorHandler().clearError();
                        });
            }
        };
    }

    @Test
    void testOnLeadershipChangeDoesNotBlock() throws Exception {
        final CompletableFuture<LeaderInformation> initialLeaderInformation =
                new CompletableFuture<>();
        final OneShotLatch latch = new OneShotLatch();

        final AtomicBoolean leadershipGranted = new AtomicBoolean(false);
        final TestingLeaderElectionDriver.Builder driverBuilder =
                TestingLeaderElectionDriver.newBuilder(
                                leadershipGranted, new AtomicReference<>(), new AtomicBoolean())
                        .setPublishLeaderInformationConsumer(
                                (lock, contenderID, leaderInformation) -> {
                                    try {
                                        lock.lock();
                                        // the first call saves the confirmed LeaderInformation
                                        if (!initialLeaderInformation.isDone()) {
                                            initialLeaderInformation.complete(leaderInformation);
                                        } else {
                                            latch.awaitQuietly();
                                        }
                                    } finally {
                                        lock.unlock();
                                    }
                                })
                        .setHasLeadershipFunction(lock -> leadershipGranted.get());
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(driverBuilder);
        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory, fatalErrorHandlerExtension.getTestingFatalErrorHandler());
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
        leadershipGranted.set(true);
        testInstance.isLeader(sessionID);

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
                (leadershipGranted, listener) -> {
                    leadershipGranted.set(true);
                    listener.isLeader(UUID.randomUUID());
                });
    }

    @Test
    void testOnRevokeLeadershipDoesNotBlock() throws Exception {
        testNonBlockingCall(
                latch ->
                        TestingGenericLeaderContender.newBuilder()
                                .setRevokeLeadershipRunnable(latch::awaitQuietly)
                                .build(),
                (leadershipGranted, listener) -> {
                    leadershipGranted.set(true);
                    listener.isLeader(UUID.randomUUID());

                    leadershipGranted.set(false);
                    // this call should not block the test execution
                    listener.notLeader();
                });
    }

    private void testNonBlockingCall(
            Function<OneShotLatch, TestingGenericLeaderContender> contenderCreator,
            BiConsumer<AtomicBoolean, MultipleComponentLeaderElectionDriver.Listener>
                    listenerAction)
            throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final TestingGenericLeaderContender contender = contenderCreator.apply(latch);

        final AtomicBoolean leadershipGranted = new AtomicBoolean(false);
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(
                                leadershipGranted, new AtomicReference<>(), new AtomicBoolean()));
        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory, fatalErrorHandlerExtension.getTestingFatalErrorHandler());
        testInstance.startLeaderElectionBackend();

        final LeaderElection leaderElection =
                testInstance.createLeaderElection(createRandomContenderID());
        leaderElection.startLeaderElection(contender);

        listenerAction.accept(leadershipGranted, testInstance);

        latch.trigger();

        leaderElection.close();
        testInstance.close();
    }

    private static String createRandomContenderID() {
        return String.format("contender-id-%s", UUID.randomUUID());
    }

    private class Context {

        private final TestingLeaderElectionDriver.Factory driverFactory;

        private final AtomicBoolean leadershipGranted;

        final String contenderID = createRandomContenderID();

        DefaultLeaderElectionService leaderElectionService;
        TestingContender testingContender;

        TestingLeaderElectionDriver testingLeaderElectionDriver;

        LeaderElection leaderElection;

        private Context() {
            this(new AtomicBoolean(false), new AtomicReference<>());
        }

        private Context(AtomicReference<LeaderInformationRegister> storedLeaderInformation) {
            this(new AtomicBoolean(false), storedLeaderInformation);
        }

        private Context(
                AtomicBoolean leadershipGranted,
                AtomicReference<LeaderInformationRegister> storedLeaderInformation) {
            this(
                    leadershipGranted,
                    TestingLeaderElectionDriver.newBuilder(
                            leadershipGranted, storedLeaderInformation, new AtomicBoolean()));
        }

        private Context(
                AtomicBoolean leadershipGranted,
                TestingLeaderElectionDriver.Builder driverBuilder) {
            this.leadershipGranted = leadershipGranted;
            this.driverFactory = new TestingLeaderElectionDriver.Factory(driverBuilder);
        }

        void grantLeadership() {
            grantLeadership(UUID.randomUUID());
        }

        void grantLeadership(UUID leaderSessionID) {
            leadershipGranted.set(true);
            leaderElectionService.isLeader(leaderSessionID);
        }

        void revokeLeadership() {
            leadershipGranted.set(false);
            leaderElectionService.notLeader();
        }

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
            try (final DefaultLeaderElectionService localLeaderElectionService =
                        new DefaultLeaderElectionService(
                                driverFactory,
                                DefaultLeaderElectionServiceTest.this.fatalErrorHandlerExtension
                                        .getTestingFatalErrorHandler(),
                                leaderEventOperationExecutor)) {
                leaderElectionService = localLeaderElectionService;
                leaderElectionService.startLeaderElectionBackend();
                testingLeaderElectionDriver = driverFactory.assertAndGetOnlyCreatedDriver();

                leaderElection = leaderElectionService.createLeaderElection(contenderID);
                testingContender = new TestingContender(TEST_URL, leaderElection);
                testingContender.startLeaderElection();

                testMethod.run();
            } finally {
                if (leaderElection != null) {
                    leaderElection.close();
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
