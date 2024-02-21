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
import org.apache.flink.util.function.TriConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.contender.waitForLeader();
                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .isEqualTo(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isEqualTo(leaderSessionID);

                                        final LeaderInformation
                                                expectedLeaderInformationInHaBackend =
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address);
                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forComponentId(ctx.componentId))
                                                .as(
                                                        "The HA backend should have its leader information updated.")
                                                .hasValue(expectedLeaderInformationInHaBackend);
                                    });

                            revokeLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.contender.waitForRevokeLeader();
                                        assertThat(ctx.contender.getLeaderSessionID()).isNull();
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isNull();

                                        final LeaderInformation
                                                expectedLeaderInformationInHaBackend =
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address);

                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forComponentId(ctx.componentId))
                                                .as(
                                                        "External storage is not touched by the leader session because the leadership is already lost.")
                                                .hasValue(expectedLeaderInformationInHaBackend);
                                    });
                        });
            }
        };
    }

    @Test
    void testErrorOnComponentIdReuse() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () ->
                                assertThatThrownBy(
                                                () ->
                                                        leaderElectionService.createLeaderElection(
                                                                contenderContext0.componentId))
                                        .isInstanceOf(IllegalStateException.class));
            }
        };
    }

    /**
     * Tests that we can shut down the DefaultLeaderElectionService if the used {@link
     * LeaderElectionDriver} holds an internal lock. See FLINK-20008 for more details.
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
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory, fatalErrorHandlerExtension.getTestingFatalErrorHandler())) {

            // creating the LeaderElection is necessary to instantiate the driver
            final LeaderElection leaderElection = testInstance.createLeaderElection("component-id");
            leaderElection.startLeaderElection(TestingGenericLeaderContender.newBuilder().build());

            final TestingLeaderElectionDriver driver =
                    driverFactory.assertAndGetOnlyCreatedDriver();

            final Thread closeThread =
                    new Thread(
                            () -> {
                                try {
                                    leaderElection.close();
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
                                    // simulates the grant process being triggered from the HA
                                    // backend's side where the same lock that is acquired during
                                    // the driver's process is also acquired while handling a
                                    // leadership event processing
                                    driver.getLock().lock();
                                    grantReachedLatch.trigger();
                                    grantContinueLatch.awaitQuietly();

                                    // grants leadership
                                    leadershipGranted.set(true);
                                    testInstance.onGrantLeadership(UUID.randomUUID());
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
    }

    @Test
    void testLazyDriverInstantiation() throws Exception {
        final AtomicBoolean driverCreated = new AtomicBoolean();
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        listener -> {
                            driverCreated.set(true);
                            return TestingLeaderElectionDriver.newNoOpBuilder().build(listener);
                        },
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        Executors.newDirectExecutorService())) {
            assertThat(driverCreated)
                    .as("The driver shouldn't have been created during service creation.")
                    .isFalse();

            try (final LeaderElection leaderElection =
                    testInstance.createLeaderElection("component-id")) {
                assertThat(driverCreated)
                        .as(
                                "The driver shouldn't have been created during LeaderElection creation.")
                        .isFalse();

                leaderElection.startLeaderElection(
                        TestingGenericLeaderContender.newBuilder().build());
                assertThat(driverCreated)
                        .as(
                                "The driver should have been created when registering the contender in the LeaderElection.")
                        .isTrue();
            }
        }
    }

    @Test
    void testReuseOfServiceIsRestricted() throws Exception {
        final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        new TestingLeaderElectionDriver.Factory(
                                TestingLeaderElectionDriver.newNoOpBuilder()));

        // The driver hasn't started, yet, which prevents the service from going into running state.
        // This results in the close method not having any effect.
        testInstance.close();

        assertThatThrownBy(() -> testInstance.createLeaderElection("component-id"))
                .as(
                        "Registering a contender on a closed service should have resulted in an IllegalStateException.")
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * The leadership can be granted as soon as the driver is instantiated. We need to make sure
     * that the event is still handled properly while registering the contender.
     */
    @Test
    void testMultipleDriverCreations() throws Exception {
        final AtomicInteger closeCount = new AtomicInteger(0);
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newNoOpBuilder()
                                .setCloseConsumer(ignoredLock -> closeCount.incrementAndGet()));

        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(driverFactory)) {

            final String componentId = "component_id";
            final int numberOfStartCloseSessions = 2;
            for (int i = 1; i <= numberOfStartCloseSessions; i++) {
                assertThat(driverFactory.getCreatedDriverCount()).isEqualTo(i - 1);
                assertThat(closeCount).hasValue(i - 1);

                try (final LeaderElection leaderElection =
                        testInstance.createLeaderElection(componentId)) {
                    leaderElection.startLeaderElection(
                            TestingGenericLeaderContender.newBuilder().build());
                }

                assertThat(driverFactory.getCreatedDriverCount()).isEqualTo(i);
                assertThat(closeCount).hasValue(i);
            }
        }
    }

    /**
     * The leadership can be granted as soon as the driver is instantiated. We need to make sure
     * that the event is still handled properly while registering the contender.
     */
    @Test
    void testGrantCallWhileInstantiatingDriver() throws Exception {
        final UUID expectedLeaderSessionID = UUID.randomUUID();
        final TestingLeaderElectionDriver.Builder driverBuilder =
                TestingLeaderElectionDriver.newNoOpBuilder();
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        listener -> {
                            final OneShotLatch waitForGrantTriggerLatch = new OneShotLatch();
                            // calling the grant from a separate thread will let the thread
                            // end in the service's lock until the registration of the
                            // contender is done
                            CompletableFuture.runAsync(
                                    () -> {
                                        waitForGrantTriggerLatch.trigger();
                                        listener.onGrantLeadership(expectedLeaderSessionID);
                                    });

                            waitForGrantTriggerLatch.await();

                            // we can't ensure easily that the future finally called the
                            // method that triggers the grant event: Waiting for the method
                            // to return would lead to a deadlock because the grant
                            // processing is going to wait for the
                            // DefaultLeaderElectionService#lock before it can submit the
                            // event processing. Adding a short sleep here is a nasty
                            // workaround to simulate the race condition.
                            Thread.sleep(100);

                            return driverBuilder.build(listener);
                        },
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        Executors.newDirectExecutorService())) {

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomComponentId());
            final TestingContender testingContender =
                    new TestingContender("unused-address", leaderElection);
            testingContender.startLeaderElection();

            testingContender.waitForLeader();

            assertThat(testingContender.getLeaderSessionID()).isEqualTo(expectedLeaderSessionID);

            leaderElection.close();
        }
    }

    @Test
    void testDelayedGrantCallAfterContenderRegistration() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newNoOpBuilder());
        final ManuallyTriggeredScheduledExecutorService leaderEventOperationExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        try (final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        driverFactory,
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        leaderEventOperationExecutor)) {

            final AtomicBoolean firstContenderReceivedGrant = new AtomicBoolean(false);
            final LeaderContender firstContender =
                    TestingGenericLeaderContender.newBuilder()
                            .setGrantLeadershipConsumer(
                                    ignoredSessionID -> firstContenderReceivedGrant.set(true))
                            .build();

            final AtomicBoolean secondContenderReceivedGrant = new AtomicBoolean(false);
            final LeaderContender secondContender =
                    TestingGenericLeaderContender.newBuilder()
                            .setGrantLeadershipConsumer(
                                    ignoredSessionID -> secondContenderReceivedGrant.set(true))
                            .build();
            try (final LeaderElection firstLeaderElection =
                    leaderElectionService.createLeaderElection("component_id_0")) {
                firstLeaderElection.startLeaderElection(firstContender);

                assertThat(driverFactory.getCreatedDriverCount())
                        .as(
                                "A single driver should have been created when registering the contender.")
                        .isEqualTo(1);
                leaderElectionService.onGrantLeadership(UUID.randomUUID());
                assertThat(firstContenderReceivedGrant).isFalse();

                try (final LeaderElection secondLeaderElection =
                        leaderElectionService.createLeaderElection("component_id_1")) {
                    secondLeaderElection.startLeaderElection(secondContender);

                    assertThat(secondContenderReceivedGrant).isFalse();

                    leaderEventOperationExecutor.trigger();
                    assertThat(firstContenderReceivedGrant).isTrue();
                    assertThat(secondContenderReceivedGrant).isTrue();
                }
            }
        }
    }

    @Test
    void testDelayedGrantCallAfterContenderBeingDeregisteredAgain() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newNoOpBuilder());
        final ManuallyTriggeredScheduledExecutorService leaderEventOperationExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        try (final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(
                        driverFactory,
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        leaderEventOperationExecutor)) {

            final AtomicBoolean leadershipGrantForwardedToContender = new AtomicBoolean(false);
            final LeaderContender leaderContender =
                    TestingGenericLeaderContender.newBuilder()
                            .setGrantLeadershipConsumer(
                                    ignoredSessionID ->
                                            leadershipGrantForwardedToContender.set(true))
                            .build();
            try (final LeaderElection leaderElection =
                    leaderElectionService.createLeaderElection("component_id")) {
                leaderElection.startLeaderElection(leaderContender);

                assertThat(driverFactory.getCreatedDriverCount())
                        .as(
                                "A single driver should have been created when registering the contender.")
                        .isEqualTo(1);
                leaderElectionService.onGrantLeadership(UUID.randomUUID());
            }

            leaderEventOperationExecutor.trigger();
            assertThat(leadershipGrantForwardedToContender).isFalse();
        }
    }

    @Test
    void testDelayedRevokeCallAfterContenderBeingDeregisteredAgain() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            grantLeadership(expectedSessionID);
                            executorService.trigger();

                            final LeaderElection leaderElection =
                                    leaderElectionService.createLeaderElection(
                                            createRandomComponentId());

                            final AtomicInteger revokeCallCount = new AtomicInteger();
                            final LeaderContender contender =
                                    TestingGenericLeaderContender.newBuilder()
                                            .setRevokeLeadershipRunnable(
                                                    revokeCallCount::incrementAndGet)
                                            .build();
                            leaderElection.startLeaderElection(contender);
                            executorService.trigger();

                            assertThat(revokeCallCount)
                                    .as("No revocation should have been triggered, yet.")
                                    .hasValue(0);

                            revokeLeadership();

                            assertThat(revokeCallCount)
                                    .as("A revocation was triggered but not processed, yet.")
                                    .hasValue(0);

                            leaderElection.close();

                            assertThat(revokeCallCount)
                                    .as(
                                            "A revocation should have been triggered and immediately processed through the close call.")
                                    .hasValue(1);

                            executorService.triggerAll();

                            assertThat(revokeCallCount)
                                    .as(
                                            "The leadership revocation event that was triggered by the HA backend shouldn't have been forwarded to the contender, anymore.")
                                    .hasValue(1);
                        });
            }
        };
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

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .isEqualTo(leaderSessionID);
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isEqualTo(leaderSessionID);

                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isEqualTo(leaderSessionID);

                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forComponentId(ctx.componentId))
                                                .hasValue(
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address));

                                        ctx.leaderElection.close();

                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .as(
                                                        "The LeaderContender should have been informed about the leadership loss.")
                                                .isNull();
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .as(
                                                        "The LeaderElectionService should have its internal state cleaned.")
                                                .isNull();
                                    });

                            assertThat(storedLeaderInformation.get().getRegisteredComponentIds())
                                    .as("The HA backend's data should have been cleaned.")
                                    .isEmpty();
                        });
            }
        };
    }

    @Test
    void testSingleLeaderInformationChangedAndShouldBeCorrected() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            final LeaderInformation expectedLeaderInformation =
                                    LeaderInformation.known(
                                            leaderSessionID, contenderContext0.address);

                            // Leader information changed on external storage. It should be
                            // corrected.
                            storedLeaderInformation.set(LeaderInformationRegister.empty());
                            leaderElectionService.onLeaderInformationChange(
                                    contenderContext0.componentId, LeaderInformation.empty());
                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forComponentId(contenderContext0.componentId))
                                    .as("Removed leader information should have been reset.")
                                    .hasValue(expectedLeaderInformation);

                            final LeaderInformation faultyLeaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            storedLeaderInformation.set(
                                    LeaderInformationRegister.of(
                                            contenderContext0.componentId,
                                            faultyLeaderInformation));
                            leaderElectionService.onLeaderInformationChange(
                                    contenderContext0.componentId, faultyLeaderInformation);
                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forComponentId(contenderContext0.componentId))
                                    .as("Overwritten leader information should have been reset.")
                                    .hasValue(expectedLeaderInformation);
                        });
            }
        };
    }

    @Test
    void testAllLeaderInformationChangeEventWithPartialCorrection() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            final LeaderInformationRegister correctLeaderInformationRegister =
                                    storedLeaderInformation.get();
                            assertThat(correctLeaderInformationRegister.getRegisteredComponentIds())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.componentId,
                                            contenderContext1.componentId);

                            // change LeaderInformation partially on external storage
                            final String componentIdWithChange = contenderContext0.componentId;
                            final String componentIdWithoutChange = contenderContext1.componentId;
                            final LeaderInformationRegister
                                    partiallyChangedLeaderInformationRegister =
                                            LeaderInformationRegister.clear(
                                                    correctLeaderInformationRegister,
                                                    componentIdWithChange);
                            storedLeaderInformation.set(partiallyChangedLeaderInformationRegister);
                            leaderElectionService.onLeaderInformationChange(
                                    partiallyChangedLeaderInformationRegister);

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forComponentId(componentIdWithChange))
                                    .as("Removed leader information should have been reset.")
                                    .hasValue(
                                            correctLeaderInformationRegister.forComponentIdOrEmpty(
                                                    componentIdWithChange));

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forComponentId(componentIdWithoutChange))
                                    .hasValue(
                                            correctLeaderInformationRegister.forComponentIdOrEmpty(
                                                    componentIdWithoutChange));
                        });
            }
        };
    }

    @Test
    void testAllLeaderInformationChangeEventWithUnknownComponentId() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID leaderSessionID = UUID.randomUUID();
                            grantLeadership(leaderSessionID);

                            final LeaderInformationRegister correctLeaderInformationRegister =
                                    storedLeaderInformation.get();
                            assertThat(correctLeaderInformationRegister.getRegisteredComponentIds())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.componentId,
                                            contenderContext1.componentId);

                            // change LeaderInformation only affects an unregistered componentId
                            final String unknownComponentId = createRandomComponentId();
                            final LeaderInformationRegister
                                    partiallyChangedLeaderInformationRegister =
                                            LeaderInformationRegister.merge(
                                                    correctLeaderInformationRegister,
                                                    unknownComponentId,
                                                    LeaderInformation.known(
                                                            UUID.randomUUID(),
                                                            "address-for-" + unknownComponentId));
                            storedLeaderInformation.set(partiallyChangedLeaderInformationRegister);
                            leaderElectionService.onLeaderInformationChange(
                                    partiallyChangedLeaderInformationRegister);

                            assertThat(storedLeaderInformation.get())
                                    .as(
                                            "The HA backend shouldn't have been touched by the service.")
                                    .isSameAs(partiallyChangedLeaderInformationRegister);
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

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, expectedSessionID))
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, UUID.randomUUID()))
                                                .isFalse();
                                    });
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

                            applyToBothContenderContexts(
                                    ctx -> assertThat(ctx.contender.getLeaderSessionID()).isNull());

                            executorService.trigger();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .isEqualTo(expectedSessionID);

                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, expectedSessionID))
                                                .isTrue();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, UUID.randomUUID()))
                                                .isFalse();
                                    });
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
                            grantLeadership(expectedSessionID);
                            executorService.trigger();

                            revokeLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, expectedSessionID))
                                                .as(
                                                        "No operation should be handled anymore after the HA backend "
                                                                + "indicated leadership loss even if the onRevokeLeadership wasn't "
                                                                + "processed, yet, because some other process could have picked up "
                                                                + "the leadership in the meantime already based on the HA "
                                                                + "backend's decision.")
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, UUID.randomUUID()))
                                                .isFalse();
                                    });
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWithLeadershipLostAndRevokeEventProcessed() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            grantLeadership(expectedSessionID);
                            revokeLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, expectedSessionID))
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, UUID.randomUUID()))
                                                .isFalse();
                                    });
                        });
            }
        };
    }

    @Test
    void testHasLeadershipAfterLeaderElectionClose() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            grantLeadership(expectedSessionID);

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.leaderElection.close();

                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.componentId, expectedSessionID))
                                                .isFalse();
                                    });
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
                                            contenderContext0.componentId,
                                            differentLeaderInformation));
                            leaderElectionService.onLeaderInformationChange(
                                    contenderContext0.componentId, differentLeaderInformation);

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forComponentId(contenderContext0.componentId))
                                    .as("The external storage shouldn't have been changed.")
                                    .hasValue(differentLeaderInformation);
                        });
            }
        };
    }

    @Test
    void testOnGrantLeadershipIsIgnoredAfterLeaderElectionClose() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            closeLeaderElectionInBothContexts();
                            grantLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .as(
                                                        "The grant event shouldn't have been processed by the LeaderElectionService.")
                                                .isNull();
                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .as(
                                                        "The grant event shouldn't have been forwarded to the contender.")
                                                .isNull();
                                    });
                        });
            }
        };
    }

    @Test
    void testOnLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingClosed() throws Exception {
        testLeadershipChangeEventHandlingBeingIgnoredAfterLeaderElectionClose(
                (listener, componentIds, externalStorage) ->
                        componentIds.forEach(
                                c ->
                                        listener.onLeaderInformationChange(
                                                c, externalStorage.forComponentIdOrEmpty(c))));
    }

    @Test
    void testAllLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingClosed() throws Exception {
        testLeadershipChangeEventHandlingBeingIgnoredAfterLeaderElectionClose(
                (listener, ignoredComponentIds, externalStorage) ->
                        listener.onLeaderInformationChange(externalStorage));
    }

    private void testLeadershipChangeEventHandlingBeingIgnoredAfterLeaderElectionClose(
            TriConsumer<LeaderElectionDriver.Listener, Iterable<String>, LeaderInformationRegister>
                    callback)
            throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            grantLeadership();

                            assertThat(storedLeaderInformation.get().getRegisteredComponentIds())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.componentId,
                                            contenderContext1.componentId);

                            contenderContext0.leaderElection.close();

                            // another contender adds its information to the external storage
                            // having additional data stored in the register helps to check whether
                            // the register was touched later on (the empty
                            // LeaderInformationRegister is implemented as a singleton which would
                            // prevent us from checking the identity of the external storage at the
                            // end of the test)
                            final String otherComponentId = createRandomComponentId();
                            final LeaderInformation otherLeaderInformation =
                                    LeaderInformation.known(
                                            UUID.randomUUID(), "address-for-" + otherComponentId);
                            final LeaderInformationRegister registerWithUnknownContender =
                                    LeaderInformationRegister.of(
                                            otherComponentId, otherLeaderInformation);
                            storedLeaderInformation.set(registerWithUnknownContender);
                            callback.accept(
                                    leaderElectionService,
                                    Arrays.asList(
                                            contenderContext0.componentId,
                                            contenderContext1.componentId),
                                    storedLeaderInformation.get());

                            final LeaderInformationRegister correctedExternalStorage =
                                    storedLeaderInformation.get();
                            assertThat(correctedExternalStorage.getRegisteredComponentIds())
                                    .as(
                                            "Only the still registered contender and the unknown one should have corrected its LeaderInformation.")
                                    .containsExactlyInAnyOrder(
                                            contenderContext1.componentId, otherComponentId);

                            contenderContext1.leaderElection.close();

                            final LeaderInformationRegister leftOverData =
                                    storedLeaderInformation.get();

                            callback.accept(
                                    leaderElectionService,
                                    Collections.singleton(contenderContext1.componentId),
                                    leftOverData);

                            assertThat(storedLeaderInformation.get().getRegisteredComponentIds())
                                    .as(
                                            "The following identity check does only make sense if we're not using an empty register.")
                                    .hasSize(1);
                            assertThat(storedLeaderInformation.get())
                                    .as("The external storage shouldn't have been touched.")
                                    .isSameAs(leftOverData);
                        });
            }
        };
    }

    @Test
    void testOnRevokeLeadershipIsTriggeredAfterLeaderElectionClose() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            grantLeadership();
                            final UUID oldSessionId =
                                    leaderElectionService.getLeaderSessionID(
                                            contenderContext0.componentId);

                            applyToBothContenderContexts(
                                    ctx -> {
                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .isEqualTo(oldSessionId);

                                        ctx.leaderElection.close();

                                        assertThat(ctx.contender.getLeaderSessionID())
                                                .as(
                                                        "LeaderContender should have been revoked as part of the close call.")
                                                .isNull();
                                    });
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
                            final UUID currentLeaderSessionId = UUID.randomUUID();
                            grantLeadership(currentLeaderSessionId);

                            final LeaderInformationRegister initiallyStoredData =
                                    storedLeaderInformation.get();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        final LeaderInformation expectedLeaderInformation =
                                                LeaderInformation.known(
                                                        currentLeaderSessionId, ctx.address);
                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forComponentId(ctx.componentId))
                                                .hasValue(expectedLeaderInformation);

                                        // Old confirm call should be ignored.
                                        ctx.leaderElection.confirmLeadership(
                                                UUID.randomUUID(), ctx.address);
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isEqualTo(currentLeaderSessionId);
                                    });

                            assertThat(storedLeaderInformation.get())
                                    .as(
                                            "The leader information in the external storage shouldn't have been updated.")
                                    .isSameAs(initiallyStoredData);
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
                            final UUID currentLeaderSessionId = UUID.randomUUID();
                            grantLeadership(currentLeaderSessionId);

                            revokeLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        // Old confirm call should be ignored.
                                        ctx.leaderElection.confirmLeadership(
                                                currentLeaderSessionId, ctx.address);

                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.componentId))
                                                .isNull();
                                    });
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

                            leaderElectionService.onError(testException);

                            applyToBothContenderContexts(
                                    contenderContext -> {
                                        assertThat(contenderContext.contender.getError())
                                                .isNotNull()
                                                .hasCause(testException);

                                        contenderContext.contender.clearError();
                                    });
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
                            closeLeaderElectionInBothContexts();

                            final Exception testException = new Exception("test leader exception");

                            leaderElectionService.onError(testException);

                            applyToBothContenderContexts(
                                    ctx ->
                                            assertThat(ctx.contender.getError())
                                                    .as("No error should have been forwarded.")
                                                    .isNull());

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
    void testGrantDoesNotBlockNotifyLeaderInformationChange() throws Exception {
        testLeaderEventDoesNotBlockLeaderInformationChangeEventHandling(
                (listener, componentId, storedLeaderInformation) -> {
                    listener.onLeaderInformationChange(
                            componentId,
                            storedLeaderInformation.forComponentIdOrEmpty(componentId));
                });
    }

    @Test
    void testGrantDoesNotBlockNotifyAllKnownLeaderInformation() throws Exception {
        testLeaderEventDoesNotBlockLeaderInformationChangeEventHandling(
                (listener, componentId, storedLeaderInformation) -> {
                    listener.onLeaderInformationChange(storedLeaderInformation);
                });
    }

    private void testLeaderEventDoesNotBlockLeaderInformationChangeEventHandling(
            TriConsumer<LeaderElectionDriver.Listener, String, LeaderInformationRegister> callback)
            throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            grantLeadership();

                            final LeaderInformation changedLeaderInformation =
                                    LeaderInformation.known(
                                            UUID.randomUUID(), contenderContext0.address);
                            storedLeaderInformation.set(
                                    LeaderInformationRegister.of(
                                            contenderContext0.componentId,
                                            changedLeaderInformation));
                            callback.accept(
                                    leaderElectionService,
                                    contenderContext0.componentId,
                                    storedLeaderInformation.get());

                            assertThat(storedLeaderInformation.get().hasNoLeaderInformation())
                                    .as(
                                            "The blocked leadership grant event shouldn't have blocked the processing of the LeaderInformation change event.")
                                    .isTrue();
                        });
            }
        };
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
                    listener.onGrantLeadership(UUID.randomUUID());
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
                    listener.onGrantLeadership(UUID.randomUUID());

                    leadershipGranted.set(false);
                    // this call should not block the test execution
                    listener.onRevokeLeadership();
                });
    }

    private void testNonBlockingCall(
            Function<OneShotLatch, TestingGenericLeaderContender> contenderCreator,
            BiConsumer<AtomicBoolean, LeaderElectionDriver.Listener> listenerAction)
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

        final LeaderElection leaderElection =
                testInstance.createLeaderElection(createRandomComponentId());
        leaderElection.startLeaderElection(contender);

        listenerAction.accept(leadershipGranted, testInstance);

        latch.trigger();

        leaderElection.close();
        testInstance.close();
    }

    private static String createRandomComponentId() {
        return String.format("component-id-%s", UUID.randomUUID());
    }

    private class Context {

        private final TestingLeaderElectionDriver.Factory driverFactory;

        private final AtomicBoolean leadershipGranted;

        DefaultLeaderElectionService leaderElectionService;
        TestingLeaderElectionDriver testingLeaderElectionDriver;

        ContenderContext contenderContext0;
        ContenderContext contenderContext1;

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
            leaderElectionService.onGrantLeadership(leaderSessionID);
        }

        void revokeLeadership() {
            leadershipGranted.set(false);
            leaderElectionService.onRevokeLeadership();
        }

        void closeLeaderElectionInBothContexts() throws Exception {
            applyToBothContenderContexts(ctx -> ctx.leaderElection.close());
        }

        void applyToBothContenderContexts(ThrowingConsumer<ContenderContext, Exception> callback)
                throws Exception {
            callback.accept(contenderContext0);
            callback.accept(contenderContext1);
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

                try (final ContenderContext localContenderContext0 =
                                ContenderContext.create(0, leaderElectionService);
                        final ContenderContext localContenderContext1 =
                                ContenderContext.create(1, leaderElectionService)) {
                    this.contenderContext0 = localContenderContext0;
                    this.contenderContext1 = localContenderContext1;

                    testingLeaderElectionDriver = driverFactory.assertAndGetOnlyCreatedDriver();
                    testMethod.run();
                }
            } finally {
                if (testingLeaderElectionDriver != null) {
                    testingLeaderElectionDriver.close();
                }
            }
        }
    }

    /** Context for holding the per-contender information. */
    private static class ContenderContext implements AutoCloseable {

        private final String componentId;
        private final String address;
        private final TestingContender contender;
        private LeaderElection leaderElection;

        private static ContenderContext create(int id, LeaderElectionService leaderElectionService)
                throws Exception {
            // randomSuffix is added to ensure uniqueness even between tests
            final String randomSuffix = UUID.randomUUID().toString().substring(0, 4);
            final String componentId = String.format("component-id-%d-%s", id, randomSuffix);
            final String address = String.format("address-%d-%s", id, randomSuffix);

            final LeaderElection leaderElection =
                    leaderElectionService.createLeaderElection(componentId);
            final TestingContender contender = new TestingContender(address, leaderElection);
            contender.startLeaderElection();

            return new ContenderContext(componentId, address, contender, leaderElection);
        }

        private ContenderContext(
                String componentId,
                String address,
                TestingContender contender,
                LeaderElection leaderElection) {
            this.componentId = componentId;
            this.address = address;
            this.contender = contender;
            this.leaderElection = leaderElection;
        }

        @Override
        public void close() throws Exception {
            leaderElection.close();
            contender.throwErrorIfPresent();
        }
    }
}
