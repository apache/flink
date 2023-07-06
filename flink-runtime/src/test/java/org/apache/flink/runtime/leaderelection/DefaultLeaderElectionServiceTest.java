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
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.TriConsumer;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractComparableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                                        ctx.assertNextEventToBeLeadershipGrantWithSessionID()
                                                .isEqualTo(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
                                                .isEqualTo(leaderSessionID);

                                        final LeaderInformation
                                                expectedLeaderInformationInHaBackend =
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address);
                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forContenderID(ctx.contenderID))
                                                .as(
                                                        "The HA backend should have its leader information updated.")
                                                .hasValue(expectedLeaderInformationInHaBackend);
                                    });

                            revokeLeadership();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        final LeaderInformation
                                                expectedLeaderInformationInHaBackend =
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address);

                                        ctx.assertNextEventToBeLeadershipRevocation().isTrue();
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
                                                .isNull();

                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forContenderID(ctx.contenderID))
                                                .as(
                                                        "External storage is not touched by the leader session because the leadership is already lost.")
                                                .hasValue(expectedLeaderInformationInHaBackend);
                                    });
                        });
            }
        };
    }

    @Test
    void testErrorOnContenderIDReuse() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () ->
                                assertThatThrownBy(
                                                () ->
                                                        leaderElectionService.createLeaderElection(
                                                                contenderContext0.contenderID))
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

    @Test
    void testGrantCallWhileInstantiatingDriver() throws Exception {
        final UUID expectedLeaderSessionID = UUID.randomUUID();
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        listener -> {
                            listener.onGrantLeadership(expectedLeaderSessionID);
                            return TestingLeaderElectionDriver.newNoOpBuilder().build(listener);
                        },
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        Executors.newDirectExecutorService())) {
            testInstance.startLeaderElectionBackend();

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomContenderID());
            final BlockingQueue<LeaderElectionEvent> eventQueue = new ArrayBlockingQueue<>(1);
            final LeaderContender testingContender =
                    TestingLeaderContender.newBuilder(
                                    eventQueue,
                                    leaderElection,
                                    "unused-address",
                                    fatalErrorHandlerExtension.getTestingFatalErrorHandler()
                                            ::onFatalError)
                            .build();
            leaderElection.startLeaderElection(testingContender);

            final LeaderElectionEvent nextEvent = eventQueue.take();
            assertThat(nextEvent.isIsLeaderEvent()).isTrue();
            assertThat(nextEvent.asIsLeaderEvent().getLeaderSessionID())
                    .isEqualTo(expectedLeaderSessionID);

            leaderElection.close();
        }
    }

    @Test
    void testDelayedGrantCallAfterContenderRegistration() throws Exception {
        new Context() {
            {
                runTestWithManuallyTriggeredEvents(
                        executorService -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            grantLeadership(expectedSessionID);

                            final String anotherContenderID = createRandomContenderID();
                            try (LeaderElection anotherLeaderElection =
                                    leaderElectionService.createLeaderElection(
                                            anotherContenderID)) {
                                final Collection<LeaderElectionEvent> eventQueue =
                                        new ArrayList<>();
                                final LeaderContender leaderContender =
                                        TestingLeaderContender.newBuilder(
                                                        eventQueue,
                                                        anotherLeaderElection,
                                                        "another-address-for-" + anotherContenderID,
                                                        fatalErrorHandlerExtension
                                                                        .getTestingFatalErrorHandler()
                                                                ::onFatalError)
                                                .build();
                                anotherLeaderElection.startLeaderElection(leaderContender);

                                assertThat(eventQueue)
                                        .as(
                                                "Leadership grant was not forwarded to the contender, yet.")
                                        .isEmpty();

                                executorService.trigger();

                                assertThat(eventQueue)
                                        .as(
                                                "Leadership grant is actually forwarded to the service.")
                                        .hasSize(1);

                                final LeaderElectionEvent.IsLeaderEvent event =
                                        Iterables.getOnlyElement(eventQueue).asIsLeaderEvent();
                                assertThat(event.getLeaderSessionID()).isEqualTo(expectedSessionID);
                            }
                        });
            }
        };
    }

    @Test
    void testMatchingSessionIDBetweenDifferentContenders() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            leaderElectionService.onGrantLeadership(expectedSessionID);

                            applyToBothContenderContexts(
                                    ctx ->
                                            ctx.assertNextEventToBeLeadershipGrantWithSessionID()
                                                    .isEqualTo(expectedSessionID));
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
                            grantLeadership();

                            final Queue<LeaderElectionEvent> eventQueue = new LinkedList<>();
                            final String anotherContenderID = createRandomContenderID();
                            try (final LeaderElection newLeaderElection =
                                    leaderElectionService.createLeaderElection(
                                            anotherContenderID)) {
                                final LeaderContender newContender =
                                        TestingLeaderContender.newBuilder(
                                                        eventQueue,
                                                        newLeaderElection,
                                                        "another-address-for-" + anotherContenderID,
                                                        fatalErrorHandlerExtension
                                                                        .getTestingFatalErrorHandler()
                                                                ::onFatalError)
                                                .build();
                                newLeaderElection.startLeaderElection(newContender);
                            }

                            executorService.trigger();
                            assertThat(eventQueue).isEmpty();
                        });
            }
        };
    }

    @Test
    void testOnRevokeCallWhileClosingService() throws Exception {
        final AtomicBoolean leadershipGranted = new AtomicBoolean();
        final TestingLeaderElectionDriver.Builder driverBuilder =
                TestingLeaderElectionDriver.newBuilder(leadershipGranted);

        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(driverBuilder);
        try (final DefaultLeaderElectionService testInstance =
                new DefaultLeaderElectionService(
                        driverFactory,
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        Executors.newDirectExecutorService())) {
            driverBuilder.setCloseConsumer(lock -> testInstance.onRevokeLeadership());
            testInstance.startLeaderElectionBackend();

            final UUID expectedLeaderSessionID = UUID.randomUUID();
            leadershipGranted.set(true);
            testInstance.onGrantLeadership(expectedLeaderSessionID);

            final LeaderElection leaderElection =
                    testInstance.createLeaderElection(createRandomContenderID());

            final Queue<LeaderElectionEvent> eventQueue = new LinkedList<>();
            final LeaderContender contender =
                    TestingLeaderContender.newBuilder(
                                    eventQueue,
                                    fatalErrorHandlerExtension.getTestingFatalErrorHandler()
                                            ::onFatalError)
                            .build();
            leaderElection.startLeaderElection(contender);

            assertThat(eventQueue.remove().asIsLeaderEvent().getLeaderSessionID())
                    .as("The next event should have been the triggered leader acquisition.")
                    .isEqualTo(expectedLeaderSessionID);

            leaderElection.close();

            assertThat(eventQueue)
                    .as("No additional event should have been forwarded to the contender.")
                    .isEmpty();
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
                                    leaderElection.startLeaderElection(
                                            TestingLeaderContender.newBuilderForNoOpContender()
                                                    .build()))
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

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.assertNextEventToBeLeadershipGrantWithSessionID()
                                                .isEqualTo(leaderSessionID);
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
                                                .isEqualTo(leaderSessionID);

                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
                                                .isEqualTo(leaderSessionID);

                                        assertThat(
                                                        storedLeaderInformation
                                                                .get()
                                                                .forContenderID(ctx.contenderID))
                                                .hasValue(
                                                        LeaderInformation.known(
                                                                leaderSessionID, ctx.address));

                                        ctx.leaderElection.close();

                                        ctx.assertNextEventToBeLeadershipRevocation()
                                                .as(
                                                        "The LeaderContender should have been informed about the leadership loss.")
                                                .isTrue();

                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
                                                .as(
                                                        "The LeaderElectionService should have its internal state cleaned.")
                                                .isNull();
                                    });

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
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
                                    contenderContext0.contenderID, LeaderInformation.empty());
                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forContenderID(contenderContext0.contenderID))
                                    .as("Removed leader information should have been reset.")
                                    .hasValue(expectedLeaderInformation);

                            final LeaderInformation faultyLeaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            storedLeaderInformation.set(
                                    LeaderInformationRegister.of(
                                            contenderContext0.contenderID,
                                            faultyLeaderInformation));
                            leaderElectionService.onLeaderInformationChange(
                                    contenderContext0.contenderID, faultyLeaderInformation);
                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forContenderID(contenderContext0.contenderID))
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
                            assertThat(correctLeaderInformationRegister.getRegisteredContenderIDs())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID);

                            // change LeaderInformation partially on external storage
                            final String contenderIdWithChange = contenderContext0.contenderID;
                            final String contenderIdWithoutChange = contenderContext1.contenderID;
                            final LeaderInformationRegister
                                    partiallyChangedLeaderInformationRegister =
                                            LeaderInformationRegister.clear(
                                                    correctLeaderInformationRegister,
                                                    contenderIdWithChange);
                            storedLeaderInformation.set(partiallyChangedLeaderInformationRegister);
                            leaderElectionService.onLeaderInformationChange(
                                    partiallyChangedLeaderInformationRegister);

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forContenderID(contenderIdWithChange))
                                    .as("Removed leader information should have been reset.")
                                    .hasValue(
                                            correctLeaderInformationRegister.forContenderIdOrEmpty(
                                                    contenderIdWithChange));

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forContenderID(contenderIdWithoutChange))
                                    .hasValue(
                                            correctLeaderInformationRegister.forContenderIdOrEmpty(
                                                    contenderIdWithoutChange));
                        });
            }
        };
    }

    @Test
    void testAllLeaderInformationChangeEventWithUnknownContenderID() throws Exception {
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
                            assertThat(correctLeaderInformationRegister.getRegisteredContenderIDs())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID);

                            // change LeaderInformation only affects an unregistered contenderID
                            final String unknownContenderID = createRandomContenderID();
                            final LeaderInformationRegister
                                    partiallyChangedLeaderInformationRegister =
                                            LeaderInformationRegister.merge(
                                                    correctLeaderInformationRegister,
                                                    unknownContenderID,
                                                    LeaderInformation.known(
                                                            UUID.randomUUID(),
                                                            "address-for-" + unknownContenderID));
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
                                                                ctx.contenderID, expectedSessionID))
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, UUID.randomUUID()))
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
                                    ctx -> assertThat(ctx.eventQueue).isEmpty());

                            executorService.trigger();

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.assertNextEventToBeLeadershipGrantWithSessionID()
                                                .isEqualTo(expectedSessionID);

                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, expectedSessionID))
                                                .isTrue();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, UUID.randomUUID()))
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
                                                                ctx.contenderID, expectedSessionID))
                                                .as(
                                                        "No operation should be handled anymore after the HA backend "
                                                                + "indicated leadership loss even if the onRevokeLeadership wasn't "
                                                                + "processed, yet, because some other process could have picked up "
                                                                + "the leadership in the meantime already based on the HA "
                                                                + "backend's decision.")
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, UUID.randomUUID()))
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
                                                                ctx.contenderID, expectedSessionID))
                                                .isFalse();
                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, UUID.randomUUID()))
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
                                                                ctx.contenderID, expectedSessionID))
                                                .isFalse();
                                    });
                        });
            }
        };
    }

    @Test
    void testNotifyAllKnownLeaderInformation() throws Exception {
        final AtomicReference<LeaderInformationRegister> storedLeaderInformation =
                new AtomicReference<>();
        new Context(storedLeaderInformation) {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            final UUID expectedSessionID = UUID.randomUUID();
                            grantLeadership(expectedSessionID);

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
                                    .as("All contenders should have been registered.")
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID);
                            contenderContext0
                                    .assertNextEventToBeLeadershipGrantWithSessionID()
                                    .isEqualTo(expectedSessionID);
                            contenderContext1
                                    .assertNextEventToBeLeadershipGrantWithSessionID()
                                    .isEqualTo(expectedSessionID);

                            final String notRegisteredContenderID = "unknown-contender-id";
                            final LeaderInformation notRegisteredLeaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "unknown-address");
                            final Map<String, LeaderInformation> records =
                                    Stream.of(
                                                    contenderContext0.contenderID,
                                                    contenderContext1.contenderID)
                                            .collect(
                                                    Collectors.toMap(
                                                            Function.identity(),
                                                            contenderID ->
                                                                    // add random leader information
                                                                    // to simulate an external
                                                                    // change
                                                                    LeaderInformation.known(
                                                                            UUID.randomUUID(),
                                                                            "other-address-for-"
                                                                                    + contenderID)));
                            // add the leader information of the contender which is not registered
                            final LeaderInformationRegister updatedLeaderInformation =
                                    LeaderInformationRegister.merge(
                                            new LeaderInformationRegister(records),
                                            notRegisteredContenderID,
                                            notRegisteredLeaderInformation);
                            // update the backend data
                            storedLeaderInformation.set(updatedLeaderInformation);

                            // trigger the notification of the external data change
                            leaderElectionService.onLeaderInformationChange(
                                    updatedLeaderInformation);

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
                                    .as("The entries in the HA backend shouldn't have changed.")
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID,
                                            notRegisteredContenderID);

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.leaderElection.close();

                                        assertThat(
                                                        leaderElectionService.hasLeadership(
                                                                ctx.contenderID, expectedSessionID))
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
                                            contenderContext0.contenderID,
                                            differentLeaderInformation));
                            leaderElectionService.onLeaderInformationChange(
                                    contenderContext0.contenderID, differentLeaderInformation);

                            assertThat(
                                            storedLeaderInformation
                                                    .get()
                                                    .forContenderID(contenderContext0.contenderID))
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
                                                                ctx.contenderID))
                                                .as(
                                                        "The grant event shouldn't have been processed by the LeaderElectionService.")
                                                .isNull();
                                        assertThat(ctx.eventQueue)
                                                .as(
                                                        "The grant event shouldn't have been forwarded to the contender.")
                                                .isEmpty();
                                    });
                        });
            }
        };
    }

    @Test
    void testOnLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingClosed() throws Exception {
        testLeadershipChangeEventHandlingBeingIgnoredAfterLeaderElectionClose(
                (listener, contenderIDs, externalStorage) ->
                        contenderIDs.forEach(
                                c ->
                                        listener.onLeaderInformationChange(
                                                c, externalStorage.forContenderIdOrEmpty(c))));
    }

    @Test
    void testAllLeaderInformationChangeIsIgnoredAfterLeaderElectionBeingClosed() throws Exception {
        testLeadershipChangeEventHandlingBeingIgnoredAfterLeaderElectionClose(
                (listener, ignoredContenderIDs, externalStorage) ->
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

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
                                    .containsExactlyInAnyOrder(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID);

                            contenderContext0.leaderElection.close();

                            // another contender adds its information to the external storage
                            // having additional data stored in the register helps to check whether
                            // the register was touched later on (the empty
                            // LeaderInformationRegister is implemented as a singleton which would
                            // prevent us from checking the identity of the external storage at the
                            // end of the test)
                            final String otherContenderID = createRandomContenderID();
                            final LeaderInformation otherLeaderInformation =
                                    LeaderInformation.known(
                                            UUID.randomUUID(), "address-for-" + otherContenderID);
                            final LeaderInformationRegister registerWithUnknownContender =
                                    LeaderInformationRegister.of(
                                            otherContenderID, otherLeaderInformation);
                            storedLeaderInformation.set(registerWithUnknownContender);
                            callback.accept(
                                    leaderElectionService,
                                    Arrays.asList(
                                            contenderContext0.contenderID,
                                            contenderContext1.contenderID),
                                    storedLeaderInformation.get());

                            final LeaderInformationRegister correctedExternalStorage =
                                    storedLeaderInformation.get();
                            assertThat(correctedExternalStorage.getRegisteredContenderIDs())
                                    .as(
                                            "Only the still registered contender and the unknown one should have corrected its LeaderInformation.")
                                    .containsExactlyInAnyOrder(
                                            contenderContext1.contenderID, otherContenderID);

                            contenderContext1.leaderElection.close();

                            final LeaderInformationRegister leftOverData =
                                    storedLeaderInformation.get();

                            callback.accept(
                                    leaderElectionService,
                                    Collections.singleton(contenderContext1.contenderID),
                                    leftOverData);

                            assertThat(storedLeaderInformation.get().getRegisteredContenderIDs())
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
                                            contenderContext0.contenderID);

                            applyToBothContenderContexts(
                                    ctx -> {
                                        ctx.assertNextEventToBeLeadershipGrantWithSessionID()
                                                .isEqualTo(oldSessionId);

                                        ctx.leaderElection.close();

                                        ctx.assertNextEventToBeLeadershipRevocation()
                                                .as(
                                                        "LeaderContender should have been revoked as part of the stop call.")
                                                .isTrue();
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
                                                                .forContenderID(ctx.contenderID))
                                                .hasValue(expectedLeaderInformation);

                                        // Old confirm call should be ignored.
                                        ctx.leaderElection.confirmLeadership(
                                                UUID.randomUUID(), ctx.address);
                                        assertThat(
                                                        leaderElectionService.getLeaderSessionID(
                                                                ctx.contenderID))
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
                                                                ctx.contenderID))
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
                                    ctx -> {
                                        assertThat(ctx.fatalErrorHandler.getException())
                                                .isInstanceOf(LeaderElectionException.class)
                                                .hasCause(testException);
                                        ctx.fatalErrorHandler.clearError();
                                    });
                        });
            }
        };
    }

    @Test
    void testErrorForwardedToFallbackErrorHandlerWithNoRegisteredContender() throws Exception {
        new Context() {
            {
                runTestWithSynchronousEventHandling(
                        () -> {
                            closeLeaderElectionInBothContexts();

                            final Exception testException = new Exception("test leader exception");

                            leaderElectionService.onError(testException);

                            applyToBothContenderContexts(
                                    ctx ->
                                            assertThat(ctx.fatalErrorHandler.getErrorFuture())
                                                    .as("No error should have been forwarded.")
                                                    .isNotDone());

                            assertThat(
                                            fatalErrorHandlerExtension
                                                    .getTestingFatalErrorHandler()
                                                    .getException())
                                    .isInstanceOf(LeaderElectionException.class)
                                    .hasCause(testException);

                            fatalErrorHandlerExtension.getTestingFatalErrorHandler().clearError();
                        });
            }
        };
    }

    @Test
    void testGrantDoesNotBlockNotifyLeaderInformationChange() throws Exception {
        testLeaderEventDoesNotBlockLeaderInformationChangeEventHandling(
                (listener, contenderID, storedLeaderInformation) -> {
                    listener.onLeaderInformationChange(
                            contenderID,
                            storedLeaderInformation.forContenderIdOrEmpty(contenderID));
                });
    }

    @Test
    void testGrantDoesNotBlockNotifyAllKnownLeaderInformation() throws Exception {
        testLeaderEventDoesNotBlockLeaderInformationChangeEventHandling(
                (listener, contenderID, storedLeaderInformation) -> {
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
                                            contenderContext0.contenderID,
                                            changedLeaderInformation));
                            callback.accept(
                                    leaderElectionService,
                                    contenderContext0.contenderID,
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
                        TestingLeaderContender.newBuilderForNoOpContender()
                                .setGrantLeadershipConsumer(
                                        (ignoredLock, ignoredSessionID) -> latch.awaitQuietly())
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
                        TestingLeaderContender.newBuilderForNoOpContender()
                                .setRevokeLeadershipConsumer(ignoredLock -> latch.awaitQuietly())
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
            Function<OneShotLatch, LeaderContender> contenderCreator,
            BiConsumer<AtomicBoolean, LeaderElectionDriver.Listener> listenerAction)
            throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final LeaderContender contender = contenderCreator.apply(latch);

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
                leaderElectionService.startLeaderElectionBackend();
                testingLeaderElectionDriver = driverFactory.assertAndGetOnlyCreatedDriver();

                try (final ContenderContext localContenderContext0 =
                                ContenderContext.create(0, leaderElectionService);
                        final ContenderContext localContenderContext1 =
                                ContenderContext.create(1, leaderElectionService)) {
                    this.contenderContext0 = localContenderContext0;
                    this.contenderContext1 = localContenderContext1;

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

        private final String contenderID;
        private final String address;
        private final LeaderElection leaderElection;
        private final BlockingQueue<LeaderElectionEvent> eventQueue;

        private final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

        private static ContenderContext create(int id, LeaderElectionService leaderElectionService)
                throws Exception {
            // randomSuffix is added to ensure uniqueness even between tests
            final String randomSuffix = UUID.randomUUID().toString().substring(0, 4);
            final String contenderID = String.format("contender-id-%d-%s", id, randomSuffix);
            final String address = String.format("address-%d-%s", id, randomSuffix);

            final LeaderElection leaderElection =
                    leaderElectionService.createLeaderElection(contenderID);

            return new ContenderContext(contenderID, address, leaderElection);
        }

        private ContenderContext(String contenderID, String address, LeaderElection leaderElection)
                throws Exception {
            this.contenderID = contenderID;
            this.address = address;
            this.leaderElection = leaderElection;

            this.eventQueue = new ArrayBlockingQueue<>(100);
            final LeaderContender contender =
                    TestingLeaderContender.newBuilder(
                                    eventQueue,
                                    leaderElection,
                                    address,
                                    fatalErrorHandler::onFatalError)
                            .build();
            this.leaderElection.startLeaderElection(contender);
        }

        private UUID getLeaderSessionIDOfNextEvent() throws InterruptedException {
            return this.eventQueue.take().asIsLeaderEvent().getLeaderSessionID();
        }

        private AbstractComparableAssert<?, UUID> assertNextEventToBeLeadershipGrantWithSessionID()
                throws InterruptedException {
            return assertThat(getLeaderSessionIDOfNextEvent());
        }

        private AbstractBooleanAssert<?> assertNextEventToBeLeadershipRevocation()
                throws InterruptedException {
            return assertThat(this.eventQueue.take().isNotLeaderEvent());
        }

        @Override
        public void close() throws Exception {
            leaderElection.close();
            fatalErrorHandler.rethrowError();
        }
    }
}
