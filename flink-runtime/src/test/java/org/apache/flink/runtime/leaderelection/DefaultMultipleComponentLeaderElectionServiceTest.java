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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultMultipleComponentLeaderElectionService}. */
class DefaultMultipleComponentLeaderElectionServiceTest {

    @RegisterExtension
    final TestingFatalErrorHandlerExtension fatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @Test
    void isLeaderInformsAllRegisteredLeaderElectionEventHandlers() throws Exception {
        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService();

        try {
            final Collection<SimpleTestingLeaderElectionEventListener> eventListeners =
                    Stream.generate(SimpleTestingLeaderElectionEventListener::new)
                            .limit(4)
                            .collect(Collectors.toList());

            int counter = 0;
            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                leaderElectionService.registerLeaderElectionEventHandler(
                        String.valueOf(counter), eventListener);
                counter++;
            }

            leaderElectionService.isLeader(UUID.randomUUID());

            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                assertThat(eventListener.hasLeadership()).isTrue();
            }
        } finally {
            leaderElectionService.close();
        }
    }

    private DefaultMultipleComponentLeaderElectionService
            createDefaultMultiplexingLeaderElectionService() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));
        return createDefaultMultiplexingLeaderElectionService(driverFactory);
    }

    private DefaultMultipleComponentLeaderElectionService
            createDefaultMultiplexingLeaderElectionService(
                    TestingLeaderElectionDriver.Factory driverFactory) throws Exception {
        return new DefaultMultipleComponentLeaderElectionService(
                fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                driverFactory,
                Executors.newDirectExecutorService());
    }

    @Test
    void notLeaderInformsAllRegisteredLeaderElectionEventHandlers() throws Exception {
        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService();

        try {
            final Collection<SimpleTestingLeaderElectionEventListener> eventListeners =
                    Stream.generate(SimpleTestingLeaderElectionEventListener::new)
                            .limit(4)
                            .collect(Collectors.toList());

            int counter = 0;
            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                leaderElectionService.registerLeaderElectionEventHandler(
                        String.valueOf(counter), eventListener);
                counter++;
            }

            leaderElectionService.isLeader(UUID.randomUUID());
            leaderElectionService.notLeader();

            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                assertThat(eventListener.hasLeadership()).isFalse();
            }
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    void handleFatalError() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(driverFactory);
        final TestingLeaderElectionDriver leaderElectionDriver =
                driverFactory.assertAndGetOnlyCreatedDriver();

        try {
            final Throwable expectedFatalError =
                    new Exception("Expected exception simulating a fatal error.");

            leaderElectionDriver.triggerFatalError(expectedFatalError);

            FlinkAssertions.assertThatFuture(
                            fatalErrorHandlerExtension
                                    .getTestingFatalErrorHandler()
                                    .getErrorFuture())
                    .eventuallySucceeds()
                    .isEqualTo(expectedFatalError);
        } finally {
            leaderElectionService.close();
            fatalErrorHandlerExtension.getTestingFatalErrorHandler().clearError();
        }
    }

    @Test
    void unregisteredEventHandlersAreNotNotified() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(driverFactory);

        try {
            final SimpleTestingLeaderElectionEventListener leaderElectionEventHandler =
                    new SimpleTestingLeaderElectionEventListener();
            final String componentId = "foobar";
            leaderElectionService.registerLeaderElectionEventHandler(
                    componentId, leaderElectionEventHandler);
            leaderElectionService.unregisterLeaderElectionEventHandler(componentId);

            leaderElectionService.isLeader(UUID.randomUUID());

            assertThat(leaderElectionEventHandler.hasLeadership()).isFalse();
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    void newlyRegisteredEventHandlersAreInformedAboutLeadership() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(driverFactory);

        try {
            leaderElectionService.isLeader(UUID.randomUUID());

            final SimpleTestingLeaderElectionEventListener leaderElectionEventHandler =
                    new SimpleTestingLeaderElectionEventListener();
            leaderElectionService.registerLeaderElectionEventHandler(
                    "foobar", leaderElectionEventHandler);

            assertThat(leaderElectionEventHandler.hasLeadership()).isTrue();
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    public void testLeaderSessionIdMatchesBetweenComponents() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(driverFactory);

        try {
            final Component preLeadershipGrantedComponent =
                    new Component(
                            "test-component-0",
                            new SimpleTestingLeaderElectionEventListener(),
                            LeaderInformation.empty());
            final Component postLeadershipGrantedComponent =
                    new Component(
                            "test-component-1",
                            new SimpleTestingLeaderElectionEventListener(),
                            LeaderInformation.empty());

            leaderElectionService.registerLeaderElectionEventHandler(
                    preLeadershipGrantedComponent.getComponentId(),
                    preLeadershipGrantedComponent.getLeaderElectionEventListener());

            leaderElectionService.isLeader(UUID.randomUUID());

            leaderElectionService.registerLeaderElectionEventHandler(
                    postLeadershipGrantedComponent.getComponentId(),
                    postLeadershipGrantedComponent.getLeaderElectionEventListener());

            final UUID preGrantLeaderInformation =
                    preLeadershipGrantedComponent
                            .getLeaderElectionEventListener()
                            .getLeaderSessionID();

            final UUID postGrantLeaderInformation =
                    postLeadershipGrantedComponent
                            .getLeaderElectionEventListener()
                            .getLeaderSessionID();

            assertThat(preGrantLeaderInformation).isEqualTo(postGrantLeaderInformation);
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    void allKnownLeaderInformationCallsEventHandlers() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(driverFactory);

        try {
            leaderElectionService.isLeader(UUID.randomUUID());

            final Collection<Component> knownLeaderInformation = createComponents(3);
            final Collection<Component> unknownLeaderInformation = createComponents(2);

            registerLeaderElectionEventHandler(leaderElectionService, knownLeaderInformation);
            registerLeaderElectionEventHandler(leaderElectionService, unknownLeaderInformation);

            leaderElectionService.notifyAllKnownLeaderInformation(
                    new LeaderInformationRegister(
                            knownLeaderInformation.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Component::getComponentId,
                                                    Component::getLeaderInformation))));

            for (Component component : knownLeaderInformation) {
                assertThat(component.getLeaderElectionEventListener().getLeaderInformation())
                        .isEqualTo(component.getLeaderInformation());
            }

            for (Component component : unknownLeaderInformation) {
                assertThat(component.getLeaderElectionEventListener().getLeaderInformation())
                        .isEqualTo(LeaderInformation.empty());
            }

        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    void allKnownLeaderInformationDoesNotBlock() throws Exception {
        final TestingLeaderElectionDriver.Factory driverFactory =
                new TestingLeaderElectionDriver.Factory(
                        TestingLeaderElectionDriver.newBuilder(new AtomicBoolean()));

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                new DefaultMultipleComponentLeaderElectionService(
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        driverFactory,
                        java.util.concurrent.Executors.newSingleThreadScheduledExecutor());

        try {
            leaderElectionService.isLeader(UUID.randomUUID());

            final String knownLeaderInformationComponent = "knownLeaderInformationComponent";
            final BlockingLeaderElectionEventHandler knownLeaderElectionEventHandler =
                    new BlockingLeaderElectionEventHandler();
            leaderElectionService.registerLeaderElectionEventHandler(
                    knownLeaderInformationComponent, knownLeaderElectionEventHandler);
            final BlockingLeaderElectionEventHandler unknownLeaderElectionEventHandler =
                    new BlockingLeaderElectionEventHandler();
            leaderElectionService.registerLeaderElectionEventHandler(
                    "unknownLeaderInformationComponent", unknownLeaderElectionEventHandler);

            // make sure that this call succeeds w/o blocking
            leaderElectionService.notifyAllKnownLeaderInformation(
                    LeaderInformationRegister.of(
                            knownLeaderInformationComponent,
                            LeaderInformation.known(UUID.randomUUID(), "localhost")));

            knownLeaderElectionEventHandler.unblock();
            unknownLeaderElectionEventHandler.unblock();
        } finally {
            leaderElectionService.close();
        }
    }

    private static final class BlockingLeaderElectionEventHandler
            implements LeaderElectionEventHandler {

        private final OneShotLatch waitingLatch = new OneShotLatch();

        @Override
        public void onGrantLeadership(UUID newLeaderSessionId) {}

        @Override
        public void onRevokeLeadership() {}

        @Override
        public void onLeaderInformationChange(LeaderInformation leaderInformation) {
            try {
                waitingLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtils.checkInterrupted(e);
            }
        }

        void unblock() {
            waitingLatch.trigger();
        }
    }

    private void registerLeaderElectionEventHandler(
            DefaultMultipleComponentLeaderElectionService leaderElectionService,
            Collection<Component> knownLeaderInformation) {
        for (Component component : knownLeaderInformation) {
            leaderElectionService.registerLeaderElectionEventHandler(
                    component.getComponentId(), component.getLeaderElectionEventListener());
        }
    }

    private Collection<Component> createComponents(int numberComponents) {
        final List<Component> result = new ArrayList<>();

        for (int i = 0; i < numberComponents; i++) {
            result.add(
                    new Component(
                            UUID.randomUUID().toString(),
                            new SimpleTestingLeaderElectionEventListener(),
                            LeaderInformation.known(UUID.randomUUID(), "localhost")));
        }

        return result;
    }

    private static final class Component {
        private final String componentId;
        private final SimpleTestingLeaderElectionEventListener leaderElectionEventListener;
        private final LeaderInformation leaderInformation;

        private Component(
                String componentId,
                SimpleTestingLeaderElectionEventListener leaderElectionEventListener,
                LeaderInformation leaderInformation) {
            this.componentId = componentId;
            this.leaderElectionEventListener = leaderElectionEventListener;
            this.leaderInformation = leaderInformation;
        }

        String getComponentId() {
            return componentId;
        }

        LeaderInformation getLeaderInformation() {
            return leaderInformation;
        }

        SimpleTestingLeaderElectionEventListener getLeaderElectionEventListener() {
            return leaderElectionEventListener;
        }
    }

    private static final class SimpleTestingLeaderElectionEventListener
            implements LeaderElectionEventHandler {

        /**
         * {@code currentLeaderSessionId} is set if the current LeaderElection client is select as
         * the leader by the HA backend.
         */
        @Nullable private UUID currentLeaderSessionId;

        /** {@code leaderInformation} is set if the leader information in the HA backend changes. */
        @Nullable private LeaderInformation leaderInformation;

        SimpleTestingLeaderElectionEventListener() {
            currentLeaderSessionId = null;
            leaderInformation = null;
        }

        public boolean hasLeadership() {
            return currentLeaderSessionId != null;
        }

        @Override
        public void onGrantLeadership(UUID newLeaderSessionId) {
            currentLeaderSessionId = newLeaderSessionId;
        }

        @Override
        public void onRevokeLeadership() {
            currentLeaderSessionId = null;
            leaderInformation = null;
        }

        @Override
        public void onLeaderInformationChange(LeaderInformation leaderInformation) {
            this.leaderInformation = leaderInformation;
        }

        @Nullable
        LeaderInformation getLeaderInformation() {
            return leaderInformation;
        }

        @Nullable
        UUID getLeaderSessionID() {
            return currentLeaderSessionId;
        }
    }
}
