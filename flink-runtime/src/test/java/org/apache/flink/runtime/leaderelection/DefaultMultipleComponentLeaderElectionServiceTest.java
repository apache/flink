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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultMultipleComponentLeaderElectionService}. */
@ExtendWith(TestLoggerExtension.class)
class DefaultMultipleComponentLeaderElectionServiceTest {

    @RegisterExtension
    public final TestingFatalErrorHandlerExtension fatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @Test
    public void isLeaderInformsAllRegisteredLeaderElectionEventHandlers() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(leaderElectionDriver);

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

            leaderElectionDriver.grantLeadership();

            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                assertThat(eventListener.hasLeadership()).isTrue();
            }
        } finally {
            leaderElectionService.close();
        }
    }

    private DefaultMultipleComponentLeaderElectionService
            createDefaultMultiplexingLeaderElectionService(
                    TestingMultipleComponentLeaderElectionDriver leaderElectionDriver)
                    throws Exception {
        return new DefaultMultipleComponentLeaderElectionService(
                fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                new TestingMultipleComponentLeaderElectionDriverFactory(leaderElectionDriver),
                Executors.newDirectExecutorService());
    }

    @Test
    public void notLeaderInformsAllRegisteredLeaderElectionEventHandlers() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(leaderElectionDriver);

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

            leaderElectionDriver.grantLeadership();
            leaderElectionDriver.revokeLeadership();

            for (SimpleTestingLeaderElectionEventListener eventListener : eventListeners) {
                assertThat(eventListener.hasLeadership()).isFalse();
            }
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    public void unregisteredEventHandlersAreNotNotified() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();

        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(leaderElectionDriver);

        try {
            final SimpleTestingLeaderElectionEventListener leaderElectionEventHandler =
                    new SimpleTestingLeaderElectionEventListener();
            final String componentId = "foobar";
            leaderElectionService.registerLeaderElectionEventHandler(
                    componentId, leaderElectionEventHandler);
            leaderElectionService.unregisterLeaderElectionEventHandler(componentId);

            leaderElectionDriver.grantLeadership();

            assertThat(leaderElectionEventHandler.hasLeadership()).isFalse();
        } finally {
            leaderElectionService.close();
        }
    }

    @Test
    public void newlyRegisteredEventHandlersAreInformedAboutLeadership() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();
        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(leaderElectionDriver);

        try {
            leaderElectionDriver.grantLeadership();

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
    public void allKnownLeaderInformationCallsEventHandlers() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();
        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                createDefaultMultiplexingLeaderElectionService(leaderElectionDriver);

        try {
            leaderElectionDriver.grantLeadership();

            final Collection<Component> knownLeaderInformation = createComponents(3);
            final Collection<Component> unknownLeaderInformation = createComponents(2);

            registerLeaderElectionEventHandler(leaderElectionService, knownLeaderInformation);
            registerLeaderElectionEventHandler(leaderElectionService, unknownLeaderInformation);

            leaderElectionService.notifyAllKnownLeaderInformation(
                    knownLeaderInformation.stream()
                            .map(
                                    component ->
                                            LeaderInformationWithComponentId.create(
                                                    component.getComponentId(),
                                                    component.getLeaderInformation()))
                            .collect(Collectors.toList()));

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
    public void allKnownLeaderInformationDoesNotBlock() throws Exception {
        final TestingMultipleComponentLeaderElectionDriver leaderElectionDriver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();
        final DefaultMultipleComponentLeaderElectionService leaderElectionService =
                new DefaultMultipleComponentLeaderElectionService(
                        fatalErrorHandlerExtension.getTestingFatalErrorHandler(),
                        new TestingMultipleComponentLeaderElectionDriverFactory(
                                leaderElectionDriver),
                        java.util.concurrent.Executors.newSingleThreadScheduledExecutor());
        try {
            leaderElectionDriver.grantLeadership();

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
                    Collections.singleton(
                            LeaderInformationWithComponentId.create(
                                    knownLeaderInformationComponent,
                                    LeaderInformation.known(UUID.randomUUID(), "localhost"))));

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

        private boolean hasLeadership;

        @Nullable private LeaderInformation leaderInformation;

        SimpleTestingLeaderElectionEventListener() {
            hasLeadership = false;
            leaderInformation = null;
        }

        public boolean hasLeadership() {
            return hasLeadership;
        }

        @Override
        public void onGrantLeadership(UUID newLeaderSessionId) {
            hasLeadership = true;
        }

        @Override
        public void onRevokeLeadership() {
            hasLeadership = false;
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
    }
}
