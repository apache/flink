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

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.TriConsumer;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultLeaderElectionTest {

    private static final String DEFAULT_TEST_CONTENDER_ID = "test-contender-id";

    @Test
    void testContenderRegistration() throws Exception {
        final AtomicReference<String> contenderIDRef = new AtomicReference<>();
        final AtomicReference<LeaderContender> contenderRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRegisterConsumer(
                                (actualContenderID, actualContender) -> {
                                    contenderIDRef.set(actualContenderID);
                                    contenderRef.set(actualContender);
                                })
                        .build();
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID)) {

            final LeaderContender contender =
                    TestingGenericLeaderContender.newBuilderForNoOpContender().build();
            testInstance.startLeaderElection(contender);

            assertThat(contenderIDRef).hasValue(DEFAULT_TEST_CONTENDER_ID);
            assertThat(contenderRef.get()).isSameAs(contender);
        }
    }

    @Test
    void testContenderRegistrationNull() throws Exception {
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(
                        TestingAbstractLeaderElectionService.newBuilder().build(),
                        DEFAULT_TEST_CONTENDER_ID)) {
            assertThatThrownBy(() -> testInstance.startLeaderElection(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void testContenderRegistrationFailure() throws Exception {
        final Exception expectedException =
                new Exception("Expected exception during contender registration.");
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRegisterConsumer(
                                (actualContenderID, actualContender) -> {
                                    throw expectedException;
                                })
                        .build();
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID)) {
            assertThatThrownBy(
                            () ->
                                    testInstance.startLeaderElection(
                                            TestingGenericLeaderContender
                                                    .newBuilderForNoOpContender()
                                                    .build()))
                    .isEqualTo(expectedException);
        }
    }

    @Test
    void testLeaderConfirmation() throws Exception {
        final AtomicReference<String> contenderIDRef = new AtomicReference<>();
        final AtomicReference<UUID> leaderSessionIDRef = new AtomicReference<>();
        final AtomicReference<String> leaderAddressRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setConfirmLeadershipConsumer(
                                (contenderID, leaderSessionID, address) -> {
                                    contenderIDRef.set(contenderID);
                                    leaderSessionIDRef.set(leaderSessionID);
                                    leaderAddressRef.set(address);
                                })
                        .build();
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID)) {

            final UUID expectedLeaderSessionID = UUID.randomUUID();
            final String expectedAddress = "random-address";
            testInstance.confirmLeadership(expectedLeaderSessionID, expectedAddress);

            assertThat(contenderIDRef).hasValue(DEFAULT_TEST_CONTENDER_ID);
            assertThat(leaderSessionIDRef).hasValue(expectedLeaderSessionID);
            assertThat(leaderAddressRef).hasValue(expectedAddress);
        }
    }

    @Test
    void testClose() throws Exception {
        final CompletableFuture<String> actualContenderID = new CompletableFuture<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRegisterConsumer((ignoredContenderID, ignoredContender) -> {})
                        .setRemoveConsumer(actualContenderID::complete)
                        .build();

        final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID);

        testInstance.startLeaderElection(
                TestingGenericLeaderContender.newBuilderForNoOpContender().build());
        testInstance.close();

        assertThat(actualContenderID).isCompletedWithValue(DEFAULT_TEST_CONTENDER_ID);
    }

    @Test
    void testCloseWithoutStart() throws Exception {
        final CompletableFuture<String> actualContenderID = new CompletableFuture<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRemoveConsumer(actualContenderID::complete)
                        .build();

        final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID);
        testInstance.close();

        assertThatFuture(actualContenderID)
                .eventuallySucceeds()
                .isEqualTo(DEFAULT_TEST_CONTENDER_ID);
    }

    @Test
    void testHasLeadershipTrue() throws Exception {
        testHasLeadership(true);
    }

    @Test
    void testHasLeadershipFalse() throws Exception {
        testHasLeadership(false);
    }

    private void testHasLeadership(boolean expectedReturnValue) throws Exception {
        final AtomicReference<String> contenderIDRef = new AtomicReference<>();
        final AtomicReference<UUID> leaderSessionIDRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setHasLeadershipFunction(
                                (actualContenderID, actualLeaderSessionID) -> {
                                    contenderIDRef.set(actualContenderID);
                                    leaderSessionIDRef.set(actualLeaderSessionID);
                                    return expectedReturnValue;
                                })
                        .build();
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(parentService, DEFAULT_TEST_CONTENDER_ID)) {

            final UUID expectedLeaderSessionID = UUID.randomUUID();
            assertThat(testInstance.hasLeadership(expectedLeaderSessionID))
                    .isEqualTo(expectedReturnValue);
            assertThat(contenderIDRef).hasValue(DEFAULT_TEST_CONTENDER_ID);
            assertThat(leaderSessionIDRef).hasValue(expectedLeaderSessionID);
        }
    }

    private static class TestingAbstractLeaderElectionService
            extends AbstractLeaderElectionService {

        private final BiConsumerWithException<String, LeaderContender, Exception> registerConsumer;
        private final Consumer<String> removeConsumer;
        private final TriConsumer<String, UUID, String> confirmLeadershipConsumer;
        private final BiFunction<String, UUID, Boolean> hasLeadershipFunction;

        private TestingAbstractLeaderElectionService(
                BiConsumerWithException<String, LeaderContender, Exception> registerConsumer,
                Consumer<String> removeConsumer,
                TriConsumer<String, UUID, String> confirmLeadershipConsumer,
                BiFunction<String, UUID, Boolean> hasLeadershipFunction) {
            super();

            this.registerConsumer = registerConsumer;
            this.removeConsumer = removeConsumer;
            this.confirmLeadershipConsumer = confirmLeadershipConsumer;
            this.hasLeadershipFunction = hasLeadershipFunction;
        }

        @Override
        protected void register(String contenderID, LeaderContender contender) throws Exception {
            registerConsumer.accept(contenderID, contender);
        }

        @Override
        protected void remove(String contenderID) {
            removeConsumer.accept(contenderID);
        }

        @Override
        protected void confirmLeadership(
                String contenderID, UUID leaderSessionID, String leaderAddress) {
            confirmLeadershipConsumer.accept(contenderID, leaderSessionID, leaderAddress);
        }

        @Override
        protected boolean hasLeadership(String contenderID, UUID leaderSessionId) {
            return hasLeadershipFunction.apply(contenderID, leaderSessionId);
        }

        public static Builder newBuilder() {
            return new Builder()
                    .setRegisterConsumer(
                            (contenderID, contender) -> {
                                throw new UnsupportedOperationException("register not supported");
                            })
                    .setRemoveConsumer(contenderID -> {})
                    .setConfirmLeadershipConsumer(
                            (contenderID, leaderSessionID, address) -> {
                                throw new UnsupportedOperationException(
                                        "confirmLeadership not supported");
                            })
                    .setHasLeadershipFunction(
                            (contenderID, leaderSessionID) -> {
                                throw new UnsupportedOperationException(
                                        "hasLeadership not supported");
                            });
        }

        private static class Builder {

            private BiConsumerWithException<String, LeaderContender, Exception> registerConsumer;
            private Consumer<String> removeConsumer;
            private TriConsumer<String, UUID, String> confirmLeadershipConsumer;
            private BiFunction<String, UUID, Boolean> hasLeadershipFunction;

            private Builder() {}

            public Builder setRegisterConsumer(
                    BiConsumerWithException<String, LeaderContender, Exception> registerConsumer) {
                this.registerConsumer = registerConsumer;
                return this;
            }

            public Builder setRemoveConsumer(Consumer<String> removeConsumer) {
                this.removeConsumer = removeConsumer;
                return this;
            }

            public Builder setConfirmLeadershipConsumer(
                    TriConsumer<String, UUID, String> confirmLeadershipConsumer) {
                this.confirmLeadershipConsumer = confirmLeadershipConsumer;
                return this;
            }

            public Builder setHasLeadershipFunction(
                    BiFunction<String, UUID, Boolean> hasLeadershipFunction) {
                this.hasLeadershipFunction = hasLeadershipFunction;
                return this;
            }

            public TestingAbstractLeaderElectionService build() {
                return new TestingAbstractLeaderElectionService(
                        registerConsumer,
                        removeConsumer,
                        confirmLeadershipConsumer,
                        hasLeadershipFunction);
            }
        }
    }
}
