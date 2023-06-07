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

import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultLeaderElectionTest {

    @Test
    void testContenderRegistration() throws Exception {
        final AtomicReference<LeaderContender> contenderRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRegisterConsumer(contenderRef::set)
                        .build();
        try (final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService)) {

            final LeaderContender contender = TestingGenericLeaderContender.newBuilder().build();
            testInstance.startLeaderElection(contender);

            assertThat(contenderRef.get()).isSameAs(contender);
        }
    }

    @Test
    void testContenderRegistrationNull() throws Exception {
        try (final DefaultLeaderElection testInstance =
                new DefaultLeaderElection(
                        TestingAbstractLeaderElectionService.newBuilder().build())) {
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
                                contender -> {
                                    throw expectedException;
                                })
                        .build();
        try (final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService)) {
            assertThatThrownBy(
                            () ->
                                    testInstance.startLeaderElection(
                                            TestingGenericLeaderContender.newBuilder().build()))
                    .isEqualTo(expectedException);
        }
    }

    @Test
    void testLeaderConfirmation() throws Exception {
        final AtomicReference<UUID> leaderSessionIDRef = new AtomicReference<>();
        final AtomicReference<String> leaderAddressRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setConfirmLeadershipConsumer(
                                (leaderSessionID, address) -> {
                                    leaderSessionIDRef.set(leaderSessionID);
                                    leaderAddressRef.set(address);
                                })
                        .build();
        try (final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService)) {

            final UUID expectedLeaderSessionID = UUID.randomUUID();
            final String expectedAddress = "random-address";
            testInstance.confirmLeadership(expectedLeaderSessionID, expectedAddress);

            assertThat(leaderSessionIDRef.get()).isEqualTo(expectedLeaderSessionID);
            assertThat(leaderAddressRef.get()).isEqualTo(expectedAddress);
        }
    }

    @Test
    void testClose() throws Exception {
        final CompletableFuture<LeaderContender> actualContender = new CompletableFuture<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRegisterConsumer(ignoredContender -> {})
                        .setRemoveConsumer(actualContender::complete)
                        .build();

        final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService);

        final LeaderContender contender = TestingGenericLeaderContender.newBuilder().build();
        testInstance.startLeaderElection(contender);
        testInstance.close();

        assertThat(actualContender).isCompletedWithValue(contender);
    }

    @Test
    void testCloseWithoutStart() throws Exception {
        final CompletableFuture<LeaderContender> actualContender = new CompletableFuture<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setRemoveConsumer(actualContender::complete)
                        .build();

        final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService);
        testInstance.close();

        assertThat(actualContender)
                .as(
                        "No removal should be triggered if there's no contender that need to be deregistered.")
                .isNotDone();
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
        final AtomicReference<UUID> leaderSessionIDRef = new AtomicReference<>();
        final AbstractLeaderElectionService parentService =
                TestingAbstractLeaderElectionService.newBuilder()
                        .setHasLeadershipFunction(
                                leaderSessionID -> {
                                    leaderSessionIDRef.set(leaderSessionID);
                                    return expectedReturnValue;
                                })
                        .build();
        try (final DefaultLeaderElection testInstance = new DefaultLeaderElection(parentService)) {

            final UUID expectedLeaderSessionID = UUID.randomUUID();
            assertThat(testInstance.hasLeadership(expectedLeaderSessionID))
                    .isEqualTo(expectedReturnValue);
            assertThat(leaderSessionIDRef.get()).isEqualTo(expectedLeaderSessionID);
        }
    }

    private static class TestingAbstractLeaderElectionService
            extends AbstractLeaderElectionService {

        private final ThrowingConsumer<LeaderContender, Exception> registerConsumer;
        private final Consumer<LeaderContender> removeConsumer;
        private final BiConsumer<UUID, String> confirmLeadershipConsumer;
        private final Function<UUID, Boolean> hasLeadershipFunction;

        private TestingAbstractLeaderElectionService(
                ThrowingConsumer<LeaderContender, Exception> registerConsumer,
                Consumer<LeaderContender> removeConsumer,
                BiConsumer<UUID, String> confirmLeadershipConsumer,
                Function<UUID, Boolean> hasLeadershipFunction) {
            super();

            this.registerConsumer = registerConsumer;
            this.removeConsumer = removeConsumer;
            this.confirmLeadershipConsumer = confirmLeadershipConsumer;
            this.hasLeadershipFunction = hasLeadershipFunction;
        }

        @Override
        protected void register(LeaderContender contender) throws Exception {
            registerConsumer.accept(contender);
        }

        @Override
        protected void remove(LeaderContender contender) {
            removeConsumer.accept(contender);
        }

        @Override
        protected void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
            confirmLeadershipConsumer.accept(leaderSessionID, leaderAddress);
        }

        @Override
        protected boolean hasLeadership(UUID leaderSessionId) {
            return hasLeadershipFunction.apply(leaderSessionId);
        }

        public static Builder newBuilder() {
            return new Builder()
                    .setRegisterConsumer(
                            contender -> {
                                throw new UnsupportedOperationException("register not supported");
                            })
                    .setRemoveConsumer(contender -> {})
                    .setConfirmLeadershipConsumer(
                            (leaderSessionID, address) -> {
                                throw new UnsupportedOperationException(
                                        "confirmLeadership not supported");
                            })
                    .setHasLeadershipFunction(
                            leaderSessionID -> {
                                throw new UnsupportedOperationException(
                                        "hasLeadership not supported");
                            });
        }

        private static class Builder {

            private ThrowingConsumer<LeaderContender, Exception> registerConsumer =
                    ignoredContender -> {};
            private Consumer<LeaderContender> removeConsumer;
            private BiConsumer<UUID, String> confirmLeadershipConsumer =
                    (ignoredSessionID, ignoredAddress) -> {};
            private Function<UUID, Boolean> hasLeadershipFunction = ignoredSessiondID -> false;

            private Builder() {}

            public Builder setRegisterConsumer(
                    ThrowingConsumer<LeaderContender, Exception> registerConsumer) {
                this.registerConsumer = registerConsumer;
                return this;
            }

            public Builder setRemoveConsumer(Consumer<LeaderContender> removeConsumer) {
                this.removeConsumer = removeConsumer;
                return this;
            }

            public Builder setConfirmLeadershipConsumer(
                    BiConsumer<UUID, String> confirmLeadershipConsumer) {
                this.confirmLeadershipConsumer = confirmLeadershipConsumer;
                return this;
            }

            public Builder setHasLeadershipFunction(Function<UUID, Boolean> hasLeadershipFunction) {
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
