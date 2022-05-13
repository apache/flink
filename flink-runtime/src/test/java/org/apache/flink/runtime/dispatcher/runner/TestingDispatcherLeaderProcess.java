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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

class TestingDispatcherLeaderProcess implements DispatcherLeaderProcess {
    private final UUID leaderSessionId;

    private final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;
    private final Consumer<Void> startConsumer;
    private final Supplier<CompletableFuture<Void>> closeAsyncSupplier;
    private final CompletableFuture<String> confirmLeaderSessionFuture;
    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private CompletableFuture<Void> terminationFuture = null;

    private TestingDispatcherLeaderProcess(
            UUID leaderSessionId,
            CompletableFuture<DispatcherGateway> dispatcherGatewayFuture,
            Consumer<Void> startConsumer,
            Supplier<CompletableFuture<Void>> closeAsyncSupplier,
            CompletableFuture<String> confirmLeaderSessionFuture,
            CompletableFuture<ApplicationStatus> shutDownFuture) {
        this.leaderSessionId = leaderSessionId;
        this.dispatcherGatewayFuture = dispatcherGatewayFuture;
        this.startConsumer = startConsumer;
        this.closeAsyncSupplier = closeAsyncSupplier;
        this.confirmLeaderSessionFuture = confirmLeaderSessionFuture;
        this.shutDownFuture = shutDownFuture;
    }

    @Override
    public void start() {
        startConsumer.accept(null);
    }

    @Override
    public UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    @Override
    public CompletableFuture<DispatcherGateway> getDispatcherGateway() {
        return dispatcherGatewayFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return confirmLeaderSessionFuture;
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (terminationFuture == null) {
            terminationFuture = closeAsyncSupplier.get();
        }

        return terminationFuture;
    }

    public static Builder newBuilder(UUID leaderSessionId) {
        return new Builder(leaderSessionId);
    }

    public static class Builder {
        private final UUID leaderSessionId;

        private CompletableFuture<DispatcherGateway> dispatcherGatewayFuture =
                new CompletableFuture<>();

        private Consumer<Void> startConsumer = (ignored) -> {};

        private Supplier<CompletableFuture<Void>> closeAsyncSupplier =
                () -> CompletableFuture.completedFuture(null);

        private CompletableFuture<String> confirmLeaderSessionFuture =
                CompletableFuture.completedFuture("Unknown address");
        private CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();

        private Builder(UUID leaderSessionId) {
            this.leaderSessionId = leaderSessionId;
        }

        public Builder setDispatcherGatewayFuture(
                CompletableFuture<DispatcherGateway> dispatcherGatewayFuture) {
            this.dispatcherGatewayFuture = dispatcherGatewayFuture;
            return this;
        }

        public Builder setStartConsumer(Consumer<Void> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setCloseAsyncSupplier(Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
            this.closeAsyncSupplier = closeAsyncSupplier;
            return this;
        }

        public Builder setConfirmLeaderSessionFuture(
                CompletableFuture<String> confirmLeaderSessionFuture) {
            this.confirmLeaderSessionFuture = confirmLeaderSessionFuture;
            return this;
        }

        public Builder setShutDownFuture(CompletableFuture<ApplicationStatus> shutDownFuture) {
            this.shutDownFuture = shutDownFuture;
            return this;
        }

        public TestingDispatcherLeaderProcess build() {
            return new TestingDispatcherLeaderProcess(
                    leaderSessionId,
                    dispatcherGatewayFuture,
                    startConsumer,
                    closeAsyncSupplier,
                    confirmLeaderSessionFuture,
                    shutDownFuture);
        }
    }
}
