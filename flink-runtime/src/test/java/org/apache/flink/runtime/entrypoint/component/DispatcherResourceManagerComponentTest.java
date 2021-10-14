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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.runner.TestingDispatcherRunner;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.willNotComplete;
import static org.junit.Assert.assertThat;

/** Tests for the {@link DispatcherResourceManagerComponent}. */
public class DispatcherResourceManagerComponentTest extends TestLogger {

    @Test
    public void unexpectedResourceManagerTermination_failsFatally() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        final TestingResourceManagerService resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setTerminationFuture(terminationFuture)
                        .build();

        createDispatcherResourceManagerComponent(fatalErrorHandler, resourceManagerService);

        final FlinkException expectedException = new FlinkException("Expected test exception.");

        terminationFuture.completeExceptionally(expectedException);

        final Throwable error = fatalErrorHandler.getException();
        assertThat(error, containsCause(expectedException));
    }

    private DispatcherResourceManagerComponent createDispatcherResourceManagerComponent(
            TestingFatalErrorHandler fatalErrorHandler,
            TestingResourceManagerService resourceManagerService) {
        return new DispatcherResourceManagerComponent(
                TestingDispatcherRunner.newBuilder().build(),
                resourceManagerService,
                new SettableLeaderRetrievalService(),
                new SettableLeaderRetrievalService(),
                FutureUtils::completedVoidFuture,
                fatalErrorHandler);
    }

    @Test
    public void unexpectedResourceManagerTermination_ifNotRunning_doesNotFailFatally() {
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final TestingResourceManagerService resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setTerminationFuture(terminationFuture)
                        .withManualTerminationFutureCompletion()
                        .build();

        final DispatcherResourceManagerComponent dispatcherResourceManagerComponent =
                createDispatcherResourceManagerComponent(fatalErrorHandler, resourceManagerService);

        dispatcherResourceManagerComponent.closeAsync();

        final FlinkException expectedException = new FlinkException("Expected test exception.");
        terminationFuture.completeExceptionally(expectedException);

        final CompletableFuture<Throwable> errorFuture = fatalErrorHandler.getErrorFuture();
        assertThat(errorFuture, willNotComplete(Duration.ofMillis(10L)));
    }

    /**
     * Testing implementation of {@link
     * org.apache.flink.runtime.resourcemanager.ResourceManagerService}, which does not actually
     * start the service internally.
     */
    private static class TestingResourceManagerService implements ResourceManagerService {
        private final CompletableFuture<Void> terminationFuture;
        private final boolean completeTerminationFutureOnClose;

        private TestingResourceManagerService(
                CompletableFuture<Void> terminationFuture,
                boolean completeTerminationFutureOnClose) {
            this.terminationFuture = terminationFuture;
            this.completeTerminationFutureOnClose = completeTerminationFutureOnClose;
        }

        @Override
        public void start() throws Exception {}

        @Override
        public CompletableFuture<Void> getTerminationFuture() {
            return terminationFuture;
        }

        @Override
        public CompletableFuture<Void> deregisterApplication(
                ApplicationStatus applicationStatus, @Nullable String diagnostics) {
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            if (completeTerminationFutureOnClose) {
                terminationFuture.complete(null);
            }
            return getTerminationFuture();
        }

        private static Builder newBuilder() {
            return new Builder();
        }

        private static class Builder {
            private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
            private boolean completeTerminationFutureOnClose = true;

            private Builder setTerminationFuture(CompletableFuture<Void> terminationFuture) {
                this.terminationFuture = terminationFuture;
                return this;
            }

            private Builder withManualTerminationFutureCompletion() {
                this.completeTerminationFutureOnClose = false;
                return this;
            }

            private TestingResourceManagerService build() {
                return new TestingResourceManagerService(
                        terminationFuture, completeTerminationFutureOnClose);
            }
        }
    }
}
