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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Testing implementation of {@link JobMasterServiceProcess}. */
public class TestingJobMasterServiceProcess implements JobMasterServiceProcess {

    private final UUID leaderSessionId;
    private final Supplier<CompletableFuture<Void>> closeAsyncSupplier;
    private final Supplier<Boolean> isInitializedAndRunningSupplier;
    private final Supplier<CompletableFuture<JobMasterGateway>> getJobMasterGatewayFutureSupplier;
    private final Supplier<CompletableFuture<JobManagerRunnerResult>> getResultFutureSupplier;
    private final Supplier<CompletableFuture<String>> getLeaderAddressFutureSupplier;

    private TestingJobMasterServiceProcess(
            UUID leaderSessionId,
            Supplier<CompletableFuture<Void>> closeAsyncSupplier,
            Supplier<Boolean> isInitializedAndRunningSupplier,
            Supplier<CompletableFuture<JobMasterGateway>> getJobMasterGatewayFutureSupplier,
            Supplier<CompletableFuture<JobManagerRunnerResult>> getResultFutureSupplier,
            Supplier<CompletableFuture<String>> getLeaderAddressFutureSupplier) {
        this.leaderSessionId = leaderSessionId;
        this.closeAsyncSupplier = closeAsyncSupplier;
        this.isInitializedAndRunningSupplier = isInitializedAndRunningSupplier;
        this.getJobMasterGatewayFutureSupplier = getJobMasterGatewayFutureSupplier;
        this.getResultFutureSupplier = getResultFutureSupplier;
        this.getLeaderAddressFutureSupplier = getLeaderAddressFutureSupplier;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsyncSupplier.get();
    }

    @Override
    public UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    @Override
    public boolean isInitializedAndRunning() {
        return isInitializedAndRunningSupplier.get();
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return getJobMasterGatewayFutureSupplier.get();
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return getResultFutureSupplier.get();
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return getLeaderAddressFutureSupplier.get();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingJobMasterServiceProcess}. */
    public static final class Builder {

        private UUID leaderSessionId = UUID.randomUUID();
        private Supplier<CompletableFuture<Void>> closeAsyncSupplier = unsupportedOperation();
        private Supplier<Boolean> isInitializedAndRunningSupplier = unsupportedOperation();
        private Supplier<CompletableFuture<JobMasterGateway>> getJobMasterGatewayFutureSupplier =
                () ->
                        CompletableFuture.completedFuture(
                                new TestingJobMasterGatewayBuilder().build());
        private Supplier<CompletableFuture<JobManagerRunnerResult>> getResultFutureSupplier =
                CompletableFuture::new;
        private Supplier<CompletableFuture<String>> getLeaderAddressFutureSupplier =
                () -> CompletableFuture.completedFuture("leader address");

        private static <T> Supplier<T> unsupportedOperation() {
            return () -> {
                throw new UnsupportedOperationException();
            };
        }

        public Builder setCloseAsyncSupplier(Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
            this.closeAsyncSupplier = closeAsyncSupplier;
            return this;
        }

        public Builder setIsInitializedAndRunningSupplier(
                Supplier<Boolean> isInitializedAndRunningSupplier) {
            this.isInitializedAndRunningSupplier = isInitializedAndRunningSupplier;
            return this;
        }

        public Builder setGetJobMasterGatewayFutureSupplier(
                Supplier<CompletableFuture<JobMasterGateway>> getJobMasterGatewayFutureSupplier) {
            this.getJobMasterGatewayFutureSupplier = getJobMasterGatewayFutureSupplier;
            return this;
        }

        public Builder setGetResultFutureSupplier(
                Supplier<CompletableFuture<JobManagerRunnerResult>> getResultFutureSupplier) {
            this.getResultFutureSupplier = getResultFutureSupplier;
            return this;
        }

        public Builder setGetLeaderAddressFutureSupplier(
                Supplier<CompletableFuture<String>> getLeaderAddressFutureSupplier) {
            this.getLeaderAddressFutureSupplier = getLeaderAddressFutureSupplier;
            return this;
        }

        public Builder setLeaderSessionId(UUID leaderSessionId) {
            this.leaderSessionId = leaderSessionId;
            return this;
        }

        public TestingJobMasterServiceProcess build() {
            return new TestingJobMasterServiceProcess(
                    leaderSessionId,
                    closeAsyncSupplier,
                    isInitializedAndRunningSupplier,
                    getJobMasterGatewayFutureSupplier,
                    getResultFutureSupplier,
                    getLeaderAddressFutureSupplier);
        }
    }
}
