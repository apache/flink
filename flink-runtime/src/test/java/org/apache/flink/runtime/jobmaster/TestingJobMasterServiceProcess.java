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

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Testing implementation of {@link JobMasterServiceProcess}. */
public class TestingJobMasterServiceProcess implements JobMasterServiceProcess {

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture;

    private final CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture;

    private final CompletableFuture<String> leaderAddressFuture;

    private final boolean isInitialized;

    private final CompletableFuture<Void> terminationFuture;

    private final boolean manualTerminationFutureCompletion;

    private TestingJobMasterServiceProcess(
            CompletableFuture<JobMasterGateway> jobMasterGatewayFuture,
            CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture,
            CompletableFuture<String> leaderAddressFuture,
            boolean isInitialized,
            CompletableFuture<Void> terminationFuture,
            boolean manualTerminationFutureCompletion) {
        this.jobMasterGatewayFuture = jobMasterGatewayFuture;
        this.jobManagerRunnerResultFuture = jobManagerRunnerResultFuture;
        this.leaderAddressFuture = leaderAddressFuture;
        this.isInitialized = isInitialized;
        this.terminationFuture = terminationFuture;
        this.manualTerminationFutureCompletion = manualTerminationFutureCompletion;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!manualTerminationFutureCompletion) {
            terminationFuture.complete(null);
        }

        return terminationFuture;
    }

    @Override
    public boolean isInitializedAndRunning() {
        return isInitialized && !terminationFuture.isDone();
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return jobMasterGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return jobManagerRunnerResultFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
                CompletableFuture.completedFuture(new TestingJobMasterGatewayBuilder().build());
        private CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture =
                new CompletableFuture<>();
        private CompletableFuture<String> leaderAddressFuture =
                CompletableFuture.completedFuture("foobar");
        private boolean isInitialized = true;
        @Nullable private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        private boolean manualTerminationFutureCompletion = false;

        public Builder setJobMasterGatewayFuture(
                CompletableFuture<JobMasterGateway> jobMasterGatewayFuture) {
            this.jobMasterGatewayFuture = jobMasterGatewayFuture;
            return this;
        }

        public Builder setJobManagerRunnerResultFuture(
                CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture) {
            this.jobManagerRunnerResultFuture = jobManagerRunnerResultFuture;
            return this;
        }

        public Builder setLeaderAddressFuture(CompletableFuture<String> leaderAddressFuture) {
            this.leaderAddressFuture = leaderAddressFuture;
            return this;
        }

        public Builder setIsInitialized(boolean isInitialized) {
            this.isInitialized = isInitialized;
            return this;
        }

        public Builder setTerminationFuture(@Nullable CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
            return this;
        }

        public Builder withManualTerminationFutureCompletion() {
            this.manualTerminationFutureCompletion = true;
            return this;
        }

        public TestingJobMasterServiceProcess build() {
            return new TestingJobMasterServiceProcess(
                    jobMasterGatewayFuture,
                    jobManagerRunnerResultFuture,
                    leaderAddressFuture,
                    isInitialized,
                    terminationFuture,
                    manualTerminationFutureCompletion);
        }
    }
}
