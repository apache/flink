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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/** Testing {@link JobLeaderIdService} implementation. */
public class TestingJobLeaderIdService implements JobLeaderIdService {
    private final Consumer<JobLeaderIdActions> startConsumer;
    private final Runnable stopRunnable;
    private final Runnable clearRunnable;
    private final Consumer<JobID> addJobConsumer;
    private final Consumer<JobID> removeJobConsumer;
    private final Function<JobID, Boolean> containsJobFunction;
    private final Function<JobID, CompletableFuture<JobMasterId>> getLeaderIdFunction;
    private final BiFunction<JobID, UUID, Boolean> isValidTimeoutFunction;

    private TestingJobLeaderIdService(
            Consumer<JobLeaderIdActions> startConsumer,
            Runnable stopRunnable,
            Runnable clearRunnable,
            Consumer<JobID> addJobConsumer,
            Consumer<JobID> removeJobConsumer,
            Function<JobID, Boolean> containsJobFunction,
            Function<JobID, CompletableFuture<JobMasterId>> getLeaderIdFunction,
            BiFunction<JobID, UUID, Boolean> isValidTimeoutFunction) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.clearRunnable = clearRunnable;
        this.addJobConsumer = addJobConsumer;
        this.removeJobConsumer = removeJobConsumer;
        this.containsJobFunction = containsJobFunction;
        this.getLeaderIdFunction = getLeaderIdFunction;
        this.isValidTimeoutFunction = isValidTimeoutFunction;
    }

    @Override
    public void start(JobLeaderIdActions initialJobLeaderIdActions) throws Exception {
        startConsumer.accept(initialJobLeaderIdActions);
    }

    @Override
    public void stop() throws Exception {
        stopRunnable.run();
    }

    @Override
    public void clear() throws Exception {
        clearRunnable.run();
    }

    @Override
    public void addJob(JobID jobId) {
        addJobConsumer.accept(jobId);
    }

    @Override
    public void removeJob(JobID jobId) {
        removeJobConsumer.accept(jobId);
    }

    @Override
    public boolean containsJob(JobID jobId) {
        return containsJobFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<JobMasterId> getLeaderId(JobID jobId) throws Exception {
        return getLeaderIdFunction.apply(jobId);
    }

    @Override
    public boolean isValidTimeout(JobID jobId, UUID timeoutId) {
        return isValidTimeoutFunction.apply(jobId, timeoutId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Consumer<JobLeaderIdActions> startConsumer = ignored -> {};
        private Runnable stopRunnable = () -> {};
        private Runnable clearRunnable = () -> {};
        private Consumer<JobID> addJobConsumer = ignored -> {};
        private Consumer<JobID> removeJobConsumer = ignored -> {};
        private Function<JobID, Boolean> containsJobFunction = ignored -> false;
        private Function<JobID, CompletableFuture<JobMasterId>> getLeaderIdFunction =
                ignored -> new CompletableFuture<>();
        private BiFunction<JobID, UUID, Boolean> isValidTimeoutFunction =
                (ignoredA, ignoredB) -> false;

        public Builder setStartConsumer(Consumer<JobLeaderIdActions> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(Runnable stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setClearRunnable(Runnable clearRunnable) {
            this.clearRunnable = clearRunnable;
            return this;
        }

        public Builder setAddJobConsumer(Consumer<JobID> addJobConsumer) {
            this.addJobConsumer = addJobConsumer;
            return this;
        }

        public Builder setRemoveJobConsumer(Consumer<JobID> removeJobConsumer) {
            this.removeJobConsumer = removeJobConsumer;
            return this;
        }

        public Builder setContainsJobFunction(Function<JobID, Boolean> containsJobFunction) {
            this.containsJobFunction = containsJobFunction;
            return this;
        }

        public Builder setGetLeaderIdFunction(
                Function<JobID, CompletableFuture<JobMasterId>> getLeaderIdFunction) {
            this.getLeaderIdFunction = getLeaderIdFunction;
            return this;
        }

        public Builder setIsValidTimeoutFunction(
                BiFunction<JobID, UUID, Boolean> isValidTimeoutFunction) {
            this.isValidTimeoutFunction = isValidTimeoutFunction;
            return this;
        }

        public TestingJobLeaderIdService build() {
            return new TestingJobLeaderIdService(
                    startConsumer,
                    stopRunnable,
                    clearRunnable,
                    addJobConsumer,
                    removeJobConsumer,
                    containsJobFunction,
                    getLeaderIdFunction,
                    isValidTimeoutFunction);
        }
    }
}
