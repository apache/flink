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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@code TestingJobResultStore} is a {@link JobResultStore} implementation that can be used in
 * tests.
 */
public class TestingJobResultStore implements JobResultStore {

    public static final JobResult DUMMY_JOB_RESULT = createSuccessfulJobResult(new JobID());

    public static JobResult createSuccessfulJobResult(JobID jobId) {
        return createJobResult(jobId, ApplicationStatus.SUCCEEDED);
    }

    public static JobResult createJobResult(JobID jobId, ApplicationStatus applicationStatus) {
        return new JobResult.Builder()
                .jobId(jobId)
                .applicationStatus(applicationStatus)
                .netRuntime(1)
                .build();
    }

    private final Function<JobResultEntry, CompletableFuture<Void>> createDirtyResultConsumer;
    private final Function<JobID, CompletableFuture<Void>> markResultAsCleanConsumer;

    private final Function<JobID, CompletableFuture<Boolean>> hasJobResultEntryFunction;
    private final Function<JobID, CompletableFuture<Boolean>> hasDirtyJobResultEntryFunction;
    private final Function<JobID, CompletableFuture<Boolean>> hasCleanJobResultEntryFunction;
    private final SupplierWithException<Set<JobResult>, ? extends IOException>
            getDirtyResultsSupplier;

    private TestingJobResultStore(
            Function<JobResultEntry, CompletableFuture<Void>> createDirtyResultConsumer,
            Function<JobID, CompletableFuture<Void>> markResultAsCleanConsumer,
            Function<JobID, CompletableFuture<Boolean>> hasJobResultEntryFunction,
            Function<JobID, CompletableFuture<Boolean>> hasDirtyJobResultEntryFunction,
            Function<JobID, CompletableFuture<Boolean>> hasCleanJobResultEntryFunction,
            SupplierWithException<Set<JobResult>, ? extends IOException> getDirtyResultsSupplier) {
        this.createDirtyResultConsumer = createDirtyResultConsumer;
        this.markResultAsCleanConsumer = markResultAsCleanConsumer;
        this.hasJobResultEntryFunction = hasJobResultEntryFunction;
        this.hasDirtyJobResultEntryFunction = hasDirtyJobResultEntryFunction;
        this.hasCleanJobResultEntryFunction = hasCleanJobResultEntryFunction;
        this.getDirtyResultsSupplier = getDirtyResultsSupplier;
    }

    @Override
    public CompletableFuture<Void> createDirtyResultAsync(JobResultEntry jobResultEntry) {
        return createDirtyResultConsumer.apply(jobResultEntry);
    }

    @Override
    public CompletableFuture<Void> markResultAsCleanAsync(JobID jobId) {
        return markResultAsCleanConsumer.apply(jobId);
    }

    @Override
    public CompletableFuture<Boolean> hasJobResultEntryAsync(JobID jobId) {
        return hasJobResultEntryFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<Boolean> hasDirtyJobResultEntryAsync(JobID jobId) {
        return hasDirtyJobResultEntryFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<Boolean> hasCleanJobResultEntryAsync(JobID jobId) {
        return hasCleanJobResultEntryFunction.apply(jobId);
    }

    @Override
    public Set<JobResult> getDirtyResults() throws IOException {
        return getDirtyResultsSupplier.get();
    }

    public static TestingJobResultStore.Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for instantiating {@code TestingJobResultStore} instances. */
    public static class Builder {

        private Function<JobResultEntry, CompletableFuture<Void>> createDirtyResultConsumer =
                jobResultEntry -> FutureUtils.completedVoidFuture();
        private Function<JobID, CompletableFuture<Void>> markResultAsCleanConsumer =
                jobID -> FutureUtils.completedVoidFuture();

        private Function<JobID, CompletableFuture<Boolean>> hasJobResultEntryFunction =
                jobID -> CompletableFuture.completedFuture(false);
        private Function<JobID, CompletableFuture<Boolean>> hasDirtyJobResultEntryFunction =
                jobID -> CompletableFuture.completedFuture(false);
        private Function<JobID, CompletableFuture<Boolean>> hasCleanJobResultEntryFunction =
                jobID -> CompletableFuture.completedFuture(false);

        private SupplierWithException<Set<JobResult>, ? extends IOException>
                getDirtyResultsSupplier = Collections::emptySet;

        public Builder withCreateDirtyResultConsumer(
                Function<JobResultEntry, CompletableFuture<Void>> createDirtyResultConsumer) {
            this.createDirtyResultConsumer = createDirtyResultConsumer;
            return this;
        }

        public Builder withMarkResultAsCleanConsumer(
                Function<JobID, CompletableFuture<Void>> markResultAsCleanConsumer) {
            this.markResultAsCleanConsumer = markResultAsCleanConsumer;
            return this;
        }

        public Builder withHasJobResultEntryFunction(
                Function<JobID, CompletableFuture<Boolean>> hasJobResultEntryFunction) {
            this.hasJobResultEntryFunction = hasJobResultEntryFunction;
            return this;
        }

        public Builder withHasDirtyJobResultEntryFunction(
                Function<JobID, CompletableFuture<Boolean>> hasDirtyJobResultEntryFunction) {
            this.hasDirtyJobResultEntryFunction = hasDirtyJobResultEntryFunction;
            return this;
        }

        public Builder withHasCleanJobResultEntryFunction(
                Function<JobID, CompletableFuture<Boolean>> hasCleanJobResultEntryFunction) {
            this.hasCleanJobResultEntryFunction = hasCleanJobResultEntryFunction;
            return this;
        }

        public Builder withGetDirtyResultsSupplier(
                SupplierWithException<Set<JobResult>, ? extends IOException>
                        getDirtyResultsSupplier) {
            this.getDirtyResultsSupplier = getDirtyResultsSupplier;
            return this;
        }

        public TestingJobResultStore build() {
            return new TestingJobResultStore(
                    createDirtyResultConsumer,
                    markResultAsCleanConsumer,
                    hasJobResultEntryFunction,
                    hasDirtyJobResultEntryFunction,
                    hasCleanJobResultEntryFunction,
                    getDirtyResultsSupplier);
        }
    }
}
