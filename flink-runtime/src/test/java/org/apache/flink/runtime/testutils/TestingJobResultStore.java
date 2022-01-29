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
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

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

    private final ThrowingConsumer<JobResultEntry, ? extends IOException> createDirtyResultConsumer;
    private final ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer;
    private final FunctionWithException<JobID, Boolean, ? extends IOException>
            hasJobResultEntryFunction;
    private final FunctionWithException<JobID, Boolean, ? extends IOException>
            hasDirtyJobResultEntryFunction;
    private final FunctionWithException<JobID, Boolean, ? extends IOException>
            hasCleanJobResultEntryFunction;
    private final SupplierWithException<Set<JobResult>, ? extends IOException>
            getDirtyResultsSupplier;

    private TestingJobResultStore(
            ThrowingConsumer<JobResultEntry, ? extends IOException> createDirtyResultConsumer,
            ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer,
            FunctionWithException<JobID, Boolean, ? extends IOException> hasJobResultEntryFunction,
            FunctionWithException<JobID, Boolean, ? extends IOException>
                    hasDirtyJobResultEntryFunction,
            FunctionWithException<JobID, Boolean, ? extends IOException>
                    hasCleanJobResultEntryFunction,
            SupplierWithException<Set<JobResult>, ? extends IOException> getDirtyResultsSupplier) {
        this.createDirtyResultConsumer = createDirtyResultConsumer;
        this.markResultAsCleanConsumer = markResultAsCleanConsumer;
        this.hasJobResultEntryFunction = hasJobResultEntryFunction;
        this.hasDirtyJobResultEntryFunction = hasDirtyJobResultEntryFunction;
        this.hasCleanJobResultEntryFunction = hasCleanJobResultEntryFunction;
        this.getDirtyResultsSupplier = getDirtyResultsSupplier;
    }

    @Override
    public void createDirtyResult(JobResultEntry jobResultEntry) throws IOException {
        createDirtyResultConsumer.accept(jobResultEntry);
    }

    @Override
    public void markResultAsClean(JobID jobId) throws IOException {
        markResultAsCleanConsumer.accept(jobId);
    }

    @Override
    public boolean hasJobResultEntry(JobID jobId) throws IOException {
        return hasJobResultEntryFunction.apply(jobId);
    }

    @Override
    public boolean hasDirtyJobResultEntry(JobID jobId) throws IOException {
        return hasDirtyJobResultEntryFunction.apply(jobId);
    }

    @Override
    public boolean hasCleanJobResultEntry(JobID jobId) throws IOException {
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

        private ThrowingConsumer<JobResultEntry, ? extends IOException> createDirtyResultConsumer =
                ignored -> {};
        private ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer =
                ignored -> {};

        private FunctionWithException<JobID, Boolean, ? extends IOException>
                hasJobResultEntryFunction = ignored -> false;
        private FunctionWithException<JobID, Boolean, ? extends IOException>
                hasDirtyJobResultEntryFunction = ignored -> false;
        private FunctionWithException<JobID, Boolean, ? extends IOException>
                hasCleanJobResultEntryFunction = ignored -> false;

        private SupplierWithException<Set<JobResult>, ? extends IOException>
                getDirtyResultsSupplier = Collections::emptySet;

        public Builder withCreateDirtyResultConsumer(
                ThrowingConsumer<JobResultEntry, ? extends IOException> createDirtyResultConsumer) {
            this.createDirtyResultConsumer = createDirtyResultConsumer;
            return this;
        }

        public Builder withMarkResultAsCleanConsumer(
                ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer) {
            this.markResultAsCleanConsumer = markResultAsCleanConsumer;
            return this;
        }

        public Builder withHasJobResultEntryFunction(
                FunctionWithException<JobID, Boolean, ? extends IOException>
                        hasJobResultEntryFunction) {
            this.hasJobResultEntryFunction = hasJobResultEntryFunction;
            return this;
        }

        public Builder withHasDirtyJobResultEntryFunction(
                FunctionWithException<JobID, Boolean, ? extends IOException>
                        hasDirtyJobResultEntryFunction) {
            this.hasDirtyJobResultEntryFunction = hasDirtyJobResultEntryFunction;
            return this;
        }

        public Builder withHasCleanJobResultEntryFunction(
                FunctionWithException<JobID, Boolean, ? extends IOException>
                        hasCleanJobResultEntryFunction) {
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
