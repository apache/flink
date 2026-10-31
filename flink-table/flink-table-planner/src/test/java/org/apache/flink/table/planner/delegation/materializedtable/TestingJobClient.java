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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/** A {@link JobClient} with a controllable status. */
class TestingJobClient implements JobClient {

    private final JobID jobId;
    private final Supplier<CompletableFuture<JobStatus>> jobStatusRequest;
    private Supplier<CompletableFuture<Void>> cancelRequest =
            () -> CompletableFuture.completedFuture(null);
    private Function<String, CompletableFuture<String>> stopRequest =
            savepointDirectory ->
                    CompletableFuture.completedFuture(savepointDirectory + "/savepoint-1");
    private boolean cancelRequested;

    TestingJobClient(JobID jobId, JobStatus jobStatus) {
        this(jobId, CompletableFuture.completedFuture(jobStatus));
    }

    TestingJobClient(JobID jobId, CompletableFuture<JobStatus> jobStatus) {
        this.jobId = jobId;
        this.jobStatusRequest = () -> jobStatus;
    }

    TestingJobClient withCancelRequest(Supplier<CompletableFuture<Void>> cancelRequest) {
        this.cancelRequest = cancelRequest;
        return this;
    }

    TestingJobClient withStopRequest(Function<String, CompletableFuture<String>> stopRequest) {
        this.stopRequest = stopRequest;
        return this;
    }

    boolean isCancelRequested() {
        return cancelRequested;
    }

    @Override
    public JobID getJobID() {
        return jobId;
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus() {
        return jobStatusRequest.get();
    }

    @Override
    public CompletableFuture<Void> cancel() {
        cancelRequested = true;
        return cancelRequest.get();
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            boolean advanceToEndOfEventTime,
            @Nullable String savepointDirectory,
            SavepointFormatType formatType) {
        return stopRequest.apply(savepointDirectory);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String savepointDirectory, SavepointFormatType formatType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
        throw new UnsupportedOperationException();
    }
}
