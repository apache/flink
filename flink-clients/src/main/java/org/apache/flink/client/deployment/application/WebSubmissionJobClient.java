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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JobClient} that only allows asking for the job id of the job it is attached to.
 *
 * <p>This is used in web submission, where we do not want the Web UI to have jobs blocking threads
 * while waiting for their completion.
 */
@PublicEvolving
public class WebSubmissionJobClient implements JobClient {

    private final JobID jobId;

    public WebSubmissionJobClient(final JobID jobId) {
        this.jobId = checkNotNull(jobId);
    }

    @Override
    public JobID getJobID() {
        return jobId;
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus() {
        throw new FlinkRuntimeException(
                "The Job Status cannot be requested when in Web Submission.");
    }

    @Override
    public CompletableFuture<Void> cancel() {
        throw new FlinkRuntimeException(
                "Cancelling the job is not supported by the Job Client when in Web Submission.");
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
        throw new FlinkRuntimeException(
                "Stop with Savepoint is not supported by the Job Client when in Web Submission.");
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
        throw new FlinkRuntimeException(
                "A savepoint cannot be taken through the Job Client when in Web Submission.");
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators() {
        throw new FlinkRuntimeException(
                "The Accumulators cannot be fetched through the Job Client when in Web Submission.");
    }

    @Override
    public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
        throw new FlinkRuntimeException(
                "The Job Result cannot be fetched through the Job Client when in Web Submission.");
    }
}
