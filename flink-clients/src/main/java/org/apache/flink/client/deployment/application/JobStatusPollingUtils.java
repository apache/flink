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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Utilities for periodically polling the status of a job. */
class JobStatusPollingUtils {

    /**
     * Polls the {@link JobStatus} of a job periodically and when the job has reached a terminal
     * state, it requests its {@link JobResult}.
     *
     * @param dispatcherGateway the {@link DispatcherGateway} to be used for requesting the details
     *     of the job.
     * @param jobId the id of the job
     * @param scheduledExecutor the executor to be used to periodically request the status of the
     *     job
     * @param rpcTimeout the timeout of the rpc
     * @param retryPeriod the interval between two consecutive job status requests
     * @return a future that will contain the job's {@link JobResult}.
     */
    static CompletableFuture<JobResult> getJobResult(
            final DispatcherGateway dispatcherGateway,
            final JobID jobId,
            final ScheduledExecutor scheduledExecutor,
            final Time rpcTimeout,
            final Time retryPeriod) {

        return pollJobResultAsync(
                () -> dispatcherGateway.requestJobStatus(jobId, rpcTimeout),
                () -> dispatcherGateway.requestJobResult(jobId, rpcTimeout),
                scheduledExecutor,
                retryPeriod.toMilliseconds());
    }

    @VisibleForTesting
    static CompletableFuture<JobResult> pollJobResultAsync(
            final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
            final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
            final ScheduledExecutor scheduledExecutor,
            final long retryMsTimeout) {
        return pollJobResultAsync(
                jobStatusSupplier,
                jobResultSupplier,
                scheduledExecutor,
                new CompletableFuture<>(),
                retryMsTimeout,
                0);
    }

    private static CompletableFuture<JobResult> pollJobResultAsync(
            final Supplier<CompletableFuture<JobStatus>> jobStatusSupplier,
            final Supplier<CompletableFuture<JobResult>> jobResultSupplier,
            final ScheduledExecutor scheduledExecutor,
            final CompletableFuture<JobResult> resultFuture,
            final long retryMsTimeout,
            final long attempt) {

        jobStatusSupplier
                .get()
                .whenComplete(
                        (jobStatus, throwable) -> {
                            if (throwable != null) {
                                resultFuture.completeExceptionally(throwable);
                            } else {
                                if (jobStatus.isGloballyTerminalState()) {
                                    jobResultSupplier
                                            .get()
                                            .whenComplete(
                                                    (jobResult, t) -> {
                                                        if (t != null) {
                                                            resultFuture.completeExceptionally(t);
                                                        } else {
                                                            resultFuture.complete(jobResult);
                                                        }
                                                    });
                                } else {
                                    scheduledExecutor.schedule(
                                            () -> {
                                                pollJobResultAsync(
                                                        jobStatusSupplier,
                                                        jobResultSupplier,
                                                        scheduledExecutor,
                                                        resultFuture,
                                                        retryMsTimeout,
                                                        attempt + 1);
                                            },
                                            retryMsTimeout,
                                            TimeUnit.MILLISECONDS);
                                }
                            }
                        });

        return resultFuture;
    }
}
