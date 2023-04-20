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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mini Dispatcher which is instantiated as the dispatcher component by the {@link
 * JobClusterEntrypoint}.
 *
 * <p>The mini dispatcher is initialized with a single {@link JobGraph} which it runs.
 *
 * <p>Depending on the {@link ClusterEntrypoint.ExecutionMode}, the mini dispatcher will directly
 * terminate after job completion if its execution mode is {@link
 * ClusterEntrypoint.ExecutionMode#DETACHED}.
 */
public class MiniDispatcher extends Dispatcher {

    private final JobClusterEntrypoint.ExecutionMode executionMode;
    private boolean jobCancelled = false;

    public MiniDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            DispatcherServices dispatcherServices,
            @Nullable JobGraph jobGraph,
            @Nullable JobResult recoveredDirtyJob,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            JobClusterEntrypoint.ExecutionMode executionMode)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                CollectionUtil.ofNullable(jobGraph),
                CollectionUtil.ofNullable(recoveredDirtyJob),
                dispatcherBootstrapFactory,
                dispatcherServices);

        this.executionMode = checkNotNull(executionMode);
    }

    @VisibleForTesting
    public MiniDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            DispatcherServices dispatcherServices,
            @Nullable JobGraph jobGraph,
            @Nullable JobResult recoveredDirtyJob,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory,
            JobClusterEntrypoint.ExecutionMode executionMode)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                CollectionUtil.ofNullable(jobGraph),
                CollectionUtil.ofNullable(recoveredDirtyJob),
                dispatcherBootstrapFactory,
                dispatcherServices,
                jobManagerRunnerRegistry,
                resourceCleanerFactory);

        this.executionMode = checkNotNull(executionMode);
    }

    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        final CompletableFuture<Acknowledge> acknowledgeCompletableFuture =
                super.submitJob(jobGraph, timeout);

        acknowledgeCompletableFuture.whenComplete(
                (Acknowledge ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        onFatalError(
                                new FlinkException(
                                        "Failed to submit job "
                                                + jobGraph.getJobID()
                                                + " in job mode.",
                                        throwable));
                    }
                });

        return acknowledgeCompletableFuture;
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        final CompletableFuture<JobResult> jobResultFuture = super.requestJobResult(jobId, timeout);

        if (executionMode == ClusterEntrypoint.ExecutionMode.NORMAL) {
            // terminate the MiniDispatcher once we served the first JobResult successfully
            jobResultFuture.thenAccept(
                    (JobResult result) -> {
                        ApplicationStatus status =
                                result.getSerializedThrowable().isPresent()
                                        ? ApplicationStatus.FAILED
                                        : ApplicationStatus.SUCCEEDED;

                        if (!ApplicationStatus.UNKNOWN.equals(result.getApplicationStatus())) {
                            log.info(
                                    "Shutting down cluster because someone retrieved the job result"
                                            + " and the status is globally terminal.");
                            shutDownFuture.complete(status);
                        }
                    });
        } else {
            log.info("Not shutting down cluster after someone retrieved the job result.");
        }

        return jobResultFuture;
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        jobCancelled = true;
        return super.cancelJob(jobId, timeout);
    }

    @Override
    protected void runPostJobGloballyTerminated(JobID jobId, JobStatus jobStatus) {
        super.runPostJobGloballyTerminated(jobId, jobStatus);

        if (jobCancelled || executionMode == ClusterEntrypoint.ExecutionMode.DETACHED) {
            // shut down if job is cancelled or we don't have to wait for the execution
            // result retrieval
            log.info(
                    "Shutting down cluster after job with state {}, jobCancelled: {}, executionMode: {}",
                    jobStatus,
                    jobCancelled,
                    executionMode);
            shutDownFuture.complete(ApplicationStatus.fromJobStatus(jobStatus));
        }
    }
}
