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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Default {@link JobMasterServiceProcess} which is responsible for creating and running a {@link
 * JobMasterService}. The process is responsible for receiving the signals from the {@link
 * JobMasterService} and to create the respective {@link JobManagerRunnerResult} from it.
 *
 * <p>The {@link JobMasterService} can be created asynchronously and the creation can also fail.
 * That is why the process needs to observe the creation operation and complete the {@link
 * #resultFuture} with an initialization failure.
 *
 * <p>The {@link #resultFuture} can be completed with the following values:
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link JobNotFinishedException} to signal that the job has not been completed by the {@link
 *       JobMasterService}
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
public class DefaultJobMasterServiceProcess
        implements JobMasterServiceProcess, OnCompletionActions {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobMasterServiceProcess.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final UUID leaderSessionId;

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
            new CompletableFuture<>();

    private final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean isRunning = true;

    public DefaultJobMasterServiceProcess(
            JobID jobId,
            UUID leaderSessionId,
            JobMasterServiceFactory jobMasterServiceFactory,
            Function<Throwable, ArchivedExecutionGraph> failedArchivedExecutionGraphFactory) {
        this.jobId = jobId;
        this.leaderSessionId = leaderSessionId;
        this.jobMasterServiceFuture =
                jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);

        jobMasterServiceFuture.whenComplete(
                (jobMasterService, throwable) -> {
                    if (throwable != null) {
                        final JobInitializationException jobInitializationException =
                                new JobInitializationException(
                                        jobId, "Could not start the JobMaster.", throwable);

                        LOG.debug(
                                "Initialization of the JobMasterService for job {} under leader id {} failed.",
                                jobId,
                                leaderSessionId,
                                jobInitializationException);

                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        new ExecutionGraphInfo(
                                                failedArchivedExecutionGraphFactory.apply(
                                                        jobInitializationException)),
                                        jobInitializationException));
                    } else {
                        registerJobMasterServiceFutures(jobMasterService);
                    }
                });
    }

    private void registerJobMasterServiceFutures(JobMasterService jobMasterService) {
        LOG.debug(
                "Successfully created the JobMasterService for job {} under leader id {}.",
                jobId,
                leaderSessionId);
        jobMasterGatewayFuture.complete(jobMasterService.getGateway());
        leaderAddressFuture.complete(jobMasterService.getAddress());

        jobMasterService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                if (isRunning) {
                                    LOG.warn(
                                            "Unexpected termination of the JobMasterService for job {} under leader id {}.",
                                            jobId,
                                            leaderSessionId);
                                    jobMasterFailed(
                                            new FlinkException(
                                                    "Unexpected termination of the JobMasterService.",
                                                    throwable));
                                }
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning) {
                isRunning = false;

                LOG.debug(
                        "Terminating the JobMasterService process for job {} under leader id {}.",
                        jobId,
                        leaderSessionId);

                resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException("Process has been closed."));

                jobMasterServiceFuture.whenComplete(
                        (jobMasterService, throwable) -> {
                            if (throwable != null) {
                                // JobMasterService creation has failed. Nothing to stop then :-)
                                terminationFuture.complete(null);
                            } else {
                                FutureUtils.forward(
                                        jobMasterService.closeAsync(), terminationFuture);
                            }
                        });

                terminationFuture.whenComplete(
                        (unused, throwable) ->
                                LOG.debug(
                                        "JobMasterService process for job {} under leader id {} has been terminated.",
                                        jobId,
                                        leaderSessionId));
            }
        }
        return terminationFuture;
    }

    @Override
    public boolean isInitializedAndRunning() {
        synchronized (lock) {
            return jobMasterServiceFuture.isDone()
                    && !jobMasterServiceFuture.isCompletedExceptionally()
                    && isRunning;
        }
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return jobMasterGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    @Override
    public void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo) {
        LOG.debug(
                "Job {} under leader id {} reached a globally terminal state {}.",
                jobId,
                leaderSessionId,
                executionGraphInfo.getArchivedExecutionGraph().getState());
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraphInfo));
    }

    @Override
    public void jobMasterFailed(Throwable cause) {
        LOG.debug("Job {} under leader id {} failed.", jobId, leaderSessionId);
        resultFuture.completeExceptionally(cause);
    }
}
