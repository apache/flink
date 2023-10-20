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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.dispatcher.UnavailableDispatcherOperationException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * {@code CheckpointResourcesCleanupRunner} implements {@link JobManagerRunner} in a way, that only
 * the checkpoint-related resources are instantiated. It triggers any job-specific cleanup that's
 * usually performed by the {@link JobMaster} without rebuilding the corresponding {@link
 * org.apache.flink.runtime.executiongraph.ExecutionGraph}.
 */
public class CheckpointResourcesCleanupRunner implements JobManagerRunner {

    private static final Logger LOG =
            LoggerFactory.getLogger(CheckpointResourcesCleanupRunner.class);

    private final JobResult jobResult;
    private final CheckpointRecoveryFactory checkpointRecoveryFactory;
    private final CheckpointsCleaner checkpointsCleaner;
    private final SharedStateRegistryFactory sharedStateRegistryFactory;
    private final Configuration jobManagerConfiguration;
    private final Executor cleanupExecutor;

    private final long initializationTimestamp;

    private final CompletableFuture<Void> cleanupFuture;
    private final CompletableFuture<JobManagerRunnerResult> resultFuture;

    public CheckpointResourcesCleanupRunner(
            JobResult jobResult,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Configuration jobManagerConfiguration,
            Executor cleanupExecutor,
            long initializationTimestamp) {
        this.jobResult = Preconditions.checkNotNull(jobResult);
        this.checkpointRecoveryFactory = Preconditions.checkNotNull(checkpointRecoveryFactory);
        this.sharedStateRegistryFactory = Preconditions.checkNotNull(sharedStateRegistryFactory);
        this.jobManagerConfiguration = Preconditions.checkNotNull(jobManagerConfiguration);
        this.cleanupExecutor = Preconditions.checkNotNull(cleanupExecutor);
        this.initializationTimestamp = initializationTimestamp;

        this.checkpointsCleaner =
                new CheckpointsCleaner(
                        jobManagerConfiguration.getBoolean(
                                CheckpointingOptions.CLEANER_PARALLEL_MODE));

        this.resultFuture = new CompletableFuture<>();
        this.cleanupFuture = resultFuture.thenCompose(ignored -> runCleanupAsync());
    }

    private CompletableFuture<Void> runCleanupAsync() {
        return CompletableFuture.runAsync(
                        () -> {
                            try {
                                cleanupCheckpoints();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        },
                        cleanupExecutor)
                .thenCompose(ignore -> checkpointsCleaner.closeAsync());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return cleanupFuture;
    }

    @Override
    public void start() throws Exception {
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(createExecutionGraphInfoFromJobResult()));
    }

    private void cleanupCheckpoints() throws Exception {
        final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore();
        final CheckpointIDCounter checkpointIDCounter = createCheckpointIDCounter();

        Exception exception = null;
        try {
            completedCheckpointStore.shutdown(getJobStatus(), checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIDCounter.shutdown(getJobStatus()).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    private CompletedCheckpointStore createCompletedCheckpointStore() throws Exception {
        return checkpointRecoveryFactory.createRecoveredCompletedCheckpointStore(
                getJobID(),
                DefaultCompletedCheckpointStoreUtils.getMaximumNumberOfRetainedCheckpoints(
                        jobManagerConfiguration, LOG),
                sharedStateRegistryFactory,
                cleanupExecutor,
                // Using RestoreMode.CLAIM to be able to discard shared state, if any.
                // Note that it also means that the original shared state might be discarded as well
                // because the initial checkpoint might be subsumed.
                RestoreMode.CLAIM);
    }

    private CheckpointIDCounter createCheckpointIDCounter() throws Exception {
        return checkpointRecoveryFactory.createCheckpointIDCounter(getJobID());
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        return FutureUtils.completedExceptionally(
                new UnavailableDispatcherOperationException(
                        "Unable to get JobMasterGateway for job in cleanup phase. The requested operation is not available in that stage."));
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobResult.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        return FutureUtils.completedExceptionally(
                new JobCancellationFailedException("Cleanup tasks are not meant to be cancelled."));
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return CompletableFuture.completedFuture(getJobStatus());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        return CompletableFuture.completedFuture(createExecutionGraphInfoFromJobResult());
    }

    @Override
    public boolean isInitialized() {
        return true;
    }

    private ExecutionGraphInfo createExecutionGraphInfoFromJobResult() {
        return generateExecutionGraphInfo(jobResult, initializationTimestamp);
    }

    private JobStatus getJobStatus() {
        return getJobStatus(jobResult);
    }

    private static JobStatus getJobStatus(JobResult jobResult) {
        return jobResult.getApplicationStatus().deriveJobStatus();
    }

    private static ExecutionGraphInfo generateExecutionGraphInfo(
            JobResult jobResult, long initializationTimestamp) {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobResult.getJobId(),
                        "unknown",
                        getJobStatus(jobResult),
                        jobResult.getSerializedThrowable().orElse(null),
                        null,
                        initializationTimestamp));
    }
}
