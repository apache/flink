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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a {@link
 * Dispatcher}.
 */
public class SessionDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess
        implements JobGraphStore.JobGraphListener {

    private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

    private final JobGraphStore jobGraphStore;

    private final JobResultStore jobResultStore;

    private final Executor ioExecutor;

    private CompletableFuture<Void> onGoingRecoveryOperation = FutureUtils.completedVoidFuture();

    private SessionDispatcherLeaderProcess(
            UUID leaderSessionId,
            DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
            JobGraphStore jobGraphStore,
            JobResultStore jobResultStore,
            Executor ioExecutor,
            FatalErrorHandler fatalErrorHandler) {
        super(leaderSessionId, fatalErrorHandler);

        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.jobGraphStore = jobGraphStore;
        this.jobResultStore = jobResultStore;
        this.ioExecutor = ioExecutor;
    }

    @Override
    protected void onStart() {
        startServices();

        onGoingRecoveryOperation =
                createDispatcherBasedOnRecoveredJobGraphsAndRecoveredDirtyJobResults();
    }

    private void startServices() {
        try {
            jobGraphStore.start(this);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not start %s when trying to start the %s.",
                            jobGraphStore.getClass().getSimpleName(), getClass().getSimpleName()),
                    e);
        }
    }

    private void createDispatcherIfRunning(
            Collection<JobGraph> jobGraphs, Collection<JobResult> recoveredDirtyJobResults) {
        runIfStateIs(State.RUNNING, () -> createDispatcher(jobGraphs, recoveredDirtyJobResults));
    }

    private void createDispatcher(
            Collection<JobGraph> jobGraphs, Collection<JobResult> recoveredDirtyJobResults) {

        final DispatcherGatewayService dispatcherService =
                dispatcherGatewayServiceFactory.create(
                        DispatcherId.fromUuid(getLeaderSessionId()),
                        jobGraphs,
                        recoveredDirtyJobResults,
                        jobGraphStore,
                        jobResultStore);

        completeDispatcherSetup(dispatcherService);
    }

    private CompletableFuture<Void>
            createDispatcherBasedOnRecoveredJobGraphsAndRecoveredDirtyJobResults() {
        final CompletableFuture<Collection<JobResult>> dirtyJobsFuture =
                CompletableFuture.supplyAsync(this::getDirtyJobResultsIfRunning, ioExecutor);

        return dirtyJobsFuture
                .thenApplyAsync(
                        dirtyJobs ->
                                this.recoverJobsIfRunning(
                                        dirtyJobs.stream()
                                                .map(JobResult::getJobId)
                                                .collect(Collectors.toSet())),
                        ioExecutor)
                .thenAcceptBoth(dirtyJobsFuture, this::createDispatcherIfRunning)
                .handle(this::onErrorIfRunning);
    }

    private Collection<JobGraph> recoverJobsIfRunning(Set<JobID> recoveredDirtyJobResults) {
        return supplyUnsynchronizedIfRunning(() -> recoverJobs(recoveredDirtyJobResults))
                .orElse(Collections.emptyList());
    }

    private Collection<JobGraph> recoverJobs(Set<JobID> recoveredDirtyJobResults) {
        log.info("Recover all persisted job graphs that are not finished, yet.");
        final Collection<JobID> jobIds = getJobIds();
        final Collection<JobGraph> recoveredJobGraphs = new ArrayList<>();

        for (JobID jobId : jobIds) {
            if (!recoveredDirtyJobResults.contains(jobId)) {
                tryRecoverJob(jobId).ifPresent(recoveredJobGraphs::add);
            } else {
                log.info(
                        "Skipping recovery of a job with job id {}, because it already reached a globally terminal state",
                        jobId);
            }
        }

        log.info("Successfully recovered {} persisted job graphs.", recoveredJobGraphs.size());

        return recoveredJobGraphs;
    }

    private Collection<JobID> getJobIds() {
        try {
            return jobGraphStore.getJobIds();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not retrieve job ids of persisted jobs.", e);
        }
    }

    private Optional<JobGraph> tryRecoverJob(JobID jobId) {
        log.info("Trying to recover job with job id {}.", jobId);
        try {
            final JobGraph jobGraph = jobGraphStore.recoverJobGraph(jobId);
            if (jobGraph == null) {
                log.info(
                        "Skipping recovery of job with job id {}, because it already finished in a previous execution",
                        jobId);
            }
            return Optional.ofNullable(jobGraph);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Could not recover job with job id %s.", jobId), e);
        }
    }

    private Collection<JobResult> getDirtyJobResultsIfRunning() {
        return supplyUnsynchronizedIfRunning(this::getDirtyJobResults)
                .orElse(Collections.emptyList());
    }

    private Collection<JobResult> getDirtyJobResults() {
        try {
            return jobResultStore.getDirtyResults();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not retrieve JobResults of globally-terminated jobs from JobResultStore",
                    e);
        }
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        return CompletableFuture.runAsync(this::stopServices, ioExecutor);
    }

    private void stopServices() {
        try {
            jobGraphStore.stop();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    // ------------------------------------------------------------
    // JobGraphListener
    // ------------------------------------------------------------

    @Override
    public void onAddedJobGraph(JobID jobId) {
        runIfStateIs(State.RUNNING, () -> handleAddedJobGraph(jobId));
    }

    private void handleAddedJobGraph(JobID jobId) {
        log.debug(
                "Job {} has been added to the {} by another process.",
                jobId,
                jobGraphStore.getClass().getSimpleName());

        // serialize all ongoing recovery operations
        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenApplyAsync(ignored -> recoverJobIfRunning(jobId), ioExecutor)
                        .thenCompose(
                                optionalJobGraph ->
                                        optionalJobGraph
                                                .flatMap(this::submitAddedJobIfRunning)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> submitAddedJobIfRunning(JobGraph jobGraph) {
        return supplyIfRunning(() -> submitAddedJob(jobGraph));
    }

    private CompletableFuture<Void> submitAddedJob(JobGraph jobGraph) {
        final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

        return dispatcherGateway
                .submitJob(jobGraph, RpcUtils.INF_TIMEOUT)
                .thenApply(FunctionUtils.nullFn())
                .exceptionally(this::filterOutDuplicateJobSubmissionException);
    }

    private Void filterOutDuplicateJobSubmissionException(Throwable throwable) {
        final Throwable strippedException = ExceptionUtils.stripCompletionException(throwable);
        if (strippedException instanceof DuplicateJobSubmissionException) {
            final DuplicateJobSubmissionException duplicateJobSubmissionException =
                    (DuplicateJobSubmissionException) strippedException;

            log.debug(
                    "Ignore recovered job {} because the job is currently being executed.",
                    duplicateJobSubmissionException.getJobID(),
                    duplicateJobSubmissionException);

            return null;
        } else {
            throw new CompletionException(throwable);
        }
    }

    private DispatcherGateway getDispatcherGatewayInternal() {
        return Preconditions.checkNotNull(getDispatcherGateway().getNow(null));
    }

    private Optional<JobGraph> recoverJobIfRunning(JobID jobId) {
        return supplyUnsynchronizedIfRunning(() -> tryRecoverJob(jobId)).flatMap(x -> x);
    }

    @Override
    public void onRemovedJobGraph(JobID jobId) {
        runIfStateIs(State.RUNNING, () -> handleRemovedJobGraph(jobId));
    }

    private void handleRemovedJobGraph(JobID jobId) {
        log.debug(
                "Job {} has been removed from the {} by another process.",
                jobId,
                jobGraphStore.getClass().getSimpleName());

        onGoingRecoveryOperation =
                onGoingRecoveryOperation
                        .thenCompose(
                                ignored ->
                                        removeJobGraphIfRunning(jobId)
                                                .orElse(FutureUtils.completedVoidFuture()))
                        .handle(this::onErrorIfRunning);
    }

    private Optional<CompletableFuture<Void>> removeJobGraphIfRunning(JobID jobId) {
        return supplyIfRunning(() -> removeJobGraph(jobId));
    }

    private CompletableFuture<Void> removeJobGraph(JobID jobId) {
        return getDispatcherService()
                .map(dispatcherService -> dispatcherService.onRemovedJobGraph(jobId))
                .orElseGet(FutureUtils::completedVoidFuture);
    }

    // ---------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------

    public static SessionDispatcherLeaderProcess create(
            UUID leaderSessionId,
            DispatcherGatewayServiceFactory dispatcherFactory,
            JobGraphStore jobGraphStore,
            JobResultStore jobResultStore,
            Executor ioExecutor,
            FatalErrorHandler fatalErrorHandler) {
        return new SessionDispatcherLeaderProcess(
                leaderSessionId,
                dispatcherFactory,
                jobGraphStore,
                jobResultStore,
                ioExecutor,
                fatalErrorHandler);
    }
}
