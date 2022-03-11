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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleaner;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible for receiving
 * job submissions, persisting them, spawning JobManagers to execute the jobs and to recover them in
 * case of a master failure. Furthermore, it knows about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends PermanentlyFencedRpcEndpoint<DispatcherId>
        implements DispatcherGateway {

    public static final String DISPATCHER_NAME = "dispatcher";

    private static final int INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY = 16;

    private final Configuration configuration;

    private final JobGraphWriter jobGraphWriter;
    private final JobResultStore jobResultStore;

    private final HighAvailabilityServices highAvailabilityServices;
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final BlobServer blobServer;

    private final FatalErrorHandler fatalErrorHandler;

    private final OnMainThreadJobManagerRunnerRegistry jobManagerRunnerRegistry;

    private final Collection<JobGraph> recoveredJobs;

    private final Collection<JobResult> recoveredDirtyJobs;

    private final DispatcherBootstrapFactory dispatcherBootstrapFactory;

    private final ExecutionGraphInfoStore executionGraphInfoStore;

    private final JobManagerRunnerFactory jobManagerRunnerFactory;
    private final CleanupRunnerFactory cleanupRunnerFactory;

    private final JobManagerMetricGroup jobManagerMetricGroup;

    private final HistoryServerArchivist historyServerArchivist;

    private final Executor ioExecutor;

    @Nullable private final String metricServiceQueryAddress;

    private final Map<JobID, CompletableFuture<Void>> jobManagerRunnerTerminationFutures;

    protected final CompletableFuture<ApplicationStatus> shutDownFuture;

    private DispatcherBootstrap dispatcherBootstrap;

    private final DispatcherCachedOperationsHandler dispatcherCachedOperationsHandler;

    private final ResourceCleaner localResourceCleaner;
    private final ResourceCleaner globalResourceCleaner;

    /** Enum to distinguish between initial job submission and re-submission for recovery. */
    protected enum ExecutionType {
        SUBMISSION,
        RECOVERY
    }

    public Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                new DefaultJobManagerRunnerRegistry(INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY));
    }

    private Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                jobManagerRunnerRegistry,
                new DispatcherResourceCleanerFactory(jobManagerRunnerRegistry, dispatcherServices));
    }

    @VisibleForTesting
    protected Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory)
            throws Exception {
        super(rpcService, RpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
        assertRecoveredJobsAndDirtyJobResults(recoveredJobs, recoveredDirtyJobs);

        this.configuration = dispatcherServices.getConfiguration();
        this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
        this.resourceManagerGatewayRetriever =
                dispatcherServices.getResourceManagerGatewayRetriever();
        this.heartbeatServices = dispatcherServices.getHeartbeatServices();
        this.blobServer = dispatcherServices.getBlobServer();
        this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
        this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
        this.jobResultStore = dispatcherServices.getJobResultStore();
        this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
        this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
        this.ioExecutor = dispatcherServices.getIoExecutor();

        this.jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        configuration, blobServer, fatalErrorHandler);

        this.jobManagerRunnerRegistry =
                new OnMainThreadJobManagerRunnerRegistry(
                        jobManagerRunnerRegistry, this.getMainThreadExecutor());

        this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

        this.executionGraphInfoStore = dispatcherServices.getArchivedExecutionGraphStore();

        this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();
        this.cleanupRunnerFactory = dispatcherServices.getCleanupRunnerFactory();

        this.jobManagerRunnerTerminationFutures =
                new HashMap<>(INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY);

        this.shutDownFuture = new CompletableFuture<>();

        this.dispatcherBootstrapFactory = checkNotNull(dispatcherBootstrapFactory);

        this.recoveredJobs = new HashSet<>(recoveredJobs);

        this.recoveredDirtyJobs = new HashSet<>(recoveredDirtyJobs);

        this.blobServer.retainJobs(
                recoveredJobs.stream().map(JobGraph::getJobID).collect(Collectors.toSet()),
                dispatcherServices.getIoExecutor());

        this.dispatcherCachedOperationsHandler =
                new DispatcherCachedOperationsHandler(
                        dispatcherServices.getOperationCaches(),
                        this::triggerSavepointAndGetLocation,
                        this::stopWithSavepointAndGetLocation);

        this.localResourceCleaner =
                resourceCleanerFactory.createLocalResourceCleaner(this.getMainThreadExecutor());
        this.globalResourceCleaner =
                resourceCleanerFactory.createGlobalResourceCleaner(this.getMainThreadExecutor());
    }

    // ------------------------------------------------------
    // Getters
    // ------------------------------------------------------

    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    // ------------------------------------------------------
    // Lifecycle methods
    // ------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            startDispatcherServices();
        } catch (Throwable t) {
            final DispatcherException exception =
                    new DispatcherException(
                            String.format("Could not start the Dispatcher %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startCleanupRetries();
        startRecoveredJobs();

        this.dispatcherBootstrap =
                this.dispatcherBootstrapFactory.create(
                        getSelfGateway(DispatcherGateway.class),
                        this.getRpcService().getScheduledExecutor(),
                        this::onFatalError);
    }

    private void startDispatcherServices() throws Exception {
        try {
            registerDispatcherMetrics(jobManagerMetricGroup);
        } catch (Exception e) {
            handleStartDispatcherServicesException(e);
        }
    }

    private static void assertRecoveredJobsAndDirtyJobResults(
            Collection<JobGraph> recoveredJobs, Collection<JobResult> recoveredDirtyJobResults) {
        final Set<JobID> jobIdsOfFinishedJobs =
                recoveredDirtyJobResults.stream()
                        .map(JobResult::getJobId)
                        .collect(Collectors.toSet());

        final boolean noRecoveredJobGraphHasDirtyJobResult =
                recoveredJobs.stream()
                        .noneMatch(
                                recoveredJobGraph ->
                                        jobIdsOfFinishedJobs.contains(
                                                recoveredJobGraph.getJobID()));

        Preconditions.checkArgument(
                noRecoveredJobGraphHasDirtyJobResult,
                "There should be no overlap between the recovered JobGraphs and the passed dirty JobResults based on their job ID.");
    }

    private void startRecoveredJobs() {
        for (JobGraph recoveredJob : recoveredJobs) {
            runRecoveredJob(recoveredJob);
        }
        recoveredJobs.clear();
    }

    private void runRecoveredJob(final JobGraph recoveredJob) {
        checkNotNull(recoveredJob);
        try {
            runJob(createJobMasterRunner(recoveredJob), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start recovered job %s.", recoveredJob.getJobID()),
                            throwable));
        }
    }

    private void startCleanupRetries() {
        recoveredDirtyJobs.forEach(this::runCleanupRetry);
        recoveredDirtyJobs.clear();
    }

    private void runCleanupRetry(final JobResult jobResult) {
        checkNotNull(jobResult);

        try {
            runJob(createJobCleanupRunner(jobResult), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start cleanup retry for job %s.",
                                    jobResult.getJobId()),
                            throwable));
        }
    }

    private void handleStartDispatcherServicesException(Exception e) throws Exception {
        try {
            stopDispatcherServices();
        } catch (Exception exception) {
            e.addSuppressed(exception);
        }

        throw e;
    }

    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping dispatcher {}.", getAddress());

        final CompletableFuture<Void> allJobsTerminationFuture =
                terminateRunningJobsAndGetTerminationFuture();

        return FutureUtils.runAfterwards(
                allJobsTerminationFuture,
                () -> {
                    dispatcherBootstrap.stop();

                    stopDispatcherServices();

                    log.info("Stopped dispatcher {}.", getAddress());
                });
    }

    private void stopDispatcherServices() throws Exception {
        Exception exception = null;
        try {
            jobManagerSharedServices.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        jobManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ------------------------------------------------------
    // RPCs
    // ------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        log.info(
                "Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

        try {
            if (isDuplicateJob(jobGraph.getJobID())) {
                if (isInGloballyTerminalState(jobGraph.getJobID())) {
                    log.warn(
                            "Ignoring JobGraph submission '{}' ({}) because the job already reached a globally-terminal state (i.e. {}) in a previous execution.",
                            jobGraph.getName(),
                            jobGraph.getJobID(),
                            Arrays.stream(JobStatus.values())
                                    .filter(JobStatus::isGloballyTerminalState)
                                    .map(JobStatus::name)
                                    .collect(Collectors.joining(", ")));
                }

                final DuplicateJobSubmissionException exception =
                        isInGloballyTerminalState(jobGraph.getJobID())
                                ? DuplicateJobSubmissionException.ofGloballyTerminated(
                                        jobGraph.getJobID())
                                : DuplicateJobSubmissionException.of(jobGraph.getJobID());
                return FutureUtils.completedExceptionally(exception);
            } else if (isPartialResourceConfigured(jobGraph)) {
                return FutureUtils.completedExceptionally(
                        new JobSubmissionException(
                                jobGraph.getJobID(),
                                "Currently jobs is not supported if parts of the vertices have "
                                        + "resources configured. The limitation will be removed in future versions."));
            } else {
                return internalSubmitJob(jobGraph);
            }
        } catch (FlinkException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> submitFailedJob(
            JobID jobId, String jobName, Throwable exception) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobId,
                        jobName,
                        JobStatus.FAILED,
                        exception,
                        null,
                        System.currentTimeMillis());
        archiveExecutionGraph(new ExecutionGraphInfo(archivedExecutionGraph));
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Checks whether the given job has already been submitted or executed.
     *
     * @param jobId identifying the submitted job
     * @return true if the job has already been submitted (is running) or has been executed
     * @throws FlinkException if the job scheduling status cannot be retrieved
     */
    private boolean isDuplicateJob(JobID jobId) throws FlinkException {
        return isInGloballyTerminalState(jobId) || jobManagerRunnerRegistry.isRegistered(jobId);
    }

    /**
     * Checks whether the given job has already been executed.
     *
     * @param jobId identifying the submitted job
     * @return true if the job has already finished, either successfully or as a failure
     * @throws FlinkException if the job scheduling status cannot be retrieved
     */
    private boolean isInGloballyTerminalState(JobID jobId) throws FlinkException {
        try {
            return jobResultStore.hasJobResultEntry(jobId);
        } catch (IOException e) {
            throw new FlinkException(
                    String.format("Failed to retrieve job scheduling status for job %s.", jobId),
                    e);
        }
    }

    private boolean isPartialResourceConfigured(JobGraph jobGraph) {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }

    private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
        log.info("Submitting job '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());
        return waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Acknowledge> handleTermination(
            JobID jobId, @Nullable Throwable terminationThrowable) {
        if (terminationThrowable != null) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .handleAsync(
                            (ignored, cleanupThrowable) -> {
                                if (cleanupThrowable != null) {
                                    log.warn(
                                            "Cleanup didn't succeed after job submission failed for job {}.",
                                            jobId,
                                            cleanupThrowable);
                                    terminationThrowable.addSuppressed(cleanupThrowable);
                                }
                                ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(
                                        terminationThrowable);
                                final Throwable strippedThrowable =
                                        ExceptionUtils.stripCompletionException(
                                                terminationThrowable);
                                log.error("Failed to submit job {}.", jobId, strippedThrowable);
                                throw new CompletionException(
                                        new JobSubmissionException(
                                                jobId, "Failed to submit job.", strippedThrowable));
                            },
                            getMainThreadExecutor());
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private void persistAndRunJob(JobGraph jobGraph) throws Exception {
        jobGraphWriter.putJobGraph(jobGraph);
        runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);
    }

    private JobManagerRunner createJobMasterRunner(JobGraph jobGraph) throws Exception {
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(jobGraph.getJobID()));
        return jobManagerRunnerFactory.createJobManagerRunner(
                jobGraph,
                configuration,
                getRpcService(),
                highAvailabilityServices,
                heartbeatServices,
                jobManagerSharedServices,
                new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
                fatalErrorHandler,
                System.currentTimeMillis());
    }

    private JobManagerRunner createJobCleanupRunner(JobResult dirtyJobResult) throws Exception {
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(dirtyJobResult.getJobId()));
        return cleanupRunnerFactory.create(
                dirtyJobResult,
                highAvailabilityServices.getCheckpointRecoveryFactory(),
                configuration,
                ioExecutor);
    }

    private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
            throws Exception {
        jobManagerRunner.start();
        jobManagerRunnerRegistry.register(jobManagerRunner);

        final JobID jobId = jobManagerRunner.getJobID();

        final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
                jobManagerRunner
                        .getResultFuture()
                        .handleAsync(
                                (jobManagerRunnerResult, throwable) -> {
                                    Preconditions.checkState(
                                            jobManagerRunnerRegistry.isRegistered(jobId)
                                                    && jobManagerRunnerRegistry.get(jobId)
                                                            == jobManagerRunner,
                                            "The job entry in runningJobs must be bound to the lifetime of the JobManagerRunner.");

                                    if (jobManagerRunnerResult != null) {
                                        return handleJobManagerRunnerResult(
                                                jobManagerRunnerResult, executionType);
                                    } else {
                                        return jobManagerRunnerFailed(jobId, throwable);
                                    }
                                },
                                getMainThreadExecutor());

        final CompletableFuture<Void> jobTerminationFuture =
                cleanupJobStateFuture.thenCompose(
                        cleanupJobState -> removeJob(jobId, cleanupJobState));

        FutureUtils.handleUncaughtException(
                jobTerminationFuture,
                (thread, throwable) -> fatalErrorHandler.onFatalError(throwable));
        registerJobManagerRunnerTerminationFuture(jobId, jobTerminationFuture);
    }

    private CleanupJobState handleJobManagerRunnerResult(
            JobManagerRunnerResult jobManagerRunnerResult, ExecutionType executionType) {
        if (jobManagerRunnerResult.isInitializationFailure()
                && executionType == ExecutionType.RECOVERY) {
            return jobManagerRunnerFailed(
                    jobManagerRunnerResult.getExecutionGraphInfo().getJobId(),
                    jobManagerRunnerResult.getInitializationFailure());
        }
        return jobReachedTerminalState(jobManagerRunnerResult.getExecutionGraphInfo());
    }

    enum CleanupJobState {
        LOCAL,
        GLOBAL
    }

    private CleanupJobState jobManagerRunnerFailed(JobID jobId, Throwable throwable) {
        jobMasterFailed(jobId, throwable);
        return CleanupJobState.LOCAL;
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(jobManagerRunnerRegistry.getRunningJobIds()));
    }

    @Override
    public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        return CompletableFuture.supplyAsync(
                () -> {
                    log.info("Disposing savepoint {}.", savepointPath);

                    try {
                        Checkpoints.disposeSavepoint(
                                savepointPath, configuration, classLoader, log);
                    } catch (IOException | FlinkException e) {
                        throw new CompletionException(
                                new FlinkException(
                                        String.format(
                                                "Could not dispose savepoint %s.", savepointPath),
                                        e));
                    }

                    return Acknowledge.get();
                },
                jobManagerSharedServices.getIoExecutor());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);

        if (maybeJob.isPresent()) {
            return maybeJob.get().cancel(timeout);
        }

        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);
        if (executionGraphInfo != null) {
            final JobStatus jobStatus = executionGraphInfo.getArchivedExecutionGraph().getState();
            if (jobStatus == JobStatus.CANCELED) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                return FutureUtils.completedExceptionally(
                        new FlinkJobTerminatedWithoutCancellationException(jobId, jobStatus));
            }
        }

        log.debug("Dispatcher is unable to cancel job {}: not found", jobId);
        return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        CompletableFuture<ResourceOverview> taskManagerOverviewFuture =
                runResourceManagerCommand(
                        resourceManagerGateway ->
                                resourceManagerGateway.requestResourceOverview(timeout));

        final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobStatus(timeout));

        CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture =
                FutureUtils.combineAll(optionalJobInformation);

        CompletableFuture<Collection<JobStatus>> allJobsFuture =
                allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

        final JobsOverview completedJobsOverview = executionGraphInfoStore.getStoredJobsOverview();

        return allJobsFuture.thenCombine(
                taskManagerOverviewFuture,
                (Collection<JobStatus> runningJobsStatus, ResourceOverview resourceOverview) -> {
                    final JobsOverview allJobsOverview =
                            JobsOverview.create(runningJobsStatus).combine(completedJobsOverview);
                    return new ClusterOverview(resourceOverview, allJobsOverview);
                });
    }

    @Override
    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
        List<CompletableFuture<Optional<JobDetails>>> individualOptionalJobDetails =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobDetails(timeout));

        CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails =
                FutureUtils.combineAll(individualOptionalJobDetails);

        CompletableFuture<Collection<JobDetails>> combinedJobDetails =
                optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

        final Collection<JobDetails> completedJobDetails =
                executionGraphInfoStore.getAvailableJobDetails();

        return combinedJobDetails.thenApply(
                (Collection<JobDetails> runningJobDetails) -> {
                    final Map<JobID, JobDetails> deduplicatedJobs = new HashMap<>();

                    completedJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));
                    runningJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));

                    return new MultipleJobsDetails(new HashSet<>(deduplicatedJobs.values()));
                });
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJobStatus(timeout))
                .orElseGet(
                        () -> {
                            // is it a completed job?
                            final JobDetails jobDetails =
                                    executionGraphInfoStore.getAvailableJobDetails(jobId);
                            if (jobDetails == null) {
                                return FutureUtils.completedExceptionally(
                                        new FlinkJobNotFoundException(jobId));
                            } else {
                                return CompletableFuture.completedFuture(jobDetails.getStatus());
                            }
                        });
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, Time timeout) {
        Function<Throwable, ExecutionGraphInfo> checkExecutionGraphStoreOnException =
                throwable -> {
                    // check whether it is a completed job
                    final ExecutionGraphInfo executionGraphInfo =
                            executionGraphInfoStore.get(jobId);
                    if (executionGraphInfo == null) {
                        throw new CompletionException(
                                ExceptionUtils.stripCompletionException(throwable));
                    } else {
                        return executionGraphInfo;
                    }
                };
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJob(timeout))
                .orElse(FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)))
                .exceptionally(checkExecutionGraphStoreOnException);
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);

            if (executionGraphInfo == null) {
                return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
            } else {
                return CompletableFuture.completedFuture(
                        JobResult.createFrom(executionGraphInfo.getArchivedExecutionGraph()));
            }
        }

        final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
        return jobManagerRunner
                .getResultFuture()
                .thenApply(
                        jobManagerRunnerResult ->
                                JobResult.createFrom(
                                        jobManagerRunnerResult
                                                .getExecutionGraphInfo()
                                                .getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        if (metricServiceQueryAddress != null) {
            return CompletableFuture.completedFuture(
                    Collections.singleton(metricServiceQueryAddress));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return runResourceManagerCommand(
                resourceManagerGateway ->
                        resourceManagerGateway.requestTaskManagerMetricQueryServiceAddresses(
                                timeout));
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        int stackTraceMaxDepth = configuration.get(ClusterOptions.THREAD_DUMP_STACKTRACE_MAX_DEPTH);
        return CompletableFuture.completedFuture(ThreadDumpInfo.dumpAndCreate(stackTraceMaxDepth));
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServer.getPort());
    }

    @Override
    public CompletableFuture<String> triggerCheckpoint(JobID jobID, Time timeout) {
        return performOperationOnJobMasterGateway(
                jobID, gateway -> gateway.triggerCheckpoint(timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> triggerSavepoint(
            final AsynchronousJobOperationKey operationKey,
            final String targetDirectory,
            SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.triggerSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> triggerSavepointAndGetLocation(
            JobID jobId,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.triggerSavepoint(
                                targetDirectory,
                                savepointMode.isTerminalMode(),
                                formatType,
                                timeout));
    }

    @Override
    public CompletableFuture<OperationResult<String>> getTriggeredSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return dispatcherCachedOperationsHandler.getSavepointStatus(operationKey);
    }

    @Override
    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.stopWithSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> stopWithSavepointAndGetLocation(
            final JobID jobId,
            final String targetDirectory,
            final SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.stopWithSavepoint(
                                targetDirectory,
                                formatType,
                                savepointMode.isTerminalMode(),
                                timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster() {
        return shutDownCluster(ApplicationStatus.SUCCEEDED);
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster(
            final ApplicationStatus applicationStatus) {
        shutDownFuture.complete(applicationStatus);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            JobID jobId,
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.deliverCoordinationRequestToCoordinator(
                                operatorId, serializedRequest, timeout));
    }

    private void registerJobManagerRunnerTerminationFuture(
            JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
        Preconditions.checkState(!jobManagerRunnerTerminationFutures.containsKey(jobId));
        jobManagerRunnerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

        // clean up the pending termination future
        jobManagerRunnerTerminationFuture.thenRunAsync(
                () -> {
                    final CompletableFuture<Void> terminationFuture =
                            jobManagerRunnerTerminationFutures.remove(jobId);

                    //noinspection ObjectEquality
                    if (terminationFuture != null
                            && terminationFuture != jobManagerRunnerTerminationFuture) {
                        jobManagerRunnerTerminationFutures.put(jobId, terminationFuture);
                    }
                },
                getMainThreadExecutor());
    }

    private CompletableFuture<Void> removeJob(JobID jobId, CleanupJobState cleanupJobState) {
        switch (cleanupJobState) {
            case LOCAL:
                return localResourceCleaner.cleanupAsync(jobId);
            case GLOBAL:
                return globalResourceCleaner
                        .cleanupAsync(jobId)
                        .thenRun(() -> markJobAsClean(jobId));
            default:
                throw new IllegalStateException("Invalid cleanup state: " + cleanupJobState);
        }
    }

    private void markJobAsClean(JobID jobId) {
        try {
            jobResultStore.markResultAsClean(jobId);
            log.debug(
                    "Cleanup for the job '{}' has finished. Job has been marked as clean.", jobId);
        } catch (IOException e) {
            log.warn("Could not properly mark job {} result as clean.", jobId, e);
        }
    }

    /** Terminate all currently running {@link JobManagerRunner}s. */
    private void terminateRunningJobs() {
        log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

        final Set<JobID> jobsToRemove = jobManagerRunnerRegistry.getRunningJobIds();

        for (JobID jobId : jobsToRemove) {
            terminateJob(jobId);
        }
    }

    private void terminateJob(JobID jobId) {
        if (jobManagerRunnerRegistry.isRegistered(jobId)) {
            final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
            jobManagerRunner.closeAsync();
        }
    }

    private CompletableFuture<Void> terminateRunningJobsAndGetTerminationFuture() {
        terminateRunningJobs();
        final Collection<CompletableFuture<Void>> values =
                jobManagerRunnerTerminationFutures.values();
        return FutureUtils.completeAll(values);
    }

    protected void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    protected CleanupJobState jobReachedTerminalState(ExecutionGraphInfo executionGraphInfo) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();
        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        Preconditions.checkArgument(
                terminalJobStatus.isTerminalState(),
                "Job %s is in state %s which is not terminal.",
                archivedExecutionGraph.getJobID(),
                terminalJobStatus);

        // the failureInfo contains the reason for why job was failed/suspended, but for
        // finished/canceled jobs it may contain the last cause of a restart (if there were any)
        // for finished/canceled jobs we don't want to print it because it is misleading
        final boolean isFailureInfoRelatedToJobTermination =
                terminalJobStatus == JobStatus.SUSPENDED || terminalJobStatus == JobStatus.FAILED;

        if (archivedExecutionGraph.getFailureInfo() != null
                && isFailureInfoRelatedToJobTermination) {
            log.info(
                    "Job {} reached terminal state {}.\n{}",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus,
                    archivedExecutionGraph.getFailureInfo().getExceptionAsString().trim());
        } else {
            log.info(
                    "Job {} reached terminal state {}.",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus);
        }

        archiveExecutionGraph(executionGraphInfo);

        if (terminalJobStatus.isGloballyTerminalState()) {
            final JobID jobId = executionGraphInfo.getJobId();
            try {
                if (jobResultStore.hasCleanJobResultEntry(jobId)) {
                    log.warn(
                            "Job {} is already marked as clean but clean up was triggered again.",
                            jobId);
                } else if (!jobResultStore.hasDirtyJobResultEntry(jobId)) {
                    jobResultStore.createDirtyResult(
                            new JobResultEntry(
                                    JobResult.createFrom(
                                            executionGraphInfo.getArchivedExecutionGraph())));
                    log.info(
                            "Job {} has been registered for cleanup in the JobResultStore after reaching a terminal state.",
                            jobId);
                }
            } catch (IOException e) {
                fatalErrorHandler.onFatalError(
                        new FlinkException(
                                String.format(
                                        "The job %s couldn't be marked as pre-cleanup finished in JobResultStore.",
                                        jobId),
                                e));
            }
        }

        return terminalJobStatus.isGloballyTerminalState()
                ? CleanupJobState.GLOBAL
                : CleanupJobState.LOCAL;
    }

    private void archiveExecutionGraph(ExecutionGraphInfo executionGraphInfo) {
        try {
            executionGraphInfoStore.put(executionGraphInfo);
        } catch (IOException e) {
            log.info(
                    "Could not store completed job {}({}).",
                    executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                    executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                    e);
        }

        // do not create an archive for suspended jobs, as this would eventually lead to multiple
        // archive attempts which we currently do not support
        if (executionGraphInfo.getArchivedExecutionGraph().getState().isGloballyTerminalState()) {
            final CompletableFuture<Acknowledge> executionGraphFuture =
                    historyServerArchivist.archiveExecutionGraph(executionGraphInfo);

            executionGraphFuture.whenComplete(
                    (Acknowledge ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            log.info(
                                    "Could not archive completed job {}({}) to the history server.",
                                    executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                                    executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                                    throwable);
                        }
                    });
        }
    }

    private void jobMasterFailed(JobID jobId, Throwable cause) {
        // we fail fatally in case of a JobMaster failure in order to restart the
        // dispatcher to recover the jobs again. This only works in HA mode, though
        onFatalError(
                new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
    }

    /** Ensures that the JobMasterGateway is available. */
    private CompletableFuture<JobMasterGateway> getJobMasterGateway(JobID jobId) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
        }

        final JobManagerRunner job = jobManagerRunnerRegistry.get(jobId);
        if (!job.isInitialized()) {
            return FutureUtils.completedExceptionally(
                    new UnavailableDispatcherOperationException(
                            "Unable to get JobMasterGateway for initializing job. "
                                    + "The requested operation is not available while the JobManager is initializing."));
        }
        return job.getJobMasterGateway();
    }

    private <T> CompletableFuture<T> performOperationOnJobMasterGateway(
            JobID jobId, Function<JobMasterGateway, CompletableFuture<T>> operation) {
        return getJobMasterGateway(jobId).thenCompose(operation);
    }

    private CompletableFuture<ResourceManagerGateway> getResourceManagerGateway() {
        return resourceManagerGatewayRetriever.getFuture();
    }

    private Optional<JobManagerRunner> getJobManagerRunner(JobID jobId) {
        return jobManagerRunnerRegistry.isRegistered(jobId)
                ? Optional.of(jobManagerRunnerRegistry.get(jobId))
                : Optional.empty();
    }

    private <T> CompletableFuture<T> runResourceManagerCommand(
            Function<ResourceManagerGateway, CompletableFuture<T>> resourceManagerCommand) {
        return getResourceManagerGateway()
                .thenApply(resourceManagerCommand)
                .thenCompose(Function.identity());
    }

    private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
        return optionalCollection.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Nonnull
    private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(
            Function<JobManagerRunner, CompletableFuture<T>> queryFunction) {

        List<CompletableFuture<Optional<T>>> optionalJobInformation =
                new ArrayList<>(jobManagerRunnerRegistry.size());

        for (JobManagerRunner job : jobManagerRunnerRegistry.getJobManagerRunners()) {
            final CompletableFuture<Optional<T>> queryResult =
                    queryFunction
                            .apply(job)
                            .handle((T value, Throwable t) -> Optional.ofNullable(value));
            optionalJobInformation.add(queryResult);
        }
        return optionalJobInformation;
    }

    private CompletableFuture<Void> waitForTerminatingJob(
            JobID jobId, JobGraph jobGraph, ThrowingConsumer<JobGraph, ?> action) {
        final CompletableFuture<Void> jobManagerTerminationFuture =
                getJobTerminationFuture(jobId)
                        .exceptionally(
                                (Throwable throwable) -> {
                                    throw new CompletionException(
                                            new DispatcherException(
                                                    String.format(
                                                            "Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.",
                                                            jobId),
                                                    throwable));
                                });

        return jobManagerTerminationFuture.thenAcceptAsync(
                FunctionUtils.uncheckedConsumer(
                        (ignored) -> {
                            jobManagerRunnerTerminationFutures.remove(jobId);
                            action.accept(jobGraph);
                        }),
                getMainThreadExecutor());
    }

    CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
        if (jobManagerRunnerRegistry.isRegistered(jobId)) {
            return FutureUtils.completedExceptionally(
                    new DispatcherException(
                            String.format("Job with job id %s is still running.", jobId)));
        } else {
            return jobManagerRunnerTerminationFutures.getOrDefault(
                    jobId, CompletableFuture.completedFuture(null));
        }
    }

    private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
        jobManagerMetricGroup.gauge(
                MetricNames.NUM_RUNNING_JOBS,
                // metrics can be called from anywhere and therefore, have to run without the main
                // thread safeguard being triggered. For metrics, we can afford to be not 100%
                // accurate
                () -> (long) jobManagerRunnerRegistry.getWrappedDelegate().size());
    }

    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
        return CompletableFuture.runAsync(() -> terminateJob(jobId), getMainThreadExecutor());
    }
}
