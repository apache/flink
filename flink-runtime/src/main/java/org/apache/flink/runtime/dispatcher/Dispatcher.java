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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
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
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private final Configuration configuration;

    private final JobGraphWriter jobGraphWriter;
    private final RunningJobsRegistry runningJobsRegistry;

    private final HighAvailabilityServices highAvailabilityServices;
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final BlobServer blobServer;

    private final FatalErrorHandler fatalErrorHandler;

    private final Map<JobID, DispatcherJob> runningJobs;

    private final Collection<JobGraph> recoveredJobs;

    private final DispatcherBootstrapFactory dispatcherBootstrapFactory;

    private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

    private final JobManagerRunnerFactory jobManagerRunnerFactory;

    private final JobManagerMetricGroup jobManagerMetricGroup;

    private final HistoryServerArchivist historyServerArchivist;

    private final Executor ioExecutor;

    @Nullable private final String metricServiceQueryAddress;

    private final Map<JobID, CompletableFuture<Void>> dispatcherJobTerminationFutures;

    protected final CompletableFuture<ApplicationStatus> shutDownFuture;

    private DispatcherBootstrap dispatcherBootstrap;

    /** Enum to distinguish between initial job submission and re-submission for recovery. */
    protected enum ExecutionType {
        SUBMISSION,
        RECOVERY
    }

    public Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        super(rpcService, AkkaRpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
        checkNotNull(dispatcherServices);

        this.configuration = dispatcherServices.getConfiguration();
        this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
        this.resourceManagerGatewayRetriever =
                dispatcherServices.getResourceManagerGatewayRetriever();
        this.heartbeatServices = dispatcherServices.getHeartbeatServices();
        this.blobServer = dispatcherServices.getBlobServer();
        this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
        this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
        this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
        this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
        this.ioExecutor = dispatcherServices.getIoExecutor();

        this.jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        configuration, blobServer, fatalErrorHandler);

        this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

        runningJobs = new HashMap<>(16);

        this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

        this.archivedExecutionGraphStore = dispatcherServices.getArchivedExecutionGraphStore();

        this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();

        this.dispatcherJobTerminationFutures = new HashMap<>(2);

        this.shutDownFuture = new CompletableFuture<>();

        this.dispatcherBootstrapFactory = checkNotNull(dispatcherBootstrapFactory);

        this.recoveredJobs = new HashSet<>(recoveredJobs);
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

    private void startRecoveredJobs() {
        for (JobGraph recoveredJob : recoveredJobs) {
            runRecoveredJob(recoveredJob);
        }
        recoveredJobs.clear();
    }

    private void runRecoveredJob(final JobGraph recoveredJob) {
        checkNotNull(recoveredJob);
        try {
            runJob(recoveredJob, ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start recovered job %s.", recoveredJob.getJobID()),
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
        log.info("Received JobGraph submission {} ({}).", jobGraph.getJobID(), jobGraph.getName());

        try {
            if (isDuplicateJob(jobGraph.getJobID())) {
                return FutureUtils.completedExceptionally(
                        new DuplicateJobSubmissionException(jobGraph.getJobID()));
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

    /**
     * Checks whether the given job has already been submitted or executed.
     *
     * @param jobId identifying the submitted job
     * @return true if the job has already been submitted (is running) or has been executed
     * @throws FlinkException if the job scheduling status cannot be retrieved
     */
    private boolean isDuplicateJob(JobID jobId) throws FlinkException {
        final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

        try {
            jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
        } catch (IOException e) {
            throw new FlinkException(
                    String.format("Failed to retrieve job scheduling status for job %s.", jobId),
                    e);
        }

        return jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE
                || runningJobs.containsKey(jobId);
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
        log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

        final CompletableFuture<Acknowledge> persistAndRunFuture =
                waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                        .thenApply(ignored -> Acknowledge.get());

        return persistAndRunFuture.handleAsync(
                (acknowledge, throwable) -> {
                    if (throwable != null) {
                        cleanUpJobData(jobGraph.getJobID(), true);

                        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(throwable);
                        final Throwable strippedThrowable =
                                ExceptionUtils.stripCompletionException(throwable);
                        log.error(
                                "Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
                        throw new CompletionException(
                                new JobSubmissionException(
                                        jobGraph.getJobID(),
                                        "Failed to submit job.",
                                        strippedThrowable));
                    } else {
                        return acknowledge;
                    }
                },
                ioExecutor);
    }

    private void persistAndRunJob(JobGraph jobGraph) throws Exception {
        jobGraphWriter.putJobGraph(jobGraph);
        runJob(jobGraph, ExecutionType.SUBMISSION);
    }

    private void runJob(JobGraph jobGraph, ExecutionType executionType) {
        Preconditions.checkState(!runningJobs.containsKey(jobGraph.getJobID()));
        long initializationTimestamp = System.currentTimeMillis();
        CompletableFuture<JobManagerRunner> jobManagerRunnerFuture =
                createJobManagerRunner(jobGraph, initializationTimestamp);

        DispatcherJob dispatcherJob =
                DispatcherJob.createFor(
                        jobManagerRunnerFuture,
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        initializationTimestamp);
        runningJobs.put(jobGraph.getJobID(), dispatcherJob);

        final JobID jobId = jobGraph.getJobID();

        final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
                dispatcherJob
                        .getResultFuture()
                        .handleAsync(
                                (dispatcherJobResult, throwable) -> {
                                    Preconditions.checkState(
                                            runningJobs.get(jobId) == dispatcherJob,
                                            "The job entry in runningJobs must be bound to the lifetime of the DispatcherJob.");

                                    if (dispatcherJobResult != null) {
                                        return handleDispatcherJobResult(
                                                jobId, dispatcherJobResult, executionType);
                                    } else {
                                        return dispatcherJobFailed(jobId, throwable);
                                    }
                                },
                                getMainThreadExecutor());

        final CompletableFuture<Void> jobTerminationFuture =
                cleanupJobStateFuture
                        .thenApply(cleanupJobState -> removeJob(jobId, cleanupJobState))
                        .thenCompose(Function.identity());

        FutureUtils.assertNoException(jobTerminationFuture);
        registerDispatcherJobTerminationFuture(jobId, jobTerminationFuture);
    }

    private CleanupJobState handleDispatcherJobResult(
            JobID jobId, DispatcherJobResult dispatcherJobResult, ExecutionType executionType) {
        if (dispatcherJobResult.isInitializationFailure()
                && executionType == ExecutionType.RECOVERY) {
            return dispatcherJobFailed(jobId, dispatcherJobResult.getInitializationFailure());
        } else {
            return jobReachedGloballyTerminalState(dispatcherJobResult.getArchivedExecutionGraph());
        }
    }

    enum CleanupJobState {
        LOCAL(false),
        GLOBAL(true);

        final boolean cleanupHAData;

        CleanupJobState(boolean cleanupHAData) {
            this.cleanupHAData = cleanupHAData;
        }
    }

    private CleanupJobState dispatcherJobFailed(JobID jobId, Throwable throwable) {
        if (throwable instanceof JobNotFinishedException) {
            jobNotFinished(jobId);
        } else {
            jobMasterFailed(jobId, throwable);
        }

        return CleanupJobState.LOCAL;
    }

    CompletableFuture<JobManagerRunner> createJobManagerRunner(
            JobGraph jobGraph, long initializationTimestamp) {
        final RpcService rpcService = getRpcService();
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        JobManagerRunner runner =
                                jobManagerRunnerFactory.createJobManagerRunner(
                                        jobGraph,
                                        configuration,
                                        rpcService,
                                        highAvailabilityServices,
                                        heartbeatServices,
                                        jobManagerSharedServices,
                                        new DefaultJobManagerJobMetricGroupFactory(
                                                jobManagerMetricGroup),
                                        fatalErrorHandler,
                                        initializationTimestamp);
                        runner.start();
                        return runner;
                    } catch (Exception e) {
                        throw new CompletionException(
                                new JobInitializationException(
                                        jobGraph.getJobID(),
                                        "Could not instantiate JobManager.",
                                        e));
                    }
                },
                ioExecutor); // do not use main thread executor. Otherwise, Dispatcher is blocked on
        // JobManager creation
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(new HashSet<>(runningJobs.keySet())));
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
                jobManagerSharedServices.getScheduledExecutorService());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        Optional<DispatcherJob> maybeJob = getDispatcherJob(jobId);
        return maybeJob.map(job -> job.cancel(timeout))
                .orElseGet(
                        () -> {
                            log.debug("Dispatcher is unable to cancel job {}: not found", jobId);
                            return FutureUtils.completedExceptionally(
                                    new FlinkJobNotFoundException(jobId));
                        });
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        CompletableFuture<ResourceOverview> taskManagerOverviewFuture =
                runResourceManagerCommand(
                        resourceManagerGateway ->
                                resourceManagerGateway.requestResourceOverview(timeout));

        final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation =
                queryJobMastersForInformation(
                        dispatcherJob -> dispatcherJob.requestJobStatus(timeout));

        CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture =
                FutureUtils.combineAll(optionalJobInformation);

        CompletableFuture<Collection<JobStatus>> allJobsFuture =
                allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

        final JobsOverview completedJobsOverview =
                archivedExecutionGraphStore.getStoredJobsOverview();

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
                queryJobMastersForInformation(dj -> dj.requestJobDetails(timeout));

        CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails =
                FutureUtils.combineAll(individualOptionalJobDetails);

        CompletableFuture<Collection<JobDetails>> combinedJobDetails =
                optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

        final Collection<JobDetails> completedJobDetails =
                archivedExecutionGraphStore.getAvailableJobDetails();

        return combinedJobDetails.thenApply(
                (Collection<JobDetails> runningJobDetails) -> {
                    final Collection<JobDetails> allJobDetails =
                            new ArrayList<>(completedJobDetails.size() + runningJobDetails.size());

                    allJobDetails.addAll(runningJobDetails);
                    allJobDetails.addAll(completedJobDetails);

                    return new MultipleJobsDetails(allJobDetails);
                });
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
        Optional<DispatcherJob> maybeJob = getDispatcherJob(jobId);
        return maybeJob.map(job -> job.requestJobStatus(timeout))
                .orElseGet(
                        () -> {
                            // is it a completed job?
                            final JobDetails jobDetails =
                                    archivedExecutionGraphStore.getAvailableJobDetails(jobId);
                            if (jobDetails == null) {
                                return FutureUtils.completedExceptionally(
                                        new FlinkJobNotFoundException(jobId));
                            } else {
                                return CompletableFuture.completedFuture(jobDetails.getStatus());
                            }
                        });
    }

    @Override
    public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
            final JobID jobId, final JobVertexID jobVertexId) {
        return performOperationOnJobMasterGateway(
                jobId, gateway -> gateway.requestOperatorBackPressureStats(jobVertexId));
    }

    @Override
    public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
        Function<Throwable, ArchivedExecutionGraph> checkExecutionGraphStoreOnException =
                throwable -> {
                    // check whether it is a completed job
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            archivedExecutionGraphStore.get(jobId);
                    if (archivedExecutionGraph == null) {
                        throw new CompletionException(
                                ExceptionUtils.stripCompletionException(throwable));
                    } else {
                        return archivedExecutionGraph;
                    }
                };
        Optional<DispatcherJob> maybeJob = getDispatcherJob(jobId);
        return maybeJob.map(job -> job.requestJob(timeout))
                .orElse(FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)))
                .exceptionally(checkExecutionGraphStoreOnException);
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        DispatcherJob job = runningJobs.get(jobId);

        if (job == null) {
            final ArchivedExecutionGraph archivedExecutionGraph =
                    archivedExecutionGraphStore.get(jobId);

            if (archivedExecutionGraph == null) {
                return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
            } else {
                return CompletableFuture.completedFuture(
                        JobResult.createFrom(archivedExecutionGraph));
            }
        } else {
            return job.getResultFuture()
                    .thenApply(
                            dispatcherJobResult ->
                                    JobResult.createFrom(
                                            dispatcherJobResult.getArchivedExecutionGraph()));
        }
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
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServer.getPort());
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            final JobID jobId,
            final String targetDirectory,
            final boolean cancelJob,
            final Time timeout) {

        return performOperationOnJobMasterGateway(
                jobId, gateway -> gateway.triggerSavepoint(targetDirectory, cancelJob, timeout));
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            final JobID jobId,
            final String targetDirectory,
            final boolean advanceToEndOfEventTime,
            final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.stopWithSavepoint(
                                targetDirectory, advanceToEndOfEventTime, timeout));
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

    private void registerDispatcherJobTerminationFuture(
            JobID jobId, CompletableFuture<Void> dispatcherJobTerminationFuture) {
        Preconditions.checkState(!dispatcherJobTerminationFutures.containsKey(jobId));
        dispatcherJobTerminationFutures.put(jobId, dispatcherJobTerminationFuture);

        // clean up the pending termination future
        dispatcherJobTerminationFuture.thenRunAsync(
                () -> {
                    final CompletableFuture<Void> terminationFuture =
                            dispatcherJobTerminationFutures.remove(jobId);

                    //noinspection ObjectEquality
                    if (terminationFuture != null
                            && terminationFuture != dispatcherJobTerminationFuture) {
                        dispatcherJobTerminationFutures.put(jobId, terminationFuture);
                    }
                },
                getMainThreadExecutor());
    }

    private CompletableFuture<Void> removeJob(JobID jobId, CleanupJobState cleanupJobState) {
        final DispatcherJob job = checkNotNull(runningJobs.remove(jobId));

        final CompletableFuture<Void> jobTerminationFuture = job.closeAsync();

        return jobTerminationFuture.thenRunAsync(
                () -> cleanUpJobData(jobId, cleanupJobState.cleanupHAData), ioExecutor);
    }

    private void cleanUpJobData(JobID jobId, boolean cleanupHA) {
        jobManagerMetricGroup.removeJob(jobId);

        boolean cleanupHABlobs = false;
        if (cleanupHA) {
            try {
                jobGraphWriter.removeJobGraph(jobId);

                // only clean up the HA blobs if we could remove the job from HA storage
                cleanupHABlobs = true;
            } catch (Exception e) {
                log.warn(
                        "Could not properly remove job {} from submitted job graph store.",
                        jobId,
                        e);
            }

            try {
                runningJobsRegistry.clearJob(jobId);
            } catch (IOException e) {
                log.warn(
                        "Could not properly remove job {} from the running jobs registry.",
                        jobId,
                        e);
            }
        } else {
            try {
                jobGraphWriter.releaseJobGraph(jobId);
            } catch (Exception e) {
                log.warn(
                        "Could not properly release job {} from submitted job graph store.",
                        jobId,
                        e);
            }
        }

        blobServer.cleanupJob(jobId, cleanupHABlobs);
    }

    /** Terminate all currently running {@link DispatcherJob}s. */
    private void terminateRunningJobs() {
        log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

        final HashSet<JobID> jobsToRemove = new HashSet<>(runningJobs.keySet());

        for (JobID jobId : jobsToRemove) {
            terminateJob(jobId);
        }
    }

    private void terminateJob(JobID jobId) {
        final DispatcherJob dispatcherJob = runningJobs.get(jobId);

        if (dispatcherJob != null) {
            dispatcherJob.closeAsync();
        }
    }

    private CompletableFuture<Void> terminateRunningJobsAndGetTerminationFuture() {
        terminateRunningJobs();
        final Collection<CompletableFuture<Void>> values = dispatcherJobTerminationFutures.values();
        return FutureUtils.completeAll(values);
    }

    protected void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    protected CleanupJobState jobReachedGloballyTerminalState(
            ArchivedExecutionGraph archivedExecutionGraph) {
        Preconditions.checkArgument(
                archivedExecutionGraph.getState().isGloballyTerminalState(),
                "Job %s is in state %s which is not globally terminal.",
                archivedExecutionGraph.getJobID(),
                archivedExecutionGraph.getState());

        log.info(
                "Job {} reached globally terminal state {}.",
                archivedExecutionGraph.getJobID(),
                archivedExecutionGraph.getState());

        archiveExecutionGraph(archivedExecutionGraph);

        return CleanupJobState.GLOBAL;
    }

    private void archiveExecutionGraph(ArchivedExecutionGraph archivedExecutionGraph) {
        try {
            archivedExecutionGraphStore.put(archivedExecutionGraph);
        } catch (IOException e) {
            log.info(
                    "Could not store completed job {}({}).",
                    archivedExecutionGraph.getJobName(),
                    archivedExecutionGraph.getJobID(),
                    e);
        }

        final CompletableFuture<Acknowledge> executionGraphFuture =
                historyServerArchivist.archiveExecutionGraph(archivedExecutionGraph);

        executionGraphFuture.whenComplete(
                (Acknowledge ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        log.info(
                                "Could not archive completed job {}({}) to the history server.",
                                archivedExecutionGraph.getJobName(),
                                archivedExecutionGraph.getJobID(),
                                throwable);
                    }
                });
    }

    protected void jobNotFinished(JobID jobId) {
        log.info("Job {} was not finished by JobManager.", jobId);
    }

    private void jobMasterFailed(JobID jobId, Throwable cause) {
        // we fail fatally in case of a JobMaster failure in order to restart the
        // dispatcher to recover the jobs again. This only works in HA mode, though
        onFatalError(
                new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
    }

    /** Ensures that the JobMasterGateway is available. */
    private CompletableFuture<JobMasterGateway> getJobMasterGateway(JobID jobId) {
        DispatcherJob job = runningJobs.get(jobId);
        if (job == null) {
            return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
        }
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

    private Optional<DispatcherJob> getDispatcherJob(JobID jobId) {
        return Optional.ofNullable(runningJobs.get(jobId));
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
            Function<DispatcherJob, CompletableFuture<T>> queryFunction) {

        List<CompletableFuture<Optional<T>>> optionalJobInformation =
                new ArrayList<>(runningJobs.size());

        for (DispatcherJob job : runningJobs.values()) {
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
                            dispatcherJobTerminationFutures.remove(jobId);
                            action.accept(jobGraph);
                        }),
                getMainThreadExecutor());
    }

    CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
        if (runningJobs.containsKey(jobId)) {
            return FutureUtils.completedExceptionally(
                    new DispatcherException(
                            String.format("Job with job id %s is still running.", jobId)));
        } else {
            return dispatcherJobTerminationFutures.getOrDefault(
                    jobId, CompletableFuture.completedFuture(null));
        }
    }

    private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
        jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_JOBS, () -> (long) runningJobs.size());
    }

    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
        return CompletableFuture.runAsync(() -> terminateJob(jobId), getMainThreadExecutor());
    }
}
