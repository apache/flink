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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.MutableVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.failure.DefaultFailureEnricherContext;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.JobStatusStore;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ReservedSlots;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAllocator;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.EnforceMinimalIncreaseRescalingController;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.EnforceParallelismChangeRescalingController;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.RescalingController;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.metrics.DeploymentStateTimeMetrics;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SchedulerNG} implementation that uses the declarative resource management and
 * automatically adapts the parallelism in case not enough resource could be acquired to run at the
 * configured parallelism, as described in FLIP-160.
 *
 * <p>This scheduler only supports jobs with streaming semantics, i.e., all vertices are connected
 * via pipelined data-exchanges.
 *
 * <p>The implementation is spread over multiple {@link State} classes that control which RPCs are
 * allowed in a given state and what state transitions are possible (see the FLIP for an overview).
 * This class can thus be roughly split into 2 parts:
 *
 * <p>1) RPCs, which must forward the call to the state via {@link State#tryRun(Class,
 * ThrowingConsumer, String)} or {@link State#tryCall(Class, FunctionWithException, String)}.
 *
 * <p>2) Context methods, which are called by states, to either transition into another state or
 * access functionality of some component in the scheduler.
 */
public class AdaptiveScheduler
        implements SchedulerNG,
                Created.Context,
                WaitingForResources.Context,
                CreatingExecutionGraph.Context,
                Executing.Context,
                Restarting.Context,
                Failing.Context,
                Finished.Context,
                StopWithSavepoint.Context {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveScheduler.class);

    private final JobGraph jobGraph;
    private final VertexParallelismStore initialParallelismStore;

    private final DeclarativeSlotPool declarativeSlotPool;

    private final long initializationTimestamp;

    private final Executor ioExecutor;
    private final ClassLoader userCodeClassLoader;

    private final CheckpointsCleaner checkpointsCleaner;
    private final CompletedCheckpointStore completedCheckpointStore;
    private final CheckpointIDCounter checkpointIdCounter;

    private final CompletableFuture<JobStatus> jobTerminationFuture = new CompletableFuture<>();

    private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

    private final ComponentMainThreadExecutor componentMainThreadExecutor;
    private final FatalErrorHandler fatalErrorHandler;
    private final Collection<FailureEnricher> failureEnrichers;

    private final Collection<JobStatusListener> jobStatusListeners;

    private final SlotAllocator slotAllocator;

    private final RescalingController rescalingController;

    private final RescalingController forceRescalingController;

    private final Duration initialResourceAllocationTimeout;

    private final Duration resourceStabilizationTimeout;

    private final ExecutionGraphFactory executionGraphFactory;

    private State state = new Created(this, LOG);

    private boolean isTransitioningState = false;

    private int numRestarts = 0;

    private final MutableVertexAttemptNumberStore vertexAttemptNumberStore =
            new DefaultVertexAttemptNumberStore();

    private BackgroundTask<ExecutionGraph> backgroundTask = BackgroundTask.finishedBackgroundTask();

    private final SchedulerExecutionMode executionMode;

    private final DeploymentStateTimeMetrics deploymentTimeMetrics;

    private final BoundedFIFOQueue<RootExceptionHistoryEntry> exceptionHistory;
    private JobGraphJobInformation jobInformation;
    private ResourceCounter desiredResources = ResourceCounter.empty();

    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    private final Duration slotIdleTimeout;

    public AdaptiveScheduler(
            JobGraph jobGraph,
            @Nullable JobResourceRequirements jobResourceRequirements,
            Configuration configuration,
            DeclarativeSlotPool declarativeSlotPool,
            SlotAllocator slotAllocator,
            Executor ioExecutor,
            ClassLoader userCodeClassLoader,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler,
            JobStatusListener jobStatusListener,
            Collection<FailureEnricher> failureEnrichers,
            ExecutionGraphFactory executionGraphFactory)
            throws JobExecutionException {

        assertPreconditions(jobGraph);

        this.jobGraph = jobGraph;
        this.executionMode = configuration.get(JobManagerOptions.SCHEDULER_MODE);

        VertexParallelismStore vertexParallelismStore =
                computeVertexParallelismStore(jobGraph, executionMode);
        if (jobResourceRequirements != null) {
            vertexParallelismStore =
                    DefaultVertexParallelismStore.applyJobResourceRequirements(
                                    vertexParallelismStore, jobResourceRequirements)
                            .orElse(vertexParallelismStore);
        }

        this.initialParallelismStore = vertexParallelismStore;
        this.jobInformation = new JobGraphJobInformation(jobGraph, vertexParallelismStore);

        this.declarativeSlotPool = declarativeSlotPool;
        this.initializationTimestamp = initializationTimestamp;
        this.ioExecutor = ioExecutor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
        this.fatalErrorHandler = fatalErrorHandler;
        this.checkpointsCleaner = checkpointsCleaner;
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph, configuration, checkpointRecoveryFactory, ioExecutor, LOG);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkpointRecoveryFactory);

        this.slotAllocator = slotAllocator;

        declarativeSlotPool.registerNewSlotsListener(this::newResourcesAvailable);

        this.componentMainThreadExecutor = mainThreadExecutor;

        this.rescalingController = new EnforceMinimalIncreaseRescalingController(configuration);

        this.forceRescalingController = new EnforceParallelismChangeRescalingController();

        this.initialResourceAllocationTimeout = initialResourceAllocationTimeout;

        this.resourceStabilizationTimeout = resourceStabilizationTimeout;

        this.executionGraphFactory = executionGraphFactory;

        final JobStatusStore jobStatusStore = new JobStatusStore(initializationTimestamp);
        final Collection<JobStatusListener> tmpJobStatusListeners = new ArrayList<>();
        tmpJobStatusListeners.add(Preconditions.checkNotNull(jobStatusListener));
        tmpJobStatusListeners.add(jobStatusStore);

        final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings =
                MetricOptions.JobStatusMetricsSettings.fromConfiguration(configuration);

        deploymentTimeMetrics =
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

        SchedulerBase.registerJobMetrics(
                jobManagerJobMetricGroup,
                jobStatusStore,
                () -> (long) numRestarts,
                deploymentTimeMetrics,
                tmpJobStatusListeners::add,
                initializationTimestamp,
                jobStatusMetricsSettings);

        jobStatusListeners = Collections.unmodifiableCollection(tmpJobStatusListeners);
        this.failureEnrichers = failureEnrichers;
        this.exceptionHistory =
                new BoundedFIFOQueue<>(
                        configuration.getInteger(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.slotIdleTimeout =
                Duration.ofMillis(configuration.get(JobManagerOptions.SLOT_IDLE_TIMEOUT));
    }

    private static void assertPreconditions(JobGraph jobGraph) throws RuntimeException {
        Preconditions.checkState(
                jobGraph.getJobType() == JobType.STREAMING,
                "The adaptive scheduler only supports streaming jobs.");

        for (JobVertex vertex : jobGraph.getVertices()) {
            Preconditions.checkState(
                    vertex.getParallelism() > 0,
                    "The adaptive scheduler expects the parallelism being set for each JobVertex (violated JobVertex: %s).",
                    vertex.getID());
            for (JobEdge jobEdge : vertex.getInputs()) {
                Preconditions.checkState(
                        jobEdge.getSource()
                                .getResultType()
                                .isPipelinedOrPipelinedBoundedResultPartition(),
                        "The adaptive scheduler supports pipelined data exchanges (violated by %s -> %s).",
                        jobEdge.getSource().getProducer(),
                        jobEdge.getTarget().getID());
            }
        }
    }

    /**
     * Creates the parallelism store for a set of vertices, optionally with a flag to leave the
     * vertex parallelism unchanged. If the flag is set, the parallelisms must be valid for
     * execution.
     *
     * <p>We need to set parallelism to the max possible value when requesting resources, but when
     * executing the graph we should respect what we are actually given.
     *
     * @param vertices The vertices to store parallelism information for
     * @param adjustParallelism Whether to adjust the parallelism
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @return The parallelism store.
     */
    @VisibleForTesting
    static VertexParallelismStore computeReactiveModeVertexParallelismStore(
            Iterable<JobVertex> vertices,
            Function<JobVertex, Integer> defaultMaxParallelismFunc,
            boolean adjustParallelism) {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        for (JobVertex vertex : vertices) {
            // if no max parallelism was configured by the user, we calculate and set a default
            final int maxParallelism =
                    vertex.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT
                            ? defaultMaxParallelismFunc.apply(vertex)
                            : vertex.getMaxParallelism();
            // If the parallelism has already been adjusted, respect what has been configured in the
            // vertex. Otherwise, scale it to the max parallelism to attempt to be "as parallel as
            // possible"
            final int parallelism;
            if (adjustParallelism) {
                parallelism = maxParallelism;
            } else {
                parallelism = vertex.getParallelism();
            }

            VertexParallelismInformation parallelismInfo =
                    new DefaultVertexParallelismInfo(
                            parallelism,
                            maxParallelism,
                            // Allow rescaling if the new desired max parallelism
                            // is not less than what was declared here during scheduling.
                            // This prevents the situation where more resources are requested
                            // based on the computed default, when actually fewer are necessary.
                            (newMax) ->
                                    newMax >= maxParallelism
                                            ? Optional.empty()
                                            : Optional.of(
                                                    "Cannot lower max parallelism in Reactive mode."));
            store.setParallelismInfo(vertex.getID(), parallelismInfo);
        }

        return store;
    }

    /**
     * Creates the parallelism store that should be used for determining scheduling requirements,
     * which may choose different parallelisms than set in the {@link JobGraph} depending on the
     * execution mode.
     *
     * @param jobGraph The job graph for execution.
     * @param executionMode The mode of scheduler execution.
     * @return The parallelism store.
     */
    private static VertexParallelismStore computeVertexParallelismStore(
            JobGraph jobGraph, SchedulerExecutionMode executionMode) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            return computeReactiveModeVertexParallelismStore(
                    jobGraph.getVertices(), SchedulerBase::getDefaultMaxParallelism, true);
        }
        return SchedulerBase.computeVertexParallelismStore(jobGraph);
    }

    /**
     * Creates the parallelism store that should be used to build the {@link ExecutionGraph}, which
     * will respect the vertex parallelism of the passed {@link JobGraph} in all execution modes.
     *
     * @param jobGraph The job graph for execution.
     * @param executionMode The mode of scheduler execution.
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @return The parallelism store.
     */
    @VisibleForTesting
    static VertexParallelismStore computeVertexParallelismStoreForExecution(
            JobGraph jobGraph,
            SchedulerExecutionMode executionMode,
            Function<JobVertex, Integer> defaultMaxParallelismFunc) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            return computeReactiveModeVertexParallelismStore(
                    jobGraph.getVertices(), defaultMaxParallelismFunc, false);
        }
        return SchedulerBase.computeVertexParallelismStore(
                jobGraph.getVertices(), defaultMaxParallelismFunc);
    }

    private void newResourcesAvailable(Collection<? extends PhysicalSlot> physicalSlots) {
        state.tryRun(
                ResourceListener.class,
                ResourceListener::onNewResourcesAvailable,
                "newResourcesAvailable");
    }

    @Override
    public void startScheduling() {
        checkIdleSlotTimeout();
        state.as(Created.class)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Can only start scheduling when being in Created state."))
                .startScheduling();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        LOG.debug("Closing the AdaptiveScheduler. Trying to suspend the current job execution.");

        state.suspend(new FlinkException("AdaptiveScheduler is being stopped."));

        Preconditions.checkState(
                state instanceof Finished,
                "Scheduler state should be finished after calling state.suspend.");

        backgroundTask.abort();
        // wait for the background task to finish and then close services
        return FutureUtils.composeAfterwards(
                FutureUtils.runAfterwardsAsync(
                        backgroundTask.getTerminationFuture(),
                        () -> stopCheckpointServicesSafely(jobTerminationFuture.get()),
                        getMainThreadExecutor()),
                checkpointsCleaner::closeAsync);
    }

    private void stopCheckpointServicesSafely(JobStatus terminalState) {
        LOG.debug("Stopping the checkpoint services with state {}.", terminalState);

        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(terminalState, checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(terminalState).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            LOG.warn("Failed to stop checkpoint services.", exception);
        }
    }

    @Override
    public void cancel() {
        state.cancel();
    }

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return jobTerminationFuture;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        final FailureEnricher.Context ctx =
                DefaultFailureEnricherContext.forGlobalFailure(
                        jobInformation.getJobID(),
                        jobInformation.getName(),
                        jobManagerJobMetricGroup,
                        ioExecutor,
                        userCodeClassLoader);
        final CompletableFuture<Map<String, String>> failureLabels =
                FailureEnricherUtils.labelFailure(
                        cause, ctx, getMainThreadExecutor(), failureEnrichers);
        state.handleGlobalFailure(cause, failureLabels);
    }

    private CompletableFuture<Map<String, String>> labelFailure(
            final TaskExecutionStateTransition taskExecutionStateTransition) {
        if (taskExecutionStateTransition.getExecutionState() == ExecutionState.FAILED
                && !failureEnrichers.isEmpty()) {
            final Throwable cause = taskExecutionStateTransition.getError(userCodeClassLoader);
            final Context ctx =
                    DefaultFailureEnricherContext.forTaskFailure(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobManagerJobMetricGroup,
                            ioExecutor,
                            userCodeClassLoader);
            return FailureEnricherUtils.labelFailure(
                    cause, ctx, getMainThreadExecutor(), failureEnrichers);
        }
        return FailureEnricherUtils.EMPTY_FAILURE_LABELS;
    }

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.updateTaskExecutionState(
                                        taskExecutionState, labelFailure(taskExecutionState)),
                        "updateTaskExecutionState")
                .orElse(false);
    }

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestNextInputSplit(
                                        vertexID, executionAttempt),
                        "requestNextInputSplit")
                .orElseThrow(
                        () -> new IOException("Scheduler is currently not executing the job."));
    }

    @Override
    public ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestPartitionState(
                                        intermediateResultId, resultPartitionId),
                        "requestPartitionState")
                .orElseThrow(() -> new PartitionProducerDisposedException(resultPartitionId));
    }

    @Override
    public ExecutionGraphInfo requestJob() {
        return new ExecutionGraphInfo(state.getJob(), exceptionHistory.toArrayList());
    }

    @Override
    public CheckpointStatsSnapshot requestCheckpointStats() {
        return state.getJob().getCheckpointStatsSnapshot();
    }

    @Override
    public void archiveFailure(RootExceptionHistoryEntry failure) {
        exceptionHistory.add(failure);
    }

    @Override
    public JobStatus requestJobStatus() {
        return state.getJobStatus();
    }

    @Override
    public JobDetails requestJobDetails() {
        return JobDetails.createDetailsForJob(state.getJob());
    }

    @Override
    public KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        final Optional<StateWithExecutionGraph> asOptional =
                state.as(StateWithExecutionGraph.class);

        if (asOptional.isPresent()) {
            return asOptional.get().requestKvStateLocation(jobId, registrationName);
        } else {
            throw new UnknownKvStateLocation(registrationName);
        }
    }

    @Override
    public void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateRegistered(
                                jobId,
                                jobVertexId,
                                keyGroupRange,
                                registrationName,
                                kvStateId,
                                kvStateServerAddress),
                "notifyKvStateRegistered");
    }

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateUnregistered(
                                jobId, jobVertexId, keyGroupRange, registrationName),
                "notifyKvStateUnregistered");
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.updateAccumulators(accumulatorSnapshot),
                "updateAccumulators");
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob, SavepointFormatType formatType) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.triggerSavepoint(
                                        targetDirectory, cancelJob, formatType),
                        "triggerSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.triggerCheckpoint(checkpointType),
                        "triggerCheckpoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.acknowledgeCheckpoint(
                                jobID,
                                executionAttemptID,
                                checkpointId,
                                checkpointMetrics,
                                checkpointState),
                "acknowledgeCheckpoint");
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyEndOfData(executionAttemptID),
                "notifyEndOfData");
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.reportCheckpointMetrics(
                                executionAttemptID, checkpointId, checkpointMetrics),
                "reportCheckpointMetrics");
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph -> stateWithExecutionGraph.declineCheckpoint(decline),
                "declineCheckpoint");
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable String targetDirectory, boolean terminate, SavepointFormatType formatType) {
        return state.tryCall(
                        Executing.class,
                        executing ->
                                executing.stopWithSavepoint(targetDirectory, terminate, formatType),
                        "stopWithSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException {
        final StateWithExecutionGraph stateWithExecutionGraph =
                state.as(StateWithExecutionGraph.class)
                        .orElseThrow(
                                () ->
                                        new TaskNotRunningException(
                                                "Task is not known or in state running on the JobManager."));

        stateWithExecutionGraph.deliverOperatorEventToCoordinator(taskExecution, operator, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.deliverCoordinationRequestToCoordinator(
                                        operator, request),
                        "deliverCoordinationRequestToCoordinator")
                .orElseGet(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException(
                                                "Coordinator of operator "
                                                        + operator
                                                        + " does not exist")));
    }

    @Override
    public JobResourceRequirements requestJobResourceRequirements() {
        final JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
        for (JobInformation.VertexInformation vertex : jobInformation.getVertices()) {
            builder.setParallelismForJobVertex(
                    vertex.getJobVertexID(), vertex.getMinParallelism(), vertex.getParallelism());
        }
        return builder.build();
    }

    @Override
    public void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            throw new UnsupportedOperationException(
                    "Cannot change the parallelism of a job running in reactive mode.");
        }
        final Optional<VertexParallelismStore> maybeUpdateVertexParallelismStore =
                DefaultVertexParallelismStore.applyJobResourceRequirements(
                        jobInformation.getVertexParallelismStore(), jobResourceRequirements);
        if (maybeUpdateVertexParallelismStore.isPresent()) {
            this.jobInformation =
                    new JobGraphJobInformation(jobGraph, maybeUpdateVertexParallelismStore.get());
            declareDesiredResources();
            state.tryRun(
                    ResourceListener.class,
                    ResourceListener::onNewResourceRequirements,
                    "Current state does not react to desired parallelism changes.");
        }
    }

    // ----------------------------------------------------------------

    @Override
    public boolean hasDesiredResources() {
        final Collection<? extends SlotInfo> freeSlots =
                declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation();
        return hasDesiredResources(desiredResources, freeSlots);
    }

    @VisibleForTesting
    static boolean hasDesiredResources(
            ResourceCounter desiredResources, Collection<? extends SlotInfo> freeSlots) {
        ResourceCounter outstandingResources = desiredResources;
        final Iterator<? extends SlotInfo> slotIterator = freeSlots.iterator();

        while (!outstandingResources.isEmpty() && slotIterator.hasNext()) {
            final SlotInfo slotInfo = slotIterator.next();
            final ResourceProfile resourceProfile = slotInfo.getResourceProfile();

            if (outstandingResources.containsResource(resourceProfile)) {
                outstandingResources = outstandingResources.subtract(resourceProfile, 1);
            } else {
                outstandingResources = outstandingResources.subtract(ResourceProfile.UNKNOWN, 1);
            }
        }

        return outstandingResources.isEmpty();
    }

    @Override
    public boolean hasSufficientResources() {
        return slotAllocator
                .determineParallelism(jobInformation, declarativeSlotPool.getAllSlotsInformation())
                .isPresent();
    }

    private JobSchedulingPlan determineParallelism(
            SlotAllocator slotAllocator, @Nullable ExecutionGraph previousExecutionGraph)
            throws NoResourceAvailableException {

        return slotAllocator
                .determineParallelismAndCalculateAssignment(
                        jobInformation,
                        declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation(),
                        JobAllocationsInformation.fromGraph(previousExecutionGraph))
                .orElseThrow(
                        () ->
                                new NoResourceAvailableException(
                                        "Not enough resources available for scheduling."));
    }

    @Override
    public ArchivedExecutionGraph getArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createSparseArchivedExecutionGraphWithJobVertices(
                jobInformation.getJobID(),
                jobInformation.getName(),
                jobStatus,
                cause,
                jobInformation.getCheckpointingSettings(),
                initializationTimestamp,
                jobGraph.getVertices(),
                initialParallelismStore);
    }

    @Override
    public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
        declareDesiredResources();

        transitionToState(
                new WaitingForResources.Factory(
                        this,
                        LOG,
                        this.initialResourceAllocationTimeout,
                        this.resourceStabilizationTimeout,
                        previousExecutionGraph));
    }

    private void declareDesiredResources() {
        final ResourceCounter newDesiredResources = calculateDesiredResources();

        if (!newDesiredResources.equals(this.desiredResources)) {
            this.desiredResources = newDesiredResources;
            declarativeSlotPool.setResourceRequirements(this.desiredResources);
        }
    }

    private ResourceCounter calculateDesiredResources() {
        return slotAllocator.calculateRequiredSlots(jobInformation.getVertices());
    }

    @Override
    public void goToExecuting(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            List<ExceptionHistoryEntry> failureCollection) {
        transitionToState(
                new Executing.Factory(
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        this,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public void goToCanceling(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            List<ExceptionHistoryEntry> failureCollection) {

        transitionToState(
                new Canceling.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public void goToRestarting(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Duration backoffTime,
            List<ExceptionHistoryEntry> failureCollection) {

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final int attemptNumber =
                    executionVertex.getCurrentExecutionAttempt().getAttemptNumber();

            this.vertexAttemptNumberStore.setAttemptCount(
                    executionVertex.getJobvertexId(),
                    executionVertex.getParallelSubtaskIndex(),
                    attemptNumber + 1);
        }

        transitionToState(
                new Restarting.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        backoffTime,
                        userCodeClassLoader,
                        failureCollection));
        numRestarts++;
    }

    @Override
    public void goToFailing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Throwable failureCause,
            List<ExceptionHistoryEntry> failureCollection) {
        transitionToState(
                new Failing.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        failureCause,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public CompletableFuture<String> goToStopWithSavepoint(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            CheckpointScheduling checkpointScheduling,
            CompletableFuture<String> savepointFuture,
            List<ExceptionHistoryEntry> failureCollection) {

        StopWithSavepoint stopWithSavepoint =
                transitionToState(
                        new StopWithSavepoint.Factory(
                                this,
                                executionGraph,
                                executionGraphHandler,
                                operatorCoordinatorHandler,
                                checkpointScheduling,
                                LOG,
                                userCodeClassLoader,
                                savepointFuture,
                                failureCollection));
        return stopWithSavepoint.getOperationFuture();
    }

    @Override
    public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        transitionToState(new Finished.Factory(this, archivedExecutionGraph, LOG));
    }

    @Override
    public void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph) {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithAvailableResourcesFuture =
                        createExecutionGraphWithAvailableResourcesAsync(previousExecutionGraph);
        transitionToState(
                new CreatingExecutionGraph.Factory(
                        this,
                        executionGraphWithAvailableResourcesFuture,
                        LOG,
                        previousExecutionGraph));
    }

    private CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
            createExecutionGraphWithAvailableResourcesAsync(
                    @Nullable ExecutionGraph previousExecutionGraph) {
        final JobSchedulingPlan schedulingPlan;
        final VertexParallelismStore adjustedParallelismStore;

        try {
            schedulingPlan = determineParallelism(slotAllocator, previousExecutionGraph);
            JobGraph adjustedJobGraph = jobInformation.copyJobGraph();

            for (JobVertex vertex : adjustedJobGraph.getVertices()) {
                JobVertexID id = vertex.getID();

                // use the determined "available parallelism" to use
                // the resources we have access to
                vertex.setParallelism(schedulingPlan.getVertexParallelism().getParallelism(id));
            }

            // use the originally configured max parallelism
            // as the default for consistent runs
            adjustedParallelismStore =
                    computeVertexParallelismStoreForExecution(
                            adjustedJobGraph,
                            executionMode,
                            (vertex) -> {
                                VertexParallelismInformation vertexParallelismInfo =
                                        initialParallelismStore.getParallelismInfo(vertex.getID());
                                return vertexParallelismInfo.getMaxParallelism();
                            });
        } catch (Exception exception) {
            return FutureUtils.completedExceptionally(exception);
        }

        return createExecutionGraphAndRestoreStateAsync(adjustedParallelismStore)
                .thenApply(
                        executionGraph ->
                                CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create(
                                        executionGraph, schedulingPlan));
    }

    @Override
    public CreatingExecutionGraph.AssignmentResult tryToAssignSlots(
            CreatingExecutionGraph.ExecutionGraphWithVertexParallelism
                    executionGraphWithVertexParallelism) {
        final ExecutionGraph executionGraph =
                executionGraphWithVertexParallelism.getExecutionGraph();

        executionGraph.start(componentMainThreadExecutor);
        executionGraph.transitionToRunning();

        executionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this));

        final JobSchedulingPlan jobSchedulingPlan =
                executionGraphWithVertexParallelism.getJobSchedulingPlan();
        return slotAllocator
                .tryReserveResources(jobSchedulingPlan)
                .map(reservedSlots -> assignSlotsToExecutionGraph(executionGraph, reservedSlots))
                .map(CreatingExecutionGraph.AssignmentResult::success)
                .orElseGet(CreatingExecutionGraph.AssignmentResult::notPossible);
    }

    @Nonnull
    private ExecutionGraph assignSlotsToExecutionGraph(
            ExecutionGraph executionGraph, ReservedSlots reservedSlots) {
        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final LogicalSlot assignedSlot = reservedSlots.getSlotFor(executionVertex.getID());
            final CompletableFuture<Void> registrationFuture =
                    executionVertex
                            .getCurrentExecutionAttempt()
                            .registerProducedPartitions(assignedSlot.getTaskManagerLocation());
            Preconditions.checkState(
                    registrationFuture.isDone(),
                    "Partition registration must be completed immediately for reactive mode");

            executionVertex.tryAssignResource(assignedSlot);
        }

        return executionGraph;
    }

    private CompletableFuture<ExecutionGraph> createExecutionGraphAndRestoreStateAsync(
            VertexParallelismStore adjustedParallelismStore) {
        backgroundTask.abort();

        backgroundTask =
                backgroundTask.runAfter(
                        () -> createExecutionGraphAndRestoreState(adjustedParallelismStore),
                        ioExecutor);

        return FutureUtils.switchExecutor(
                backgroundTask.getResultFuture(), getMainThreadExecutor());
    }

    @Nonnull
    private ExecutionGraph createExecutionGraphAndRestoreState(
            VertexParallelismStore adjustedParallelismStore) throws Exception {
        return executionGraphFactory.createAndRestoreExecutionGraph(
                jobInformation.copyJobGraph(),
                completedCheckpointStore,
                checkpointsCleaner,
                checkpointIdCounter,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                initializationTimestamp,
                vertexAttemptNumberStore,
                adjustedParallelismStore,
                deploymentTimeMetrics,
                // adaptive scheduler works in streaming mode, actually it only
                // supports must be pipelined result partition, mark partition finish is
                // no need.
                rp -> false,
                LOG);
    }

    /**
     * In regular mode, rescale the job if added resource meets {@link
     * JobManagerOptions#MIN_PARALLELISM_INCREASE}. In force mode rescale if the parallelism has
     * changed.
     */
    @Override
    public boolean shouldRescale(ExecutionGraph executionGraph, boolean forceRescale) {
        final Optional<VertexParallelism> maybeNewParallelism =
                slotAllocator.determineParallelism(
                        jobInformation, declarativeSlotPool.getAllSlotsInformation());
        return maybeNewParallelism
                .filter(
                        vertexParallelism -> {
                            RescalingController rescalingControllerToUse =
                                    forceRescale ? forceRescalingController : rescalingController;
                            return rescalingControllerToUse.shouldRescale(
                                    getCurrentParallelism(executionGraph), vertexParallelism);
                        })
                .isPresent();
    }

    private static VertexParallelism getCurrentParallelism(ExecutionGraph executionGraph) {
        return new VertexParallelism(
                executionGraph.getAllVertices().values().stream()
                        .collect(
                                Collectors.toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism)));
    }

    @Override
    public void onFinished(ArchivedExecutionGraph archivedExecutionGraph) {

        @Nullable
        final Throwable optionalFailure =
                archivedExecutionGraph.getFailureInfo() != null
                        ? archivedExecutionGraph.getFailureInfo().getException()
                        : null;
        LOG.info(
                "Job {} reached terminal state {}.",
                archivedExecutionGraph.getJobID(),
                archivedExecutionGraph.getState(),
                optionalFailure);

        jobTerminationFuture.complete(archivedExecutionGraph.getState());
    }

    @Override
    public FailureResult howToHandleFailure(Throwable failure) {
        if (ExecutionFailureHandler.isUnrecoverableError(failure)) {
            return FailureResult.canNotRestart(
                    new JobException("The failure is not recoverable", failure));
        }

        restartBackoffTimeStrategy.notifyFailure(failure);
        if (restartBackoffTimeStrategy.canRestart()) {
            return FailureResult.canRestart(
                    failure, Duration.ofMillis(restartBackoffTimeStrategy.getBackoffTime()));
        } else {
            return FailureResult.canNotRestart(
                    new JobException(
                            "Recovery is suppressed by " + restartBackoffTimeStrategy, failure));
        }
    }

    @Override
    public Executor getIOExecutor() {
        return ioExecutor;
    }

    @Override
    public ComponentMainThreadExecutor getMainThreadExecutor() {
        return componentMainThreadExecutor;
    }

    @Override
    public JobManagerJobMetricGroup getMetricGroup() {
        return jobManagerJobMetricGroup;
    }

    @Override
    public boolean isState(State expectedState) {
        return expectedState == this.state;
    }

    @Override
    public void runIfState(State expectedState, Runnable action) {
        if (isState(expectedState)) {
            try {
                action.run();
            } catch (Throwable t) {
                fatalErrorHandler.onFatalError(t);
            }
        } else {
            LOG.debug(
                    "Ignoring scheduled action because expected state {} is not the actual state {}.",
                    expectedState,
                    state);
        }
    }

    @Override
    public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
        return componentMainThreadExecutor.schedule(
                () -> runIfState(expectedState, action), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ----------------------------------------------------------------

    /**
     * Transition the scheduler to another state. This method guards against state transitions while
     * there is already a transition ongoing. This effectively means that you can not call this
     * method from a State constructor or State#onLeave.
     *
     * @param targetState State to transition to
     * @param <T> Type of the target state
     * @return A target state instance
     */
    @VisibleForTesting
    <T extends State> T transitionToState(StateFactory<T> targetState) {
        Preconditions.checkState(
                !isTransitioningState,
                "State transitions must not be triggered while another state transition is in progress.");
        Preconditions.checkState(
                state.getClass() != targetState.getStateClass(),
                "Attempted to transition into the very state the scheduler is already in.");
        componentMainThreadExecutor.assertRunningInMainThread();

        try {
            isTransitioningState = true;
            LOG.debug(
                    "Transition from state {} to {}.",
                    state.getClass().getSimpleName(),
                    targetState.getStateClass().getSimpleName());

            final JobStatus previousJobStatus = state.getJobStatus();

            state.onLeave(targetState.getStateClass());
            T targetStateInstance = targetState.getState();
            state = targetStateInstance;

            final JobStatus newJobStatus = state.getJobStatus();

            if (previousJobStatus != newJobStatus) {
                final long timestamp = System.currentTimeMillis();
                jobStatusListeners.forEach(
                        listener ->
                                listener.jobStatusChanges(
                                        jobInformation.getJobID(), newJobStatus, timestamp));
            }

            return targetStateInstance;
        } finally {
            isTransitioningState = false;
        }
    }

    @VisibleForTesting
    State getState() {
        return state;
    }

    /**
     * Check for slots that are idle for more than {@link JobManagerOptions#SLOT_IDLE_TIMEOUT} and
     * release them back to the ResourceManager.
     */
    private void checkIdleSlotTimeout() {
        if (getState().getJobStatus().isGloballyTerminalState()) {
            // Job has reached the terminal state, so we can return all slots to the ResourceManager
            // to speed things up because we no longer need them. This optimization lets us skip
            // waiting for the slot pool service to close.
            for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
                declarativeSlotPool.releaseSlot(
                        slotInfo.getAllocationId(),
                        new FlinkException(
                                "Returning slots to their owners, because the job has reached a globally terminal state."));
            }
            return;
        } else if (getState().getJobStatus().isTerminalState()) {
            // do nothing
            // prevent idleness check running again while scheduler was already shut down
            // don't release slots because JobMaster may want to hold on to slots in case
            // it re-acquires leadership
            return;
        }
        declarativeSlotPool.releaseIdleSlots(System.currentTimeMillis());
        getMainThreadExecutor()
                .schedule(
                        this::checkIdleSlotTimeout,
                        slotIdleTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);
    }
}
