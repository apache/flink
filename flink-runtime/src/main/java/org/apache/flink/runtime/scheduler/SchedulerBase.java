/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobStatusProvider;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.metrics.DeploymentStateTimeMetrics;
import org.apache.flink.runtime.scheduler.metrics.JobStatusMetrics;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationHandlerImpl;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationManager;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class which can be used to implement {@link SchedulerNG}. */
public abstract class SchedulerBase implements SchedulerNG, CheckpointScheduling {

    private final Logger log;

    private final JobGraph jobGraph;

    private final ExecutionGraph executionGraph;

    private final SchedulingTopology schedulingTopology;

    protected final StateLocationRetriever stateLocationRetriever;

    protected final InputsLocationsRetriever inputsLocationsRetriever;

    private final CompletedCheckpointStore completedCheckpointStore;

    private final CheckpointsCleaner checkpointsCleaner;

    private final CheckpointIDCounter checkpointIdCounter;

    protected final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    protected final ExecutionVertexVersioner executionVertexVersioner;

    private final KvStateHandler kvStateHandler;

    private final ExecutionGraphHandler executionGraphHandler;

    protected final OperatorCoordinatorHandler operatorCoordinatorHandler;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private final BoundedFIFOQueue<RootExceptionHistoryEntry> exceptionHistory;

    private final ExecutionGraphFactory executionGraphFactory;

    private final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings;

    private final DeploymentStateTimeMetrics deploymentStateTimeMetrics;

    private final VertexEndOfDataListener vertexEndOfDataListener;

    public SchedulerBase(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final ExecutionVertexVersioner executionVertexVersioner,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final ExecutionGraphFactory executionGraphFactory,
            final VertexParallelismStore vertexParallelismStore)
            throws Exception {

        this.log = checkNotNull(log);
        this.jobGraph = checkNotNull(jobGraph);
        this.executionGraphFactory = executionGraphFactory;

        this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.mainThreadExecutor = mainThreadExecutor;

        this.checkpointsCleaner = checkpointsCleaner;
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph,
                        jobMasterConfiguration,
                        checkNotNull(checkpointRecoveryFactory),
                        ioExecutor,
                        log);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkNotNull(checkpointRecoveryFactory));

        this.jobStatusMetricsSettings =
                MetricOptions.JobStatusMetricsSettings.fromConfiguration(jobMasterConfiguration);
        this.deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

        this.executionGraph =
                createAndRestoreExecutionGraph(
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        initializationTimestamp,
                        mainThreadExecutor,
                        jobStatusListener,
                        vertexParallelismStore);

        this.schedulingTopology = executionGraph.getSchedulingTopology();

        stateLocationRetriever =
                executionVertexId ->
                        getExecutionVertex(executionVertexId).getPreferredLocationBasedOnState();
        inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);

        this.kvStateHandler = new KvStateHandler(executionGraph);
        this.executionGraphHandler =
                new ExecutionGraphHandler(executionGraph, log, ioExecutor, this.mainThreadExecutor);

        this.operatorCoordinatorHandler =
                new DefaultOperatorCoordinatorHandler(executionGraph, this::handleGlobalFailure);
        operatorCoordinatorHandler.initializeOperatorCoordinators(this.mainThreadExecutor);

        this.exceptionHistory =
                new BoundedFIFOQueue<>(
                        jobMasterConfiguration.getInteger(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));

        this.vertexEndOfDataListener = new VertexEndOfDataListener(executionGraph);
    }

    private void shutDownCheckpointServices(JobStatus jobStatus) {
        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(jobStatus, checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(jobStatus).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            log.error("Error while shutting down checkpoint services.", exception);
        }
    }

    private static int normalizeParallelism(int parallelism) {
        if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            return 1;
        }
        return parallelism;
    }

    /**
     * Get a default value to use for a given vertex's max parallelism if none was specified.
     *
     * @param vertex the vertex to compute a default max parallelism for
     * @return the computed max parallelism
     */
    public static int getDefaultMaxParallelism(JobVertex vertex) {
        return KeyGroupRangeAssignment.computeDefaultMaxParallelism(
                normalizeParallelism(vertex.getParallelism()));
    }

    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices, Function<JobVertex, Integer> defaultMaxParallelismFunc) {
        return computeVertexParallelismStore(
                vertices, defaultMaxParallelismFunc, SchedulerBase::normalizeParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms, with a custom function for
     * default max parallelism calculation and a custom function for normalizing vertex parallelism.
     *
     * @param vertices the vertices to compute parallelism for
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @param normalizeParallelismFunc a function for normalizing vertex parallelism
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices,
            Function<JobVertex, Integer> defaultMaxParallelismFunc,
            Function<Integer, Integer> normalizeParallelismFunc) {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        for (JobVertex vertex : vertices) {
            int parallelism = normalizeParallelismFunc.apply(vertex.getParallelism());

            int maxParallelism = vertex.getMaxParallelism();
            final boolean autoConfigured;
            // if no max parallelism was configured by the user, we calculate and set a default
            if (maxParallelism == JobVertex.MAX_PARALLELISM_DEFAULT) {
                maxParallelism = defaultMaxParallelismFunc.apply(vertex);
                autoConfigured = true;
            } else {
                autoConfigured = false;
            }

            VertexParallelismInformation parallelismInfo =
                    new DefaultVertexParallelismInfo(
                            parallelism,
                            maxParallelism,
                            // Allow rescaling if the max parallelism was not set explicitly by the
                            // user
                            (newMax) ->
                                    autoConfigured
                                            ? Optional.empty()
                                            : Optional.of(
                                                    "Cannot override a configured max parallelism."));
            store.setParallelismInfo(vertex.getID(), parallelismInfo);
        }

        return store;
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms.
     *
     * @param vertices the vertices to compute parallelism for
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices) {
        return computeVertexParallelismStore(vertices, SchedulerBase::getDefaultMaxParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all vertices of a given job graph, which will
     * set defaults and ensure that the returned store contains valid parallelisms.
     *
     * @param jobGraph the job graph to retrieve vertices from
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(JobGraph jobGraph) {
        return computeVertexParallelismStore(jobGraph.getVertices());
    }

    private ExecutionGraph createAndRestoreExecutionGraph(
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            JobStatusListener jobStatusListener,
            VertexParallelismStore vertexParallelismStore)
            throws Exception {

        final ExecutionGraph newExecutionGraph =
                executionGraphFactory.createAndRestoreExecutionGraph(
                        jobGraph,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.fromJobType(
                                jobGraph.getJobType()),
                        initializationTimestamp,
                        new DefaultVertexAttemptNumberStore(),
                        vertexParallelismStore,
                        deploymentStateTimeMetrics,
                        getMarkPartitionFinishedStrategy(),
                        log);

        newExecutionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this));
        newExecutionGraph.registerJobStatusListener(jobStatusListener);
        newExecutionGraph.start(mainThreadExecutor);

        return newExecutionGraph;
    }

    protected void resetForNewExecutions(final Collection<ExecutionVertexID> vertices) {
        vertices.stream().forEach(this::resetForNewExecution);
    }

    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        getExecutionVertex(executionVertexId).resetForNewExecution();
    }

    protected void restoreState(
            final Set<ExecutionVertexID> vertices, final boolean isGlobalRecovery)
            throws Exception {
        vertexEndOfDataListener.restoreVertices(vertices);

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            // batch failover case - we only need to notify the OperatorCoordinators,
            // not do any actual state restore
            if (isGlobalRecovery) {
                notifyCoordinatorsOfEmptyGlobalRestore();
            } else {
                notifyCoordinatorsOfSubtaskRestore(
                        getInvolvedExecutionJobVerticesAndSubtasks(vertices),
                        OperatorCoordinator.NO_CHECKPOINT);
            }
            return;
        }

        // if there is checkpointed state, reload it into the executions

        // abort pending checkpoints to
        // i) enable new checkpoint triggering without waiting for last checkpoint expired.
        // ii) ensure the EXACTLY_ONCE semantics if needed.
        checkpointCoordinator.abortPendingCheckpoints(
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

        if (isGlobalRecovery) {
            final Set<ExecutionJobVertex> jobVerticesToRestore =
                    getInvolvedExecutionJobVertices(vertices);

            checkpointCoordinator.restoreLatestCheckpointedStateToAll(jobVerticesToRestore, true);

        } else {
            final Map<ExecutionJobVertex, IntArrayList> subtasksToRestore =
                    getInvolvedExecutionJobVerticesAndSubtasks(vertices);

            final OptionalLong restoredCheckpointId =
                    checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(
                            subtasksToRestore.keySet());

            // Ideally, the Checkpoint Coordinator would call OperatorCoordinator.resetSubtask, but
            // the Checkpoint Coordinator is not aware of subtasks in a local failover. It always
            // assigns state to all subtasks, and for the subtask execution attempts that are still
            // running (or not waiting to be deployed) the state assignment has simply no effect.
            // Because of that, we need to do the "subtask restored" notification here.
            // Once the Checkpoint Coordinator is properly aware of partial (region) recovery,
            // this code should move into the Checkpoint Coordinator.
            final long checkpointId =
                    restoredCheckpointId.orElse(OperatorCoordinator.NO_CHECKPOINT);
            notifyCoordinatorsOfSubtaskRestore(subtasksToRestore, checkpointId);
        }
    }

    private void notifyCoordinatorsOfSubtaskRestore(
            final Map<ExecutionJobVertex, IntArrayList> restoredSubtasks, final long checkpointId) {

        for (final Map.Entry<ExecutionJobVertex, IntArrayList> vertexSubtasks :
                restoredSubtasks.entrySet()) {
            final ExecutionJobVertex jobVertex = vertexSubtasks.getKey();
            final IntArrayList subtasks = vertexSubtasks.getValue();

            final Collection<OperatorCoordinatorHolder> coordinators =
                    jobVertex.getOperatorCoordinators();
            if (coordinators.isEmpty()) {
                continue;
            }

            while (!subtasks.isEmpty()) {
                final int subtask =
                        subtasks.removeLast(); // this is how IntArrayList implements iterations
                for (final OperatorCoordinatorHolder opCoordinator : coordinators) {
                    opCoordinator.subtaskReset(subtask, checkpointId);
                }
            }
        }
    }

    private void notifyCoordinatorsOfEmptyGlobalRestore() throws Exception {
        for (final ExecutionJobVertex ejv : getExecutionGraph().getAllVertices().values()) {
            if (!ejv.isInitialized()) {
                continue;
            }
            for (final OperatorCoordinatorHolder coordinator : ejv.getOperatorCoordinators()) {
                coordinator.resetToCheckpoint(OperatorCoordinator.NO_CHECKPOINT, null);
            }
        }
    }

    private Set<ExecutionJobVertex> getInvolvedExecutionJobVertices(
            final Set<ExecutionVertexID> executionVertices) {

        final Set<ExecutionJobVertex> tasks = new HashSet<>();
        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            tasks.add(executionVertex.getJobVertex());
        }
        return tasks;
    }

    private Map<ExecutionJobVertex, IntArrayList> getInvolvedExecutionJobVerticesAndSubtasks(
            final Set<ExecutionVertexID> executionVertices) {

        final HashMap<ExecutionJobVertex, IntArrayList> result = new HashMap<>();

        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            final IntArrayList subtasks =
                    result.computeIfAbsent(
                            executionVertex.getJobVertex(), (key) -> new IntArrayList(32));
            subtasks.add(executionVertex.getParallelSubtaskIndex());
        }

        return result;
    }

    protected void transitionToScheduled(final List<ExecutionVertexID> verticesToDeploy) {
        verticesToDeploy.forEach(
                executionVertexId ->
                        getExecutionVertex(executionVertexId)
                                .getCurrentExecutionAttempt()
                                .transitionState(ExecutionState.SCHEDULED));
    }

    protected void setGlobalFailureCause(@Nullable final Throwable cause, long timestamp) {
        if (cause != null) {
            executionGraph.initFailureCause(cause, timestamp);
        }
    }

    protected ComponentMainThreadExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    protected void failJob(
            Throwable cause, long timestamp, CompletableFuture<Map<String, String>> failureLabels) {
        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.failJob(cause, timestamp);
        getJobTerminationFuture().thenRun(() -> archiveGlobalFailure(cause, failureLabels));
    }

    protected final SchedulingTopology getSchedulingTopology() {
        return schedulingTopology;
    }

    protected final ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        return executionGraph.getResultPartitionAvailabilityChecker();
    }

    protected final void transitionToRunning() {
        executionGraph.transitionToRunning();
    }

    public ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
        return executionGraph
                .getAllVertices()
                .get(executionVertexId.getJobVertexId())
                .getTaskVertices()[executionVertexId.getSubtaskIndex()];
    }

    public ExecutionJobVertex getExecutionJobVertex(final JobVertexID jobVertexId) {
        return executionGraph.getAllVertices().get(jobVertexId);
    }

    protected JobGraph getJobGraph() {
        return jobGraph;
    }

    protected abstract long getNumberOfRestarts();

    protected MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        // blocking partition always need mark finished.
        return ResultPartitionType::isBlockingOrBlockingPersistentResultPartition;
    }

    private Map<ExecutionVertexID, ExecutionVertexVersion> incrementVersionsOfAllVertices() {
        return executionVertexVersioner.recordVertexModifications(
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet()));
    }

    protected abstract void cancelAllPendingSlotRequestsInternal();

    protected void transitionExecutionGraphState(
            final JobStatus current, final JobStatus newState) {
        executionGraph.transitionState(current, newState);
    }

    @VisibleForTesting
    CheckpointCoordinator getCheckpointCoordinator() {
        return executionGraph.getCheckpointCoordinator();
    }

    /**
     * ExecutionGraph is exposed to make it easier to rework tests to be based on the new scheduler.
     * ExecutionGraph is expected to be used only for state check. Yet at the moment, before all the
     * actions are factored out from ExecutionGraph and its sub-components, some actions may still
     * be performed directly on it.
     */
    @VisibleForTesting
    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    public final void startScheduling() {
        mainThreadExecutor.assertRunningInMainThread();
        registerJobMetrics(
                jobManagerJobMetricGroup,
                executionGraph,
                this::getNumberOfRestarts,
                deploymentStateTimeMetrics,
                executionGraph::registerJobStatusListener,
                executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
                jobStatusMetricsSettings);
        operatorCoordinatorHandler.startAllOperatorCoordinators();
        startSchedulingInternal();
    }

    public static void registerJobMetrics(
            MetricGroup metrics,
            JobStatusProvider jobStatusProvider,
            Gauge<Long> numberOfRestarts,
            DeploymentStateTimeMetrics deploymentTimeMetrics,
            Consumer<JobStatusListener> jobStatusListenerRegistrar,
            long initializationTimestamp,
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings) {
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(jobStatusProvider));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(jobStatusProvider));
        metrics.gauge(MetricNames.NUM_RESTARTS, numberOfRestarts::getValue);
        metrics.gauge(MetricNames.FULL_RESTARTS, numberOfRestarts::getValue);

        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(initializationTimestamp, jobStatusMetricsSettings);
        jobStatusMetrics.registerMetrics(metrics);
        jobStatusListenerRegistrar.accept(jobStatusMetrics);

        deploymentTimeMetrics.registerMetrics(metrics);
    }

    protected abstract void startSchedulingInternal();

    @Override
    public CompletableFuture<Void> closeAsync() {
        mainThreadExecutor.assertRunningInMainThread();

        final FlinkException cause = new FlinkException("Scheduler is being stopped.");

        final CompletableFuture<Void> checkpointServicesShutdownFuture =
                FutureUtils.composeAfterwards(
                        executionGraph
                                .getTerminationFuture()
                                .thenAcceptAsync(
                                        this::shutDownCheckpointServices, getMainThreadExecutor()),
                        checkpointsCleaner::closeAsync);

        FutureUtils.assertNoException(checkpointServicesShutdownFuture);

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.suspend(cause);
        operatorCoordinatorHandler.disposeAllOperatorCoordinators();
        return checkpointServicesShutdownFuture;
    }

    @Override
    public void cancel() {
        mainThreadExecutor.assertRunningInMainThread();

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.cancel();
    }

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return executionGraph.getTerminationFuture();
    }

    protected final void archiveGlobalFailure(
            Throwable failure, CompletableFuture<Map<String, String>> failureLabels) {
        archiveGlobalFailure(
                failure,
                executionGraph.getStatusTimestamp(JobStatus.FAILED),
                failureLabels,
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toSet()));
    }

    private void archiveGlobalFailure(
            Throwable failure,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            Iterable<Execution> executions) {
        exceptionHistory.add(
                RootExceptionHistoryEntry.fromGlobalFailure(
                        failure, timestamp, failureLabels, executions));
        log.debug("Archive global failure.", failure);
    }

    protected final void archiveFromFailureHandlingResult(
            FailureHandlingResultSnapshot failureHandlingResult) {
        if (failureHandlingResult.getRootCauseExecution().isPresent()) {
            final Execution rootCauseExecution =
                    failureHandlingResult.getRootCauseExecution().get();

            final RootExceptionHistoryEntry rootEntry =
                    RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(
                            failureHandlingResult);
            exceptionHistory.add(rootEntry);

            log.debug(
                    "Archive local failure causing attempt {} to fail: {}",
                    rootCauseExecution.getAttemptId(),
                    rootEntry.getExceptionAsString());
        } else {
            archiveGlobalFailure(
                    failureHandlingResult.getRootCause(),
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels(),
                    failureHandlingResult.getConcurrentlyFailedExecution());
        }
    }

    @Override
    public final boolean updateTaskExecutionState(
            final TaskExecutionStateTransition taskExecutionState) {

        final ExecutionAttemptID attemptId = taskExecutionState.getID();
        final Execution execution = executionGraph.getRegisteredExecutions().get(attemptId);
        if (execution != null && executionGraph.updateState(taskExecutionState)) {
            onTaskExecutionStateUpdate(execution, taskExecutionState);
            return true;
        }

        return false;
    }

    private void onTaskExecutionStateUpdate(
            final Execution execution, final TaskExecutionStateTransition taskExecutionState) {

        // only notifies a state update if it's effective, namely it successfully
        // turns the execution state to the expected value.
        if (execution.getState() != taskExecutionState.getExecutionState()) {
            return;
        }

        // only notifies FINISHED and FAILED states which are needed at the moment.
        // can be refined in FLINK-14233 after the actions are factored out from ExecutionGraph.
        switch (taskExecutionState.getExecutionState()) {
            case FINISHED:
                onTaskFinished(execution, taskExecutionState.getIOMetrics());
                break;
            case FAILED:
                onTaskFailed(execution);
                break;
        }
    }

    protected abstract void onTaskFinished(final Execution execution, final IOMetrics ioMetrics);

    protected abstract void onTaskFailed(final Execution execution);

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestNextInputSplit(vertexID, executionAttempt);
    }

    @Override
    public ExecutionState requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {

        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestPartitionState(intermediateResultId, resultPartitionId);
    }

    @VisibleForTesting
    public Iterable<RootExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory.toArrayList();
    }

    @Override
    public ExecutionGraphInfo requestJob() {
        mainThreadExecutor.assertRunningInMainThread();
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createFrom(executionGraph), getExceptionHistory());
    }

    @Override
    public CheckpointStatsSnapshot requestCheckpointStats() {
        mainThreadExecutor.assertRunningInMainThread();
        return executionGraph.getCheckpointStatsSnapshot();
    }

    @Override
    public JobStatus requestJobStatus() {
        return executionGraph.getState();
    }

    @Override
    public JobDetails requestJobDetails() {
        mainThreadExecutor.assertRunningInMainThread();
        return JobDetails.createDetailsForJob(executionGraph);
    }

    @Override
    public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        return kvStateHandler.requestKvStateLocation(jobId, registrationName);
    }

    @Override
    public void notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateRegistered(
                jobId,
                jobVertexId,
                keyGroupRange,
                registrationName,
                kvStateId,
                kvStateServerAddress);
    }

    @Override
    public void notifyKvStateUnregistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateUnregistered(
                jobId, jobVertexId, keyGroupRange, registrationName);
    }

    @Override
    public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
        mainThreadExecutor.assertRunningInMainThread();

        executionGraph.updateAccumulators(accumulatorSnapshot);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, getJobId(), log);

        log.info(
                "Triggering {}savepoint for job {}.",
                cancelJob ? "cancel-with-" : "",
                jobGraph.getJobID());

        if (cancelJob) {
            stopCheckpointScheduler();
        }

        return checkpointCoordinator
                .triggerSavepoint(targetDirectory, formatType)
                .thenApply(CompletedCheckpoint::getExternalPointer)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                if (cancelJob) {
                                    startCheckpointScheduler();
                                }
                                throw new CompletionException(throwable);
                            } else if (cancelJob) {
                                log.info(
                                        "Savepoint stored in {}. Now cancelling {}.",
                                        path,
                                        jobGraph.getJobID());
                                cancel();
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        final JobID jobID = jobGraph.getJobID();
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(String.format("Job %s is not a streaming job.", jobID));
        }
        log.info("Triggering a manual checkpoint for job {}.", jobID);

        return checkpointCoordinator
                .triggerCheckpoint(checkpointType)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                throw new CompletionException(throwable);
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public void stopCheckpointScheduler() {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();
        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be stopped due to the CheckpointCoordinator being shutdown.");
        } else {
            checkpointCoordinator.stopCheckpointScheduler();
        }
    }

    @Override
    public void startCheckpointScheduler() {
        mainThreadExecutor.assertRunningInMainThread();
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be started due to the CheckpointCoordinator being shutdown.");
        } else if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
            try {
                checkpointCoordinator.startCheckpointScheduler();
            } catch (IllegalStateException ignored) {
                // Concurrent shut down of the coordinator
            }
        }
    }

    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            final TaskStateSnapshot checkpointState) {

        executionGraphHandler.acknowledgeCheckpoint(
                jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
    }

    @Override
    public void declineCheckpoint(final DeclineCheckpoint decline) {

        executionGraphHandler.declineCheckpoint(decline);
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID, ExecutionAttemptID attemptId, long id, CheckpointMetrics metrics) {
        executionGraphHandler.reportCheckpointMetrics(attemptId, id, metrics);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final boolean terminate,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, executionGraph.getJobID(), log);

        log.info("Triggering stop-with-savepoint for job {}.", jobGraph.getJobID());

        // we stop the checkpoint coordinator so that we are guaranteed
        // to have only the data of the synchronous savepoint committed.
        // in case of failure, and if the job restarts, the coordinator
        // will be restarted by the CheckpointCoordinatorDeActivator.
        stopCheckpointScheduler();

        final CompletableFuture<Collection<ExecutionState>> executionTerminationsFuture =
                getCombinedExecutionTerminationFuture();

        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSynchronousSavepoint(
                        terminate, targetDirectory, formatType);

        final StopWithSavepointTerminationManager stopWithSavepointTerminationManager =
                new StopWithSavepointTerminationManager(
                        new StopWithSavepointTerminationHandlerImpl(
                                jobGraph.getJobID(), this, log));

        return stopWithSavepointTerminationManager.stopWithSavepoint(
                savepointFuture, executionTerminationsFuture, mainThreadExecutor);
    }

    /**
     * Returns a {@code CompletableFuture} collecting the termination states of all {@link Execution
     * Executions} of the underlying {@link ExecutionGraph}.
     *
     * @return a {@code CompletableFuture} that completes after all underlying {@code Executions}
     *     have been terminated.
     */
    private CompletableFuture<Collection<ExecutionState>> getCombinedExecutionTerminationFuture() {
        return FutureUtils.combineAll(
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .map(Execution::getTerminalStateFuture)
                        .collect(Collectors.toList()));
    }

    // ------------------------------------------------------------------------
    //  Operator Coordinators
    //
    //  Note: It may be worthwhile to move the OperatorCoordinators out
    //        of the scheduler (have them owned by the JobMaster directly).
    //        Then we could avoid routing these events through the scheduler and
    //        doing this lazy initialization dance. However, this would require
    //        that the Scheduler does not eagerly construct the CheckpointCoordinator
    //        in the ExecutionGraph and does not eagerly restore the savepoint while
    //        doing that. Because during savepoint restore, the OperatorCoordinators
    //        (or at least their holders) already need to exist, to accept the restored
    //        state. But some components they depend on (Scheduler and MainThreadExecutor)
    //        are not fully usable and accessible at that point.
    // ------------------------------------------------------------------------

    @Override
    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        operatorCoordinatorHandler.deliverOperatorEventToCoordinator(
                taskExecutionId, operatorId, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        return operatorCoordinatorHandler.deliverCoordinationRequestToCoordinator(
                operator, request);
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {
        if (jobGraph.getJobType() == JobType.STREAMING
                && jobGraph.isCheckpointingEnabled()
                && jobGraph.getCheckpointingSettings()
                        .getCheckpointCoordinatorConfiguration()
                        .isEnableCheckpointsAfterTasksFinish()) {
            vertexEndOfDataListener.recordTaskEndOfData(executionAttemptID);
            if (vertexEndOfDataListener.areAllTasksOfJobVertexEndOfData(
                    executionAttemptID.getJobVertexId())) {
                List<OperatorIDPair> operatorIDPairs =
                        executionGraph
                                .getJobVertex(executionAttemptID.getJobVertexId())
                                .getOperatorIDs();
                CheckpointCoordinator checkpointCoordinator =
                        executionGraph.getCheckpointCoordinator();
                if (checkpointCoordinator != null) {
                    for (OperatorIDPair operatorIDPair : operatorIDPairs) {
                        checkpointCoordinator.setIsProcessingBacklog(
                                operatorIDPair.getGeneratedOperatorID(), false);
                    }
                }
            }
            if (vertexEndOfDataListener.areAllTasksEndOfData()) {
                triggerCheckpoint(CheckpointType.CONFIGURED);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  access utils for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    JobID getJobId() {
        return jobGraph.getJobID();
    }

    @VisibleForTesting
    VertexEndOfDataListener getVertexEndOfDataListener() {
        return vertexEndOfDataListener;
    }
}
