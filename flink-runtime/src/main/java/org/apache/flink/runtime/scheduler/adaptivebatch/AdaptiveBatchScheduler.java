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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResult;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertex;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.DefaultExecutionDeployer;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionOperations;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersion;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_FULL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This scheduler decides the parallelism of JobVertex according to the data volume it consumes. A
 * dynamically built up ExecutionGraph is used for this purpose.
 */
public class AdaptiveBatchScheduler extends DefaultScheduler {

    private final DefaultLogicalTopology logicalTopology;

    private final VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider;

    private final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId;

    private final Map<IntermediateDataSetID, BlockingResultInfo> blockingResultInfos;

    private final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint;

    private final Map<JobVertexID, CompletableFuture<Integer>>
            sourceParallelismFuturesByJobVertexId;

    private final SpeculativeExecutionHandler speculativeExecutionHandler;

    /**
     * A set of JobVertex Ids associated with JobVertices whose operatorCoordinators did not
     * successfully recover. And if any execution within these job vertices needs to be restarted in
     * the future, all other executions within the same vertex must also be restarted to ensure the
     * consistency and correctness of the state.
     */
    private Set<JobVertexID> jobVerticesWithUnRecoveredCoordinators = new HashSet<>();

    private final BatchJobRecoveryHandler jobRecoveryHandler;

    public AdaptiveBatchScheduler(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction,
            final ScheduledExecutor delayExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final Collection<FailureEnricher> failureEnrichers,
            final ExecutionGraphFactory executionGraphFactory,
            final ShuffleMaster<?> shuffleMaster,
            final Time rpcTimeout,
            final VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider,
            final int defaultMaxParallelism,
            final BlocklistOperations blocklistOperations,
            final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint,
            final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId,
            final BatchJobRecoveryHandler jobRecoveryHandler)
            throws Exception {

        super(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                startUpAction,
                delayExecutor,
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulingStrategyFactory,
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                failureEnrichers,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout,
                computeVertexParallelismStoreForDynamicGraph(
                        jobGraph.getVertices(), defaultMaxParallelism),
                new DefaultExecutionDeployer.Factory());

        this.logicalTopology = DefaultLogicalTopology.fromJobGraph(jobGraph);

        this.vertexParallelismAndInputInfosDecider =
                checkNotNull(vertexParallelismAndInputInfosDecider);

        this.forwardGroupsByJobVertexId = checkNotNull(forwardGroupsByJobVertexId);

        this.blockingResultInfos = new HashMap<>();

        this.hybridPartitionDataConsumeConstraint = hybridPartitionDataConsumeConstraint;

        this.sourceParallelismFuturesByJobVertexId = new HashMap<>();

        speculativeExecutionHandler =
                createSpeculativeExecutionHandler(
                        log, jobMasterConfiguration, executionVertexVersioner, blocklistOperations);

        this.jobRecoveryHandler = jobRecoveryHandler;
    }

    private SpeculativeExecutionHandler createSpeculativeExecutionHandler(
            Logger log,
            Configuration jobMasterConfiguration,
            ExecutionVertexVersioner executionVertexVersioner,
            BlocklistOperations blocklistOperations) {

        if (jobMasterConfiguration.get(BatchExecutionOptions.SPECULATIVE_ENABLED)) {
            return new DefaultSpeculativeExecutionHandler(
                    jobMasterConfiguration,
                    blocklistOperations,
                    this::getExecutionVertex,
                    () -> getExecutionGraph().getRegisteredExecutions(),
                    (newSpeculativeExecutions, verticesToDeploy) ->
                            executionDeployer.allocateSlotsAndDeploy(
                                    newSpeculativeExecutions,
                                    executionVertexVersioner.getExecutionVertexVersions(
                                            verticesToDeploy)),
                    log);
        } else {
            return new DummySpeculativeExecutionHandler();
        }
    }

    @Override
    protected void startSchedulingInternal() {
        speculativeExecutionHandler.init(
                getExecutionGraph(), getMainThreadExecutor(), jobManagerJobMetricGroup);
        jobRecoveryHandler.initialize(new DefaultBatchJobRecoveryContext());

        if (jobRecoveryHandler.needRecover()) {
            jobRecoveryHandler.startRecovering();
        } else {
            tryComputeSourceParallelismThenRunAsync(
                    (Void value, Throwable throwable) -> {
                        if (getExecutionGraph().getState() == JobStatus.CREATED) {
                            initializeVerticesIfPossible();
                            super.startSchedulingInternal();
                        }
                    });
        }
    }

    /**
     * Modifies the vertices which need to be restarted. If any task needing restarting belongs to
     * job vertices with unrecovered operator coordinators, all tasks within those job vertices need
     * to be restarted once.
     */
    @Override
    protected void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        FailureHandlingResult wrappedResult = failureHandlingResult;
        if (failureHandlingResult.canRestart()) {
            Set<ExecutionVertexID> originalNeedToRestartVertices =
                    failureHandlingResult.getVerticesToRestart();

            Set<JobVertexID> extraNeedToRestartJobVertices =
                    originalNeedToRestartVertices.stream()
                            .map(ExecutionVertexID::getJobVertexId)
                            .filter(jobVerticesWithUnRecoveredCoordinators::contains)
                            .collect(Collectors.toSet());

            jobVerticesWithUnRecoveredCoordinators.removeAll(extraNeedToRestartJobVertices);

            Set<ExecutionVertexID> needToRestartVertices =
                    extraNeedToRestartJobVertices.stream()
                            .flatMap(
                                    jobVertexId -> {
                                        ExecutionJobVertex jobVertex =
                                                getExecutionJobVertex(jobVertexId);
                                        return Arrays.stream(jobVertex.getTaskVertices())
                                                .map(ExecutionVertex::getID);
                                    })
                            .collect(Collectors.toSet());
            needToRestartVertices.addAll(originalNeedToRestartVertices);

            wrappedResult =
                    FailureHandlingResult.restartable(
                            failureHandlingResult.getFailedExecution().orElse(null),
                            failureHandlingResult.getError(),
                            failureHandlingResult.getTimestamp(),
                            failureHandlingResult.getFailureLabels(),
                            needToRestartVertices,
                            failureHandlingResult.getRestartDelayMS(),
                            failureHandlingResult.isGlobalFailure(),
                            failureHandlingResult.isRootCause());
        }

        super.maybeRestartTasks(wrappedResult);
    }

    @VisibleForTesting
    boolean isRecovering() {
        return jobRecoveryHandler.isRecovering();
    }

    @Override
    protected void resetForNewExecutions(Collection<ExecutionVertexID> vertices) {
        super.resetForNewExecutions(vertices);
        if (!isRecovering()) {
            jobRecoveryHandler.onExecutionVertexReset(vertices);
        }
    }

    private void initializeJobVertex(
            ExecutionJobVertex jobVertex,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos,
            long createTimestamp)
            throws JobException {
        if (!jobVertex.isParallelismDecided()) {
            changeJobVertexParallelism(jobVertex, parallelism);
        } else {
            checkState(parallelism == jobVertex.getParallelism());
        }
        checkState(canInitialize(jobVertex));
        getExecutionGraph().initializeJobVertex(jobVertex, createTimestamp, jobVertexInputInfos);
        if (!isRecovering()) {
            jobRecoveryHandler.onExecutionJobVertexInitialization(
                    jobVertex.getJobVertex().getID(), parallelism, jobVertexInputInfos);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        // stop job event manager.
        jobRecoveryHandler.stop(requestJobStatus().isGloballyTerminalState());
        speculativeExecutionHandler.stopSlowTaskDetector();
        return super.closeAsync();
    }

    @Override
    protected void onTaskFinished(final Execution execution, final IOMetrics ioMetrics) {
        speculativeExecutionHandler.notifyTaskFinished(execution, this::cancelPendingExecutions);
        if (!isRecovering()) {
            jobRecoveryHandler.onExecutionFinished(execution.getVertex().getID());
        }

        checkNotNull(ioMetrics);
        updateResultPartitionBytesMetrics(ioMetrics.getResultPartitionBytes());
        ExecutionVertexVersion currentVersion =
                executionVertexVersioner.getExecutionVertexVersion(execution.getVertex().getID());
        tryComputeSourceParallelismThenRunAsync(
                (Void value, Throwable throwable) -> {
                    if (executionVertexVersioner.isModified(currentVersion)) {
                        log.debug(
                                "Initialization of vertices will be skipped, because the execution"
                                        + " vertex version has been modified.");
                        return;
                    }
                    initializeVerticesIfPossible();
                    super.onTaskFinished(execution, ioMetrics);
                });
    }

    private CompletableFuture<?> cancelPendingExecutions(
            final ExecutionVertexID executionVertexId) {
        final List<Execution> pendingExecutions =
                getExecutionVertex(executionVertexId).getCurrentExecutions().stream()
                        .filter(
                                e ->
                                        !e.getState().isTerminal()
                                                && e.getState() != ExecutionState.CANCELING)
                        .collect(Collectors.toList());
        if (pendingExecutions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info(
                "Canceling {} un-finished executions of {} because one of its executions has finished.",
                pendingExecutions.size(),
                executionVertexId);

        final CompletableFuture<?> future =
                FutureUtils.combineAll(
                        pendingExecutions.stream()
                                .map(this::cancelExecution)
                                .collect(Collectors.toList()));
        cancelAllPendingSlotRequestsForVertex(executionVertexId);
        return future;
    }

    @Override
    protected void onTaskFailed(final Execution execution) {
        speculativeExecutionHandler.notifyTaskFailed(execution);

        super.onTaskFailed(execution);
    }

    @Override
    protected void handleTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        if (!speculativeExecutionHandler.handleTaskFailure(
                failedExecution, error, this::handleLocalExecutionAttemptFailure)) {
            super.handleTaskFailure(failedExecution, error);
        }
    }

    private void handleLocalExecutionAttemptFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        executionSlotAllocator.cancel(failedExecution.getAttemptId());

        final FailureHandlingResult failureHandlingResult =
                recordTaskFailure(failedExecution, error);
        if (failureHandlingResult.canRestart()) {
            archiveFromFailureHandlingResult(
                    createFailureHandlingResultSnapshot(failureHandlingResult));
        } else {
            failJob(
                    error,
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels());
        }
    }

    private void updateResultPartitionBytesMetrics(
            Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
        checkNotNull(resultPartitionBytes);
        resultPartitionBytes.forEach(
                (partitionId, partitionBytes) -> {
                    IntermediateResult result =
                            getExecutionGraph()
                                    .getAllIntermediateResults()
                                    .get(partitionId.getIntermediateDataSetID());
                    checkNotNull(result);

                    blockingResultInfos.compute(
                            result.getId(),
                            (ignored, resultInfo) -> {
                                if (resultInfo == null) {
                                    resultInfo = createFromIntermediateResult(result);
                                }
                                resultInfo.recordPartitionInfo(
                                        partitionId.getPartitionNumber(), partitionBytes);
                                return resultInfo;
                            });
                });
    }

    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {
        List<ExecutionVertex> executionVertices =
                verticesToDeploy.stream()
                        .map(this::getExecutionVertex)
                        .collect(Collectors.toList());
        enrichInputBytesForExecutionVertices(executionVertices);
        super.allocateSlotsAndDeploy(verticesToDeploy);
    }

    @Override
    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        speculativeExecutionHandler.resetForNewExecution(executionVertexId);

        final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
        if (executionVertex.getExecutionState() == ExecutionState.FINISHED) {
            executionVertex
                    .getProducedPartitions()
                    .values()
                    .forEach(
                            partition -> {
                                blockingResultInfos.computeIfPresent(
                                        partition.getIntermediateResult().getId(),
                                        (ignored, resultInfo) -> {
                                            resultInfo.resetPartitionInfo(
                                                    partition.getPartitionNumber());
                                            return resultInfo;
                                        });
                            });
        }

        super.resetForNewExecution(executionVertexId);
    }

    @Override
    protected MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        return (rp) ->
                // for blocking result partition case, it always needs mark
                // partition finished. for hybrid only consume finished partition case, as
                // downstream needs the producer's finished status to start consuming the produced
                // data, the notification of partition finished is required.
                rp.isBlockingOrBlockingPersistentResultPartition()
                        || hybridPartitionDataConsumeConstraint.isOnlyConsumeFinishedPartition();
    }

    private void tryComputeSourceParallelismThenRunAsync(BiConsumer<Void, Throwable> action) {
        // Ensure `initializeVerticesIfPossible` is invoked asynchronously post
        // `computeDynamicSourceParallelism`. Any method required to run after
        // `initializeVerticesIfPossible` should be enqueued within the same asynchronous action to
        // maintain the correct execution order.
        FutureUtils.ConjunctFuture<Void> dynamicSourceParallelismFutures =
                FutureUtils.waitForAll(computeDynamicSourceParallelism());
        dynamicSourceParallelismFutures
                .whenCompleteAsync(action, getMainThreadExecutor())
                .exceptionally(
                        throwable -> {
                            log.error("An unexpected error occurred while scheduling.", throwable);
                            this.handleGlobalFailure(new SuppressRestartsException(throwable));
                            return null;
                        });
    }

    public List<CompletableFuture<Integer>> computeDynamicSourceParallelism() {
        final List<CompletableFuture<Integer>> dynamicSourceParallelismFutures = new ArrayList<>();
        for (ExecutionJobVertex jobVertex : getExecutionGraph().getVerticesTopologically()) {
            List<SourceCoordinator<?, ?>> sourceCoordinators = jobVertex.getSourceCoordinators();
            if (sourceCoordinators.isEmpty() || jobVertex.isParallelismDecided()) {
                continue;
            }
            if (sourceParallelismFuturesByJobVertexId.containsKey(jobVertex.getJobVertexId())) {
                dynamicSourceParallelismFutures.add(
                        sourceParallelismFuturesByJobVertexId.get(jobVertex.getJobVertexId()));
                continue;
            }

            // We need to wait for the upstream vertex to complete, otherwise, dynamic filtering
            // information will be inaccessible during source parallelism inference.
            Optional<List<BlockingResultInfo>> consumedResultsInfo =
                    tryGetConsumedResultsInfo(jobVertex);
            if (consumedResultsInfo.isPresent()) {
                List<CompletableFuture<Integer>> sourceParallelismFutures =
                        sourceCoordinators.stream()
                                .map(
                                        sourceCoordinator ->
                                                sourceCoordinator.inferSourceParallelismAsync(
                                                        vertexParallelismAndInputInfosDecider
                                                                .computeSourceParallelismUpperBound(
                                                                        jobVertex.getJobVertexId(),
                                                                        jobVertex
                                                                                .getMaxParallelism()),
                                                        vertexParallelismAndInputInfosDecider
                                                                .getDataVolumePerTask()))
                                .collect(Collectors.toList());
                CompletableFuture<Integer> dynamicSourceParallelismFuture =
                        mergeDynamicParallelismFutures(sourceParallelismFutures);
                sourceParallelismFuturesByJobVertexId.put(
                        jobVertex.getJobVertexId(), dynamicSourceParallelismFuture);
                dynamicSourceParallelismFutures.add(dynamicSourceParallelismFuture);
            }
        }

        return dynamicSourceParallelismFutures;
    }

    @VisibleForTesting
    static CompletableFuture<Integer> mergeDynamicParallelismFutures(
            List<CompletableFuture<Integer>> sourceParallelismFutures) {
        return sourceParallelismFutures.stream()
                .reduce(
                        CompletableFuture.completedFuture(ExecutionConfig.PARALLELISM_DEFAULT),
                        (a, b) -> a.thenCombine(b, Math::max));
    }

    @VisibleForTesting
    public void initializeVerticesIfPossible() {
        final List<ExecutionJobVertex> newlyInitializedJobVertices = new ArrayList<>();
        try {
            final long createTimestamp = System.currentTimeMillis();
            for (ExecutionJobVertex jobVertex : getExecutionGraph().getVerticesTopologically()) {
                if (jobVertex.isInitialized()) {
                    continue;
                }

                if (canInitialize(jobVertex)) {
                    // This branch is for: If the parallelism is user-specified(decided), the
                    // downstream job vertices can be initialized earlier, so that it can be
                    // scheduled together with its upstream in hybrid shuffle mode.

                    // Note that in current implementation, the decider will not load balance
                    // (evenly distribute data) for job vertices whose parallelism has already been
                    // decided, so we can call the initializeJobVertex method, specifying the
                    // user-defined parallelism as its argument.
                    // TODO: In the future, if we want to load balance for job vertices whose
                    // parallelism has already been decided, we need to refactor the logic here.
                    initializeJobVertex(
                            jobVertex,
                            jobVertex.getParallelism(),
                            VertexInputInfoComputationUtils.computeVertexInputInfos(
                                    jobVertex,
                                    getExecutionGraph().getAllIntermediateResults()::get),
                            createTimestamp);
                    newlyInitializedJobVertices.add(jobVertex);
                } else {
                    Optional<List<BlockingResultInfo>> consumedResultsInfo =
                            tryGetConsumedResultsInfo(jobVertex);
                    if (consumedResultsInfo.isPresent()) {
                        ParallelismAndInputInfos parallelismAndInputInfos =
                                tryDecideParallelismAndInputInfos(
                                        jobVertex, consumedResultsInfo.get());
                        initializeJobVertex(
                                jobVertex,
                                parallelismAndInputInfos.getParallelism(),
                                parallelismAndInputInfos.getJobVertexInputInfos(),
                                createTimestamp);
                        newlyInitializedJobVertices.add(jobVertex);
                    }
                }
            }
        } catch (JobException ex) {
            log.error("Unexpected error occurred when initializing ExecutionJobVertex", ex);
            this.handleGlobalFailure(new SuppressRestartsException(ex));
        }

        if (newlyInitializedJobVertices.size() > 0) {
            updateTopology(newlyInitializedJobVertices);
        }
    }

    private ParallelismAndInputInfos tryDecideParallelismAndInputInfos(
            final ExecutionJobVertex jobVertex, List<BlockingResultInfo> inputs) {
        int vertexInitialParallelism = jobVertex.getParallelism();
        ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getJobVertexId());
        if (!jobVertex.isParallelismDecided() && forwardGroup != null) {
            checkState(!forwardGroup.isParallelismDecided());
        }

        int vertexMinParallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        if (sourceParallelismFuturesByJobVertexId.containsKey(jobVertex.getJobVertexId())) {
            int dynamicSourceParallelism = getDynamicSourceParallelism(jobVertex);
            // If the JobVertex only acts as a source vertex, dynamicSourceParallelism will serve as
            // the vertex's initial parallelism and will remain unchanged. If the JobVertex is also
            // a source with upstream inputs, dynamicSourceParallelism will serve as the vertex's
            // minimum parallelism, with the final parallelism being the maximum of
            // dynamicSourceParallelism and the vertex's dynamic parallelism according to upstream
            // inputs.
            if (!inputs.isEmpty()) {
                vertexMinParallelism = dynamicSourceParallelism;
            } else {
                vertexInitialParallelism = dynamicSourceParallelism;
            }
        }

        final ParallelismAndInputInfos parallelismAndInputInfos =
                vertexParallelismAndInputInfosDecider.decideParallelismAndInputInfosForVertex(
                        jobVertex.getJobVertexId(),
                        inputs,
                        vertexInitialParallelism,
                        vertexMinParallelism,
                        jobVertex.getMaxParallelism());

        if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {}.",
                    jobVertex.getName(),
                    jobVertex.getJobVertexId(),
                    parallelismAndInputInfos.getParallelism());
        } else {
            checkState(parallelismAndInputInfos.getParallelism() == vertexInitialParallelism);
        }

        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelismAndInputInfos.getParallelism());

            // When the parallelism for a forward group is determined, we ensure that the
            // parallelism for all job vertices within that group is also set.
            // This approach ensures that each forward edge produces single subpartition.
            //
            // This setting is crucial because the Sink V2 committer relies on the interplay
            // between the CommittableSummary and the CommittableWithLineage, which are sent by
            // the upstream Sink V2 Writer. The committer expects to receive CommittableSummary
            // before CommittableWithLineage.
            //
            // If the number of subpartitions produced by a forward edge is greater than one,
            // the ordering of these elements received by the committer cannot be assured, which
            // would break the assumption that CommittableSummary is received before
            // CommittableWithLineage.
            for (JobVertexID jobVertexId : forwardGroup.getJobVertexIds()) {
                ExecutionJobVertex executionJobVertex = getExecutionJobVertex(jobVertexId);
                if (!executionJobVertex.isParallelismDecided()) {
                    log.info(
                            "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                            executionJobVertex.getName(),
                            executionJobVertex.getJobVertexId(),
                            parallelismAndInputInfos.getParallelism());
                    changeJobVertexParallelism(
                            executionJobVertex, parallelismAndInputInfos.getParallelism());
                } else {
                    checkState(
                            parallelismAndInputInfos.getParallelism()
                                    == executionJobVertex.getParallelism());
                }
            }
        }

        return parallelismAndInputInfos;
    }

    private int getDynamicSourceParallelism(ExecutionJobVertex jobVertex) {
        CompletableFuture<Integer> dynamicSourceParallelismFuture =
                sourceParallelismFuturesByJobVertexId.get(jobVertex.getJobVertexId());
        int dynamicSourceParallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        if (dynamicSourceParallelismFuture != null) {
            dynamicSourceParallelism = dynamicSourceParallelismFuture.join();
            int vertexMaxParallelism = jobVertex.getMaxParallelism();
            if (dynamicSourceParallelism > vertexMaxParallelism) {
                log.info(
                        "The dynamic inferred source parallelism {} is larger than the maximum parallelism {}. "
                                + "Use {} as the upper bound parallelism of source job vertex {}.",
                        dynamicSourceParallelism,
                        vertexMaxParallelism,
                        vertexMaxParallelism,
                        jobVertex.getJobVertexId());
                dynamicSourceParallelism = vertexMaxParallelism;
            } else if (dynamicSourceParallelism > 0) {
                log.info(
                        "Parallelism of JobVertex: {} ({}) is decided to be {} according to dynamic source parallelism inference.",
                        jobVertex.getName(),
                        jobVertex.getJobVertexId(),
                        dynamicSourceParallelism);
            } else {
                dynamicSourceParallelism = ExecutionConfig.PARALLELISM_DEFAULT;
            }
        }

        return dynamicSourceParallelism;
    }

    private void enrichInputBytesForExecutionVertices(List<ExecutionVertex> executionVertices) {
        for (ExecutionVertex ev : executionVertices) {
            List<IntermediateResult> intermediateResults = ev.getJobVertex().getInputs();
            boolean hasHybridEdge =
                    intermediateResults.stream()
                            .anyMatch(
                                    ir ->
                                            ir.getResultType() == HYBRID_FULL
                                                    || ir.getResultType() == HYBRID_SELECTIVE);
            if (intermediateResults.isEmpty() || hasHybridEdge) {
                continue;
            }
            long inputBytes = 0;
            for (IntermediateResult intermediateResult : intermediateResults) {
                ExecutionVertexInputInfo inputInfo =
                        ev.getExecutionVertexInputInfo(intermediateResult.getId());
                IndexRange partitionIndexRange = inputInfo.getPartitionIndexRange();
                IndexRange subpartitionIndexRange = inputInfo.getSubpartitionIndexRange();
                BlockingResultInfo blockingResultInfo =
                        checkNotNull(getBlockingResultInfo(intermediateResult.getId()));
                inputBytes +=
                        blockingResultInfo.getNumBytesProduced(
                                partitionIndexRange, subpartitionIndexRange);
            }
            ev.setInputBytes(inputBytes);
        }
    }

    private void changeJobVertexParallelism(ExecutionJobVertex jobVertex, int parallelism) {
        if (jobVertex.isParallelismDecided()) {
            return;
        }
        // update the JSON Plan, it's needed to enable REST APIs to return the latest parallelism of
        // job vertices
        jobVertex.getJobVertex().setDynamicParallelism(parallelism);
        try {
            getExecutionGraph().setJsonPlan(JsonPlanGenerator.generatePlan(getJobGraph()));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            getExecutionGraph().setJsonPlan("{}");
        }

        jobVertex.setParallelism(parallelism);
    }

    /** Get information of consumable results. */
    private Optional<List<BlockingResultInfo>> tryGetConsumedResultsInfo(
            final ExecutionJobVertex jobVertex) {

        List<BlockingResultInfo> consumableResultInfo = new ArrayList<>();

        DefaultLogicalVertex logicalVertex = logicalTopology.getVertex(jobVertex.getJobVertexId());
        Iterable<DefaultLogicalResult> consumedResults = logicalVertex.getConsumedResults();

        for (DefaultLogicalResult consumedResult : consumedResults) {
            final ExecutionJobVertex producerVertex =
                    getExecutionJobVertex(consumedResult.getProducer().getId());
            if (producerVertex.isFinished()) {
                BlockingResultInfo resultInfo =
                        checkNotNull(blockingResultInfos.get(consumedResult.getId()));
                consumableResultInfo.add(resultInfo);
            } else {
                // not all inputs consumable, return Optional.empty()
                return Optional.empty();
            }
        }

        return Optional.of(consumableResultInfo);
    }

    private boolean canInitialize(final ExecutionJobVertex jobVertex) {
        if (jobVertex.isInitialized() || !jobVertex.isParallelismDecided()) {
            return false;
        }

        // all the upstream job vertices need to have been initialized
        for (JobEdge inputEdge : jobVertex.getJobVertex().getInputs()) {
            final ExecutionJobVertex producerVertex =
                    getExecutionGraph().getJobVertex(inputEdge.getSource().getProducer().getID());
            checkNotNull(producerVertex);
            if (!producerVertex.isInitialized()) {
                return false;
            }
        }

        return true;
    }

    private void updateTopology(final List<ExecutionJobVertex> newlyInitializedJobVertices) {
        for (ExecutionJobVertex vertex : newlyInitializedJobVertices) {
            initializeOperatorCoordinatorsFor(vertex);
        }

        // notify execution graph updated, and try to update the execution topology.
        getExecutionGraph().notifyNewlyInitializedJobVertices(newlyInitializedJobVertices);
    }

    private void initializeOperatorCoordinatorsFor(ExecutionJobVertex vertex) {
        operatorCoordinatorHandler.registerAndStartNewCoordinators(
                vertex.getOperatorCoordinators(), getMainThreadExecutor(), vertex.getParallelism());
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices in a dynamic graph, which
     * will set defaults and ensure that the returned store contains valid parallelisms, with the
     * configured default max parallelism.
     *
     * @param vertices the vertices to compute parallelism for
     * @param defaultMaxParallelism the global default max parallelism
     * @return the computed parallelism store
     */
    @VisibleForTesting
    public static VertexParallelismStore computeVertexParallelismStoreForDynamicGraph(
            Iterable<JobVertex> vertices, int defaultMaxParallelism) {
        // Resets the JobVertices to their original parallelism after JM failover, maintaining
        // consistency between the job graph loaded from the file and the one in memory.
        resetDynamicParallelism(vertices);

        // for dynamic graph, there is no need to normalize vertex parallelism. if the max
        // parallelism is not configured and the parallelism is a positive value, max
        // parallelism can be computed against the parallelism, otherwise it needs to use the
        // global default max parallelism.
        return computeVertexParallelismStore(
                vertices,
                v -> {
                    if (v.getParallelism() > 0) {
                        return getDefaultMaxParallelism(v);
                    } else {
                        return defaultMaxParallelism;
                    }
                },
                Function.identity());
    }

    private static void resetDynamicParallelism(Iterable<JobVertex> vertices) {
        for (JobVertex vertex : vertices) {
            if (vertex.isDynamicParallelism()) {
                vertex.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
            }
        }
    }

    private static BlockingResultInfo createFromIntermediateResult(IntermediateResult result) {
        checkArgument(result != null);
        // Note that for dynamic graph, different partitions in the same result have the same number
        // of subpartitions.
        if (result.getConsumingDistributionPattern() == DistributionPattern.POINTWISE) {
            return new PointwiseBlockingResultInfo(
                    result.getId(),
                    result.getNumberOfAssignedPartitions(),
                    result.getPartitions()[0].getNumberOfSubpartitions());
        } else {
            return new AllToAllBlockingResultInfo(
                    result.getId(),
                    result.getNumberOfAssignedPartitions(),
                    result.getPartitions()[0].getNumberOfSubpartitions(),
                    result.isBroadcast());
        }
    }

    @VisibleForTesting
    BlockingResultInfo getBlockingResultInfo(IntermediateDataSetID resultId) {
        return blockingResultInfos.get(resultId);
    }

    @VisibleForTesting
    SpeculativeExecutionHandler getSpeculativeExecutionHandler() {
        return speculativeExecutionHandler;
    }

    private class DefaultBatchJobRecoveryContext implements BatchJobRecoveryContext {

        private final FailoverStrategy restartStrategyOnResultConsumable =
                new RestartPipelinedRegionFailoverStrategy.Factory()
                        .create(getSchedulingTopology(), getResultPartitionAvailabilityChecker());

        private final FailoverStrategy restartStrategyNotOnResultConsumable =
                new RestartPipelinedRegionFailoverStrategy.Factory()
                        .create(getSchedulingTopology(), ignored -> true);

        @Override
        public ExecutionGraph getExecutionGraph() {
            return AdaptiveBatchScheduler.this.getExecutionGraph();
        }

        @Override
        public ShuffleMaster<?> getShuffleMaster() {
            return shuffleMaster;
        }

        @Override
        public Set<ExecutionVertexID> getTasksNeedingRestart(
                ExecutionVertexID vertexId, boolean considerResultConsumable) {
            if (considerResultConsumable) {
                return restartStrategyOnResultConsumable.getTasksNeedingRestart(vertexId, null);
            } else {
                return restartStrategyNotOnResultConsumable.getTasksNeedingRestart(vertexId, null);
            }
        }

        @Override
        public ComponentMainThreadExecutor getMainThreadExecutor() {
            return AdaptiveBatchScheduler.this.getMainThreadExecutor();
        }

        @Override
        public void resetVerticesInRecovering(Set<ExecutionVertexID> verticesToReset)
                throws Exception {
            for (ExecutionVertexID executionVertexID : verticesToReset) {
                notifyCoordinatorsAboutTaskFailure(
                        getExecutionVertex(executionVertexID).getCurrentExecutionAttempt(), null);
            }
            resetForNewExecutions(verticesToReset);
            restoreState(verticesToReset, false);
        }

        @Override
        public void updateResultPartitionBytesMetrics(
                Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
            AdaptiveBatchScheduler.this.updateResultPartitionBytesMetrics(resultPartitionBytes);
        }

        @Override
        public void initializeJobVertex(
                ExecutionJobVertex jobVertex,
                int parallelism,
                Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos,
                long createTimestamp)
                throws JobException {
            AdaptiveBatchScheduler.this.initializeJobVertex(
                    jobVertex, parallelism, jobVertexInputInfos, createTimestamp);
        }

        @Override
        public void updateTopology(final List<ExecutionJobVertex> newlyInitializedJobVertices) {
            AdaptiveBatchScheduler.this.updateTopology(newlyInitializedJobVertices);
        }

        @Override
        public void onRecoveringFinished(Set<JobVertexID> jobVerticesWithUnRecoveredCoordinators) {
            AdaptiveBatchScheduler.this.jobVerticesWithUnRecoveredCoordinators =
                    new HashSet<>(jobVerticesWithUnRecoveredCoordinators);
            tryComputeSourceParallelismThenRunAsync(
                    (Void value, Throwable throwable) ->
                            schedulingStrategy.scheduleAllVerticesIfPossible());
        }

        @Override
        public void onRecoveringFailed() {
            // call #initializeVerticesIfPossible to avoid an empty execution graph
            initializeVerticesIfPossible();
            handleGlobalFailure(
                    new FlinkRuntimeException("Recover failed from JM failover, fail global."));
        }

        @Override
        public void failJob(
                Throwable cause,
                long timestamp,
                CompletableFuture<Map<String, String>> failureLabels) {
            AdaptiveBatchScheduler.this.failJob(cause, timestamp, failureLabels);
        }
    }
}
