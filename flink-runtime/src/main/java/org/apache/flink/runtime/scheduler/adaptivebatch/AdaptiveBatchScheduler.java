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
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResult;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertex;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexInitializedEvent;
import org.apache.flink.runtime.jobmaster.event.ExecutionVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.ExecutionVertexResetEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.runtime.jobmaster.event.JobEventManager;
import org.apache.flink.runtime.jobmaster.event.JobEventReplayHandler;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.scheduler.DefaultExecutionDeployer;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionOperations;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersion;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.DefaultShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_FULL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.HYBRID_SELECTIVE;
import static org.apache.flink.runtime.operators.coordination.OperatorCoordinator.NO_CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This scheduler decides the parallelism of JobVertex according to the data volume it consumes. A
 * dynamically built up ExecutionGraph is used for this purpose.
 */
public class AdaptiveBatchScheduler extends DefaultScheduler implements JobEventReplayHandler {

    private final DefaultLogicalTopology logicalTopology;

    private final VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider;

    private final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId;

    private final Map<IntermediateDataSetID, BlockingResultInfo> blockingResultInfos;

    private final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint;

    private final Map<JobVertexID, CompletableFuture<Integer>>
            sourceParallelismFuturesByJobVertexId;

    // ============================================================================
    //  JobMaster Recover fields
    // ============================================================================

    private final JobEventManager jobEventManager;

    private final List<List<ExecutionVertexVersion>> executionsToDeploy = new ArrayList<>();

    private final Map<ExecutionVertexID, ExecutionVertexFinishedEvent>
            executionVertexFinishedEventMap = new LinkedHashMap<>();

    private final List<ExecutionJobVertexInitializedEvent> jobVertexInitializedEvents =
            new ArrayList<>();

    private static final ResourceID UNKNOWN_PRODUCER = ResourceID.generate();

    private final Duration previousWorkerRecoveryTimeout;

    private final long snapshotMinPauseMills;

    private final Clock clock;

    private final boolean isJobRecoveryEnabled;

    /** The timestamp (via {@link Clock#relativeTimeMillis()}) of the last snapshot. */
    private long lastSnapshotRelativeTime;

    private final Set<JobVertexID> needToSnapshotJobVertices = new HashSet<>();

    private final Set<JobVertexID> finishedAndUnsupportedBatchSnapshotJobVertices = new HashSet<>();

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
            int defaultMaxParallelism,
            final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint,
            final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId,
            @Nullable final JobEventManager jobEventManager)
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

        this.isJobRecoveryEnabled =
                jobMasterConfiguration.get(BatchExecutionOptions.JOB_RECOVERY_ENABLED)
                        && shuffleMaster.supportsBatchSnapshot();

        this.jobEventManager = jobEventManager;

        this.previousWorkerRecoveryTimeout =
                jobMasterConfiguration.get(
                        BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT);

        this.snapshotMinPauseMills =
                jobMasterConfiguration
                        .get(BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE)
                        .toMillis();

        this.clock = SystemClock.getInstance();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        // stop job event manager.
        if (isJobRecoveryEnabled) {
            jobEventManager.stop(requestJobStatus().isGloballyTerminalState());
        }
        return super.closeAsync();
    }

    @Override
    protected void startSchedulingInternal() {
        final boolean needRecover;
        if (isJobRecoveryEnabled) {
            try {
                needRecover = !jobEventManager.isEmpty();
                jobEventManager.start();
            } catch (Throwable throwable) {
                this.handleGlobalFailure(new SuppressRestartsException(throwable));
                return;
            }
        } else {
            needRecover = false;
        }

        tryComputeSourceParallelismThenRunAsync(
                (Void value, Throwable throwable) -> {
                    if (getExecutionGraph().getState() == JobStatus.CREATED) {
                        if (needRecover) {
                            getMainThreadExecutor()
                                    .schedule(
                                            this::recoverAndStartScheduling,
                                            previousWorkerRecoveryTimeout.toMillis(),
                                            TimeUnit.MILLISECONDS);
                        } else {
                            initializeVerticesIfPossible();
                            super.startSchedulingInternal();
                        }
                    }
                });
    }

    private void recoverAndStartScheduling() {
        getMainThreadExecutor().assertRunningInMainThread();
        checkState(isJobRecoveryEnabled);

        enterRecovering();

        // replay job events
        if (!jobEventManager.replay(this)) {
            log.warn(
                    "Fail to replay log for {}, will start the job as a new one.",
                    getExecutionGraph().getJobID());
            recoverFailed();
            return;
        }
        log.info("Replay all job events successfully.");

        CompletableFuture<Collection<PartitionWithMetrics>> existingPartitions =
                ((InternalExecutionGraphAccessor) getExecutionGraph())
                        .getShuffleMaster()
                        .getAllPartitionWithMetrics(getJobId());

        existingPartitions.whenCompleteAsync(
                (partitions, throwable) -> {
                    if (throwable != null) {
                        log.info("Recover fail", throwable);
                        recoverFailed();
                    }
                    try {
                        processAfterGetExistingPartitions(partitions);
                    } catch (Exception exception) {
                        log.info("Recover fail", exception);
                        recoverFailed();
                    }
                },
                getMainThreadExecutor());
    }

    private ResultPartitionID createResultPartitionId(IntermediateResultPartitionID partitionId) {
        final Execution producer = getProducer(partitionId).getPartitionProducer();
        return new ResultPartitionID(partitionId, producer.getAttemptId());
    }

    private ExecutionVertex getProducer(IntermediateResultPartitionID partitionId) {
        return getExecutionGraph().getResultPartitionOrThrow(partitionId).getProducer();
    }

    private List<ExecutionVertex> getConsumers(IntermediateResultPartitionID partitionId) {
        List<ConsumerVertexGroup> consumerVertexGroups =
                getExecutionGraph()
                        .getResultPartitionOrThrow(partitionId)
                        .getConsumerVertexGroups();
        List<ExecutionVertex> executionVertices = new ArrayList<>();
        for (ConsumerVertexGroup group : consumerVertexGroups) {
            for (ExecutionVertexID executionVertexID : group) {
                executionVertices.add(getExecutionVertex(executionVertexID));
            }
        }
        return executionVertices;
    }

    private void processAfterGetExistingPartitions(
            Collection<PartitionWithMetrics> partitionWithMetrics) throws Exception {
        getMainThreadExecutor().assertRunningInMainThread();
        checkState(isJobRecoveryEnabled);

        Set<ResultPartitionID> actualPartitions =
                partitionWithMetrics.stream()
                        .map(PartitionWithMetrics::getPartition)
                        .map(ShuffleDescriptor::getResultPartitionID)
                        .collect(Collectors.toSet());

        ReconcileResult reconcileResult = reconcilePartitions(actualPartitions);

        log.info(
                "All unknown partitions {}, missing partitions {}, available partitions {}",
                reconcileResult.unknownPartitions,
                reconcileResult.missingPartitions,
                reconcileResult.availablePartitions);

        // release all unknown partitions
        ((InternalExecutionGraphAccessor) getExecutionGraph())
                .getPartitionTracker()
                .stopTrackingAndReleasePartitions(reconcileResult.unknownPartitions);

        // start tracking all available partitions
        Map<IntermediateResultPartitionID, ResultPartitionBytes> availablePartitionBytes =
                new HashMap<>();
        partitionWithMetrics.stream()
                .filter(
                        partitionAndMetric ->
                                reconcileResult.availablePartitions.contains(
                                        partitionAndMetric.getPartition().getResultPartitionID()))
                .forEach(
                        partitionAndMetric -> {
                            ShuffleDescriptor shuffleDescriptor = partitionAndMetric.getPartition();

                            // we cannot get the producer id when using remote shuffle
                            ResourceID producerTaskExecutorId = UNKNOWN_PRODUCER;
                            if (shuffleDescriptor.storesLocalResourcesOn().isPresent()) {
                                producerTaskExecutorId =
                                        shuffleDescriptor.storesLocalResourcesOn().get();
                            }
                            IntermediateResultPartition partition =
                                    getExecutionGraph()
                                            .getResultPartitionOrThrow(
                                                    shuffleDescriptor
                                                            .getResultPartitionID()
                                                            .getPartitionId());
                            ((InternalExecutionGraphAccessor) getExecutionGraph())
                                    .getPartitionTracker()
                                    .startTrackingPartition(
                                            producerTaskExecutorId,
                                            Execution.createResultPartitionDeploymentDescriptor(
                                                    partition, shuffleDescriptor));

                            availablePartitionBytes.put(
                                    shuffleDescriptor.getResultPartitionID().getPartitionId(),
                                    partitionAndMetric.getPartitionMetrics().getPartitionBytes());
                        });

        // recover the produced partitions for executions
        Map<
                        ExecutionVertexID,
                        Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>>
                allDescriptors = new HashMap<>();
        ((InternalExecutionGraphAccessor) getExecutionGraph())
                .getPartitionTracker()
                .getAllTrackedNonClusterPartitions()
                .forEach(
                        descriptor -> {
                            ExecutionVertexID vertexId =
                                    descriptor
                                            .getShuffleDescriptor()
                                            .getResultPartitionID()
                                            .getProducerId()
                                            .getExecutionVertexId();
                            if (!allDescriptors.containsKey(vertexId)) {
                                allDescriptors.put(vertexId, new HashMap<>());
                            }

                            allDescriptors
                                    .get(vertexId)
                                    .put(descriptor.getPartitionId(), descriptor);
                        });

        allDescriptors.forEach(
                (vertexId, descriptors) ->
                        getExecutionVertex(vertexId)
                                .getCurrentExecutionAttempt()
                                .recoverProducedPartitions(descriptors));

        // recover result partition bytes
        updateResultPartitionBytesMetrics(availablePartitionBytes);

        // restart all producers of missing partitions
        List<ExecutionVertexID> missingPartitionVertices =
                reconcileResult.missingPartitions.stream()
                        .map(ResultPartitionID::getPartitionId)
                        .map(this::getProducer)
                        .map(ExecutionVertex::getID)
                        .collect(Collectors.toList());

        // get all vertices need to restart according failover strategy.
        Set<ExecutionVertexID> verticesToRestart = new HashSet<>();
        for (ExecutionVertexID executionVertexId : missingPartitionVertices) {
            if (!verticesToRestart.contains(executionVertexId)) {
                verticesToRestart.addAll(
                        failoverStrategy.getTasksNeedingRestart(executionVertexId, null));
            }
        }

        // restart all vertices to deploy, to trigger the check of result partitions
        restartVertices(verticesToRestart);

        recoverFinished();
    }

    @Override
    protected void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        FailureHandlingResult wrappedResult = failureHandlingResult;
        if (isJobRecoveryEnabled && failureHandlingResult.canRestart()) {
            Set<ExecutionVertexID> originalNeedToRestartVertices =
                    failureHandlingResult.getVerticesToRestart();

            Set<JobVertexID> extraNeedToRestartJobVertices =
                    originalNeedToRestartVertices.stream()
                            .map(ExecutionVertexID::getJobVertexId)
                            .filter(finishedAndUnsupportedBatchSnapshotJobVertices::contains)
                            .collect(Collectors.toSet());

            finishedAndUnsupportedBatchSnapshotJobVertices.removeAll(extraNeedToRestartJobVertices);

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

    private ReconcileResult reconcilePartitions(Set<ResultPartitionID> actualPartitions) {
        checkState(isJobRecoveryEnabled);

        Set<ResultPartitionID> expectedPartitions =
                getExecutionGraph().getAllIntermediateResults().values().stream()
                        .flatMap(result -> Arrays.stream(result.getPartitions()))
                        .filter(this::isPartitionShouldBeReconciled)
                        .map(IntermediateResultPartition::getPartitionId)
                        .map(this::createResultPartitionId)
                        .collect(Collectors.toSet());

        Set<ResultPartitionID> unknownPartitions =
                Sets.difference(actualPartitions, expectedPartitions);
        Set<ResultPartitionID> missPartitions =
                Sets.difference(expectedPartitions, actualPartitions);
        Set<ResultPartitionID> availablePartitions =
                Sets.intersection(expectedPartitions, actualPartitions);

        return new ReconcileResult(unknownPartitions, missPartitions, availablePartitions);
    }

    private boolean isPartitionShouldBeReconciled(IntermediateResultPartition partition) {
        // 1. Check if the producer of this partition is finished.
        boolean isProducerFinished =
                getProducer(partition.getPartitionId()).getExecutionState()
                        == ExecutionState.FINISHED;

        if (!isProducerFinished) {
            return false;
        }

        // 2. Check if all downstream vertices for this partition are initialized.
        boolean allConsumersInitialized =
                partition.getIntermediateResult().getConsumerVertices().stream()
                        .allMatch(
                                jobVertexId -> getExecutionJobVertex(jobVertexId).isInitialized());

        if (!allConsumersInitialized) {
            return true;
        }

        // 3. If all downstream vertices are initialized, we need to check if the all partition
        // consumers finished.
        return getConsumers(partition.getPartitionId()).stream()
                .anyMatch(vertex -> vertex.getExecutionState() != ExecutionState.FINISHED);
    }

    private static class ReconcileResult {
        private final Set<ResultPartitionID> unknownPartitions;
        private final Set<ResultPartitionID> missingPartitions;
        private final Set<ResultPartitionID> availablePartitions;

        ReconcileResult(
                Set<ResultPartitionID> unknownPartitions,
                Set<ResultPartitionID> missingPartitions,
                Set<ResultPartitionID> availablePartitions) {
            this.unknownPartitions = checkNotNull(unknownPartitions);
            this.missingPartitions = checkNotNull(missingPartitions);
            this.availablePartitions = checkNotNull(availablePartitions);
        }
    }

    @Override
    public boolean updateTaskExecutionState(final TaskExecutionStateTransition taskExecutionState) {
        boolean success = super.updateTaskExecutionState(taskExecutionState);

        if (success
                && isJobRecoveryEnabled
                && taskExecutionState.getExecutionState() == ExecutionState.FINISHED
                && !isRecovering()) {
            final ExecutionVertexID executionVertexId =
                    taskExecutionState.getID().getExecutionVertexId();
            notifyExecutionFinished(executionVertexId, taskExecutionState);
        }
        return success;
    }

    @Override
    protected void resetForNewExecutions(Collection<ExecutionVertexID> vertices) {
        super.resetForNewExecutions(vertices);
        if (isJobRecoveryEnabled && !isRecovering()) {
            notifyExecutionVertexReset(vertices);
        }
    }

    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {
        if (isJobRecoveryEnabled && isRecovering()) {
            if (log.isDebugEnabled()) {
                log.debug("Try to deploy {} in recovering, cache them.", verticesToDeploy);
            }

            // record versions
            List<ExecutionVertexVersion> executionVertexVersions =
                    verticesToDeploy.stream()
                            .map(executionVertexVersioner::recordModification)
                            .collect(Collectors.toList());
            executionsToDeploy.add(executionVertexVersions);
        } else {
            List<ExecutionVertex> executionVertices =
                    verticesToDeploy.stream()
                            .map(this::getExecutionVertex)
                            .collect(Collectors.toList());
            enrichInputBytesForExecutionVertices(executionVertices);
            super.allocateSlotsAndDeploy(verticesToDeploy);
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
        if (isJobRecoveryEnabled && !isRecovering()) {
            notifyExecutionJobVertexInitialization(
                    jobVertex.getJobVertex().getID(), parallelism, jobVertexInputInfos);
        }
    }

    private void restartVertices(Set<ExecutionVertexID> verticesToRestart) throws Exception {
        checkState(isJobRecoveryEnabled);

        Set<JobVertexID> extraNeedToRestartJobVertices =
                verticesToRestart.stream()
                        .map(ExecutionVertexID::getJobVertexId)
                        .filter(finishedAndUnsupportedBatchSnapshotJobVertices::contains)
                        .collect(Collectors.toSet());

        finishedAndUnsupportedBatchSnapshotJobVertices.removeAll(extraNeedToRestartJobVertices);

        verticesToRestart.addAll(
                extraNeedToRestartJobVertices.stream()
                        .flatMap(
                                jobVertexId -> {
                                    ExecutionJobVertex jobVertex =
                                            getExecutionJobVertex(jobVertexId);
                                    return Arrays.stream(jobVertex.getTaskVertices())
                                            .map(ExecutionVertex::getID);
                                })
                        .collect(Collectors.toSet()));

        // record versions so that revised tasks' deployment will be skipped if it was in
        // #executionsToDeploy
        executionVertexVersioner.recordVertexModifications(verticesToRestart);

        // we only reset tasks which are not CREATED.
        Set<ExecutionVertexID> verticesToReset =
                verticesToRestart.stream()
                        .filter(
                                executionVertexID ->
                                        getExecutionVertex(executionVertexID).getExecutionState()
                                                != ExecutionState.CREATED)
                        .collect(Collectors.toSet());

        for (ExecutionVertexID executionVertexID : verticesToReset) {
            notifyCoordinatorsAboutTaskFailure(
                    getExecutionVertex(executionVertexID).getCurrentExecutionAttempt(), null);
        }
        resetForNewExecutions(verticesToReset);
        restoreState(verticesToReset, false);

        // restart vertices.
        schedulingStrategy.restartTasks(verticesToRestart);
    }

    private void notifyExecutionFinished(
            ExecutionVertexID executionVertexId, TaskExecutionStateTransition taskExecutionState) {
        checkState(isJobRecoveryEnabled);
        checkState(taskExecutionState.getExecutionState() == ExecutionState.FINISHED);
        Execution execution = getExecutionVertex(executionVertexId).getCurrentExecutionAttempt();

        // check whether the job vertex is finished.
        ExecutionJobVertex jobVertex = execution.getVertex().getJobVertex();
        boolean jobVertexFinished = jobVertex.getAggregateState() == ExecutionState.FINISHED;

        // snapshot operator coordinators if needed.
        needToSnapshotJobVertices.add(executionVertexId.getJobVertexId());
        final Map<OperatorID, CompletableFuture<byte[]>> operatorCoordinatorSnapshotFutures =
                new HashMap<>();
        CompletableFuture<ShuffleMasterSnapshot> shuffleMasterSnapshotFuture = null;
        long currentRelativeTime = clock.relativeTimeMillis();
        if (jobVertexFinished
                || (currentRelativeTime - lastSnapshotRelativeTime >= snapshotMinPauseMills)) {
            // operator coordinator
            operatorCoordinatorSnapshotFutures.putAll(snapshotOperatorCoordinators());
            lastSnapshotRelativeTime = currentRelativeTime;
            needToSnapshotJobVertices.clear();

            // shuffle master
            shuffleMasterSnapshotFuture = snapshotShuffleMaster();
        }

        // write job event.
        jobEventManager.writeEvent(
                new ExecutionVertexFinishedEvent(
                        execution.getAttemptId(),
                        execution.getAssignedResourceLocation(),
                        operatorCoordinatorSnapshotFutures,
                        shuffleMasterSnapshotFuture,
                        execution.getIOMetrics(),
                        execution.getUserAccumulators()),
                jobVertexFinished);
    }

    private CompletableFuture<ShuffleMasterSnapshot> snapshotShuffleMaster() {
        checkState(isJobRecoveryEnabled);
        checkState(shuffleMaster.supportsBatchSnapshot());
        CompletableFuture<ShuffleMasterSnapshot> shuffleMasterSnapshotFuture =
                new CompletableFuture<>();
        shuffleMaster.snapshotState(
                shuffleMasterSnapshotFuture, new DefaultShuffleMasterSnapshotContext());
        return shuffleMasterSnapshotFuture;
    }

    private void restoreShuffleMaster(List<ShuffleMasterSnapshot> snapshots) {
        checkState(isJobRecoveryEnabled);
        checkState(shuffleMaster.supportsBatchSnapshot());
        shuffleMaster.restoreState(snapshots);
    }

    private Map<OperatorID, CompletableFuture<byte[]>> snapshotOperatorCoordinators() {
        checkState(isJobRecoveryEnabled);
        final Map<OperatorID, CompletableFuture<byte[]>> snapshotFutures = new HashMap<>();

        for (JobVertexID jobVertexId : needToSnapshotJobVertices) {
            final ExecutionJobVertex jobVertex = checkNotNull(getExecutionJobVertex(jobVertexId));

            log.info(
                    "Snapshot operator coordinators of {} to job event, checkpointId {}.",
                    jobVertex.getName(),
                    NO_CHECKPOINT);

            for (OperatorCoordinatorHolder holder : jobVertex.getOperatorCoordinators()) {
                if (holder.coordinator().supportsBatchSnapshot()) {
                    final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
                    holder.checkpointCoordinator(NO_CHECKPOINT, checkpointFuture);
                    snapshotFutures.put(holder.operatorId(), checkpointFuture);
                }
            }
        }
        return snapshotFutures;
    }

    private void restoreAndConsistentOperatorCoordinators(
            Map<OperatorID, byte[]> snapshots, Map<OperatorID, JobVertexID> operatorToJobVertex)
            throws Exception {
        checkState(isJobRecoveryEnabled);
        for (Map.Entry<OperatorID, byte[]> entry : snapshots.entrySet()) {
            OperatorID operatorId = entry.getKey();
            JobVertexID jobVertexId = checkNotNull(operatorToJobVertex.get(operatorId));
            ExecutionJobVertex jobVertex = getExecutionJobVertex(jobVertexId);
            log.info(
                    "Restore operator coordinators of {} from job event, checkpointId {}.",
                    jobVertex.getName(),
                    NO_CHECKPOINT);

            for (OperatorCoordinatorHolder holder : jobVertex.getOperatorCoordinators()) {
                if (holder.coordinator().supportsBatchSnapshot()) {
                    byte[] snapshot = snapshots.get(holder.operatorId());
                    holder.resetToCheckpoint(NO_CHECKPOINT, snapshot);
                }
            }
        }
    }

    private void reviseVertices() throws Exception {
        checkState(isJobRecoveryEnabled);
        Set<ExecutionVertexID> verticesToRevise = new HashSet<>();
        for (ExecutionJobVertex jobVertex : getExecutionGraph().getAllVertices().values()) {
            if (!jobVertex.isInitialized() || jobVertex.getOperatorCoordinators().isEmpty()) {
                continue;
            }

            boolean allSupportsBatchSnapshot =
                    jobVertex.getOperatorCoordinators().stream()
                            .allMatch(holder -> holder.coordinator().supportsBatchSnapshot());

            Set<ExecutionVertexID> unfinishedTasks =
                    Arrays.stream(jobVertex.getTaskVertices())
                            .filter(vertex -> vertex.getExecutionState() != ExecutionState.FINISHED)
                            .map(
                                    executionVertex -> {
                                        // transition to terminal state to allow reset it
                                        executionVertex
                                                .getCurrentExecutionAttempt()
                                                .transitionState(ExecutionState.CANCELED);
                                        return executionVertex.getID();
                                    })
                            .collect(Collectors.toSet());

            if (allSupportsBatchSnapshot) {
                log.info(
                        "All operator coordinators of jobVertex {} support batch snapshot, "
                                + "add {} unfinished tasks to revise.",
                        jobVertex.getName(),
                        unfinishedTasks.size());
                verticesToRevise.addAll(unfinishedTasks);
            } else if (unfinishedTasks.isEmpty()) {
                log.info(
                        "JobVertex {} is finished, but not all of its operator coordinators support "
                                + "batch snapshot. Therefore, if any single task within it requires "
                                + "a restart in the future, all tasks associated with this JobVertex "
                                + "need to be restarted as well.",
                        jobVertex.getName());
                finishedAndUnsupportedBatchSnapshotJobVertices.add(jobVertex.getJobVertexId());
            } else {
                log.info(
                        "Restart all tasks of jobVertex {} because it has not been finished and not "
                                + "all of its operator coordinators support batch snapshot.",
                        jobVertex.getName());
                verticesToRevise.addAll(
                        Arrays.stream(jobVertex.getTaskVertices())
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()));
            }
        }
        restartVertices(verticesToRevise);
    }

    private void notifyExecutionVertexReset(Collection<ExecutionVertexID> vertices) {
        checkState(isJobRecoveryEnabled);
        // write execute vertex reset event.
        jobEventManager.writeEvent(new ExecutionVertexResetEvent(new ArrayList<>(vertices)), false);
    }

    private void notifyExecutionJobVertexInitialization(
            JobVertexID jobVertexId,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos) {
        checkState(isJobRecoveryEnabled);
        // write execution job vertex initialized event.
        jobEventManager.writeEvent(
                new ExecutionJobVertexInitializedEvent(
                        jobVertexId, parallelism, jobVertexInputInfos),
                false);
    }

    @VisibleForTesting
    boolean isRecovering() {
        return getExecutionGraph().getState() == JobStatus.RECONCILING;
    }

    private void enterRecovering() {
        log.info("Try to recover from JM failover.");
        getExecutionGraph().transitionState(JobStatus.CREATED, JobStatus.RECONCILING);
    }

    private void recoverFinished() {
        log.info("Job {} successfully recovered from JM failover", getJobId());

        getExecutionGraph().transitionState(JobStatus.RECONCILING, JobStatus.RUNNING);
        checkExecutionGraphState();
        getExecutionVerticesToDeploy().forEach(super::allocateSlotsAndDeploy);
    }

    private List<List<ExecutionVertexID>> getExecutionVerticesToDeploy() {
        final List<List<ExecutionVertexID>> ret = new ArrayList<>();
        for (List<ExecutionVertexVersion> vertexVersions : executionsToDeploy) {
            ret.add(
                    vertexVersions.stream()
                            .filter(
                                    version -> {
                                        ExecutionState state =
                                                getExecutionVertex(version.getExecutionVertexId())
                                                        .getExecutionState();
                                        return !executionVertexVersioner.isModified(version)
                                                && state == ExecutionState.CREATED;
                                    })
                            .map(ExecutionVertexVersion::getExecutionVertexId)
                            .collect(Collectors.toList()));
        }

        return ret;
    }

    private void recoverFailed() {
        String message =
                String.format("Job %s recover failed from JM failover, fail global.", getJobId());
        log.warn(message);

        getExecutionGraph().transitionState(JobStatus.RECONCILING, JobStatus.RUNNING);
        executionsToDeploy.clear();

        // clear job events and restart job event manager.
        jobEventManager.stop(true);
        try {
            jobEventManager.start();
        } catch (Throwable throwable) {
            failJob(
                    throwable,
                    System.currentTimeMillis(),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            return;
        }

        // call #initializeVerticesIfPossible to avoid an empty execution graph
        initializeVerticesIfPossible();
        handleGlobalFailure(
                new FlinkRuntimeException("Recover failed from JM failover, fail global."));
    }

    private void checkExecutionGraphState() {
        for (ExecutionVertex executionVertex : getExecutionGraph().getAllExecutionVertices()) {
            ExecutionState state = executionVertex.getExecutionState();
            checkState(state == ExecutionState.CREATED || state == ExecutionState.FINISHED);
        }
    }

    @Override
    public void startReplay() {
        // do nothing.
    }

    @Override
    public void replayOneEvent(JobEvent jobEvent) {
        checkState(isJobRecoveryEnabled);
        if (jobEvent instanceof ExecutionVertexFinishedEvent) {
            ExecutionVertexFinishedEvent event = (ExecutionVertexFinishedEvent) jobEvent;
            executionVertexFinishedEventMap.put(event.getExecutionVertexId(), event);
        } else if (jobEvent instanceof ExecutionVertexResetEvent) {
            ExecutionVertexResetEvent event = (ExecutionVertexResetEvent) jobEvent;
            for (ExecutionVertexID executionVertexId : event.getExecutionVertexIds()) {
                executionVertexFinishedEventMap.remove(executionVertexId);
            }
        } else if (jobEvent instanceof ExecutionJobVertexInitializedEvent) {
            jobVertexInitializedEvents.add((ExecutionJobVertexInitializedEvent) jobEvent);
        } else {
            throw new IllegalStateException("Unsupported job event " + jobEvent);
        }
    }

    @Override
    public void finalizeReplay() throws Exception {
        checkState(isJobRecoveryEnabled);
        // recover job vertex initialization info and update topology
        long currentTimeMillis = System.currentTimeMillis();
        final List<ExecutionJobVertex> initializedJobVertices = new ArrayList<>();
        for (ExecutionJobVertexInitializedEvent event : jobVertexInitializedEvents) {
            final ExecutionJobVertex jobVertex = getExecutionJobVertex(event.getJobVertexId());
            log.info("Start init jobVertex {}", jobVertex.getJobVertex().getName());
            initializeJobVertex(
                    jobVertex,
                    event.getParallelism(),
                    event.getJobVertexInputInfos(),
                    currentTimeMillis);
            initializedJobVertices.add(jobVertex);
        }
        updateTopology(initializedJobVertices);

        // eager initialize
        initializeVerticesIfPossible();
        // make sure source vertices can be scheduled.
        schedulingStrategy.startScheduling();

        // remove the last batch of vertices that do not record the states of operator coordinator
        // and shuffle master
        LinkedList<ExecutionVertexFinishedEvent> finishedEvents =
                new LinkedList<>(executionVertexFinishedEventMap.values());
        while (!finishedEvents.isEmpty()
                && !finishedEvents.getLast().hasOperatorCoordinatorAndShuffleMasterSnapshots()) {
            finishedEvents.removeLast();
        }

        if (finishedEvents.isEmpty()) {
            return;
        }

        // find the last operator coordinator state for each operator coordinator
        Map<OperatorID, byte[]> operatorCoordinatorSnapshots = new HashMap<>();

        List<ShuffleMasterSnapshot> shuffleMasterSnapshots = new ArrayList<>();

        // transition states of all vertices
        for (ExecutionVertexFinishedEvent event : finishedEvents) {
            JobVertexID jobVertexId = event.getExecutionVertexId().getJobVertexId();
            ExecutionJobVertex jobVertex = getExecutionGraph().getJobVertex(jobVertexId);
            checkState(jobVertex.isInitialized());

            int subTaskIndex = event.getExecutionVertexId().getSubtaskIndex();
            Execution execution =
                    jobVertex.getTaskVertices()[subTaskIndex].getCurrentExecutionAttempt();
            // recover execution info.
            execution.recoverExecution(
                    event.getExecutionAttemptId(), event.getTaskManagerLocation());

            // transition state to DEPLOYING.
            execution.transitionState(ExecutionState.SCHEDULED);
            execution.transitionState(ExecutionState.DEPLOYING);

            // update state to INITIALIZING
            final TaskExecutionState initializingState =
                    new TaskExecutionState(execution.getAttemptId(), ExecutionState.INITIALIZING);
            updateTaskExecutionState(new TaskExecutionStateTransition(initializingState));

            // update state to RUNNING
            final TaskExecutionState runningState =
                    new TaskExecutionState(execution.getAttemptId(), ExecutionState.RUNNING);
            updateTaskExecutionState(new TaskExecutionStateTransition(runningState));

            // update state to FINISHED, maybe trigger scheduling.
            final TaskExecutionState finishedState =
                    new TaskExecutionState(
                            execution.getAttemptId(),
                            ExecutionState.FINISHED,
                            null,
                            new AccumulatorSnapshot(
                                    getJobId(),
                                    execution.getAttemptId(),
                                    Optional.ofNullable(event.getUserAccumulators())
                                            .orElse(new ConcurrentHashMap<>(4))),
                            event.getIOMetrics());

            updateTaskExecutionState(new TaskExecutionStateTransition(finishedState));

            // operator coordinator
            for (Map.Entry<OperatorID, CompletableFuture<byte[]>> entry :
                    event.getOperatorCoordinatorSnapshotFutures().entrySet()) {
                checkState(entry.getValue().isDone());
                operatorCoordinatorSnapshots.put(entry.getKey(), entry.getValue().get());
            }

            // shuffle master
            if (event.getShuffleMasterSnapshotFuture() != null) {
                ShuffleMasterSnapshot shuffleMasterSnapshot =
                        event.getShuffleMasterSnapshotFuture().get();
                if (shuffleMasterSnapshot.isIncremental()) {
                    shuffleMasterSnapshots.add(shuffleMasterSnapshot);
                } else {
                    shuffleMasterSnapshots = Arrays.asList(shuffleMasterSnapshot);
                }
            }
        }

        // restore operator coordinator state if needed.
        final Map<OperatorID, JobVertexID> operatorToJobVertex = new HashMap<>();
        for (ExecutionJobVertex jobVertex : getExecutionGraph().getAllVertices().values()) {
            if (!jobVertex.isInitialized()) {
                continue;
            }

            for (OperatorCoordinatorHolder holder : jobVertex.getOperatorCoordinators()) {
                operatorToJobVertex.put(holder.operatorId(), jobVertex.getJobVertexId());
            }
        }

        try {
            restoreAndConsistentOperatorCoordinators(
                    operatorCoordinatorSnapshots, operatorToJobVertex);
        } catch (Exception exception) {
            log.warn("Restore coordinator operator failed.", exception);
            throw exception;
        }

        reviseVertices();

        // restore shuffle master state
        restoreShuffleMaster(shuffleMasterSnapshots);
    }

    @Override
    protected void onTaskFinished(final Execution execution, final IOMetrics ioMetrics) {
        checkNotNull(ioMetrics);
        updateResultPartitionBytesMetrics(ioMetrics.getResultPartitionBytes());
        ExecutionVertexVersion currentVersion =
                executionVertexVersioner.getExecutionVertexVersion(execution.getVertex().getID());

        BiConsumer<Void, Throwable> onTaskFinishedAction =
                (Void value, Throwable throwable) -> {
                    if (executionVertexVersioner.isModified(currentVersion)) {
                        log.debug(
                                "Initialization of vertices will be skipped, because the execution"
                                        + " vertex version has been modified.");
                        return;
                    }
                    initializeVerticesIfPossible();
                    super.onTaskFinished(execution, ioMetrics);
                };

        if (isRecovering()) {
            tryComputeSourceParallelismThenRunSync(onTaskFinishedAction);
        } else {
            tryComputeSourceParallelismThenRunAsync(onTaskFinishedAction);
        }
    }

    private void updateResultPartitionBytesMetrics(
            @Nullable
                    Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
        if (resultPartitionBytes == null) {
            // During recovering, the result partition bytes info will be recovered in method
            // #finalizeReplay, the argument passed from method #onTaskFinished is null
            return;
        }

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
    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
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

    private void tryComputeSourceParallelismThenRunSync(BiConsumer<Void, Throwable> action) {
        FutureUtils.ConjunctFuture<Void> dynamicSourceParallelismFutures =
                FutureUtils.waitForAll(computeDynamicSourceParallelism());
        try {
            dynamicSourceParallelismFutures.join();
            action.accept(null, null);
        } catch (Exception e) {
            log.error("An unexpected error occurred while scheduling.", e);
            this.handleGlobalFailure(new SuppressRestartsException(e));
        }
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
                    // decided, so we can call the
                    // ExecutionGraph#initializeJobVertex(ExecutionJobVertex, long) to initialize.
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
        if (!jobVertex.isParallelismDecided()
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertex.getJobVertexId(),
                    vertexInitialParallelism);
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
        // This method resets the parallelism of JobVertices within a JobGraph to their initial
        // state after JM failover.
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
}
