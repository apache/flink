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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexInitializedEvent;
import org.apache.flink.runtime.jobmaster.event.ExecutionVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.ExecutionVertexResetEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.runtime.jobmaster.event.JobEventManager;
import org.apache.flink.runtime.jobmaster.event.JobEventReplayHandler;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.DefaultShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.operators.coordination.OperatorCoordinator.NO_CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link BatchJobRecoveryHandler} and {@link JobEventReplayHandler}. */
public class DefaultBatchJobRecoveryHandler
        implements BatchJobRecoveryHandler, JobEventReplayHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JobEventManager jobEventManager;

    private BatchJobRecoveryContext context;

    /** The timestamp (via {@link Clock#relativeTimeMillis()}) of the last snapshot. */
    private long lastSnapshotRelativeTime;

    private final Set<JobVertexID> needToSnapshotJobVertices = new HashSet<>();

    private static final ResourceID UNKNOWN_PRODUCER = ResourceID.generate();

    private final long snapshotMinPauseMills;

    private Clock clock;

    private final Map<ExecutionVertexID, ExecutionVertexFinishedEvent>
            executionVertexFinishedEventMap = new LinkedHashMap<>();

    private final List<ExecutionJobVertexInitializedEvent> jobVertexInitializedEvents =
            new ArrayList<>();

    /**
     * A set of JobVertex Ids associated with JobVertices whose operatorCoordinators did not
     * successfully recover. And if any execution within these job vertices needs to be restarted in
     * the future, all other executions within the same vertex must also be restarted to ensure the
     * consistency and correctness of the state.
     */
    private final Set<JobVertexID> jobVerticesWithUnRecoveredCoordinators = new HashSet<>();

    private final Duration previousWorkerRecoveryTimeout;

    public DefaultBatchJobRecoveryHandler(
            JobEventManager jobEventManager, Configuration jobMasterConfiguration) {
        this.jobEventManager = jobEventManager;

        this.previousWorkerRecoveryTimeout =
                jobMasterConfiguration.get(
                        BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT);
        this.snapshotMinPauseMills =
                jobMasterConfiguration
                        .get(BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE)
                        .toMillis();
    }

    @Override
    public void initialize(BatchJobRecoveryContext context) {
        this.context = checkNotNull(context);
        this.clock = SystemClock.getInstance();

        try {
            jobEventManager.start();
        } catch (Throwable throwable) {
            context.failJob(
                    throwable,
                    System.currentTimeMillis(),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Override
    public void stop(boolean cleanUp) {
        jobEventManager.stop(cleanUp);
    }

    @Override
    public void startRecovering() {
        context.getMainThreadExecutor().assertRunningInMainThread();

        startRecoveringInternal();

        // notify the shuffle master the recovery process has started and try to fetch partitions
        context.getShuffleMaster()
                .notifyPartitionRecoveryStarted(context.getExecutionGraph().getJobID());

        if (!jobEventManager.replay(this)) {
            log.warn(
                    "Fail to replay log for {}, will start the job as a new one.",
                    context.getExecutionGraph().getJobID());
            recoverFailed();
            return;
        }
        log.info("Replay all job events successfully.");

        recoverPartitions()
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                recoverFailed();
                            }
                            try {
                                recoverFinished();
                            } catch (Exception exception) {
                                recoverFailed();
                            }
                        });
    }

    @Override
    public boolean needRecover() {
        try {
            return jobEventManager.hasJobEvents();
        } catch (Throwable throwable) {
            context.failJob(
                    throwable,
                    System.currentTimeMillis(),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            return false;
        }
    }

    @Override
    public boolean isRecovering() {
        return context.getExecutionGraph().getState() == JobStatus.RECONCILING;
    }

    private void restoreShuffleMaster(List<ShuffleMasterSnapshot> snapshots) {
        checkState(context.getShuffleMaster().supportsBatchSnapshot());
        context.getShuffleMaster().restoreState(snapshots);
    }

    private void startRecoveringInternal() {
        log.info("Try to recover status from previously failed job master.");
        context.getExecutionGraph().transitionState(JobStatus.CREATED, JobStatus.RECONCILING);
    }

    private void restoreOperatorCoordinators(
            Map<OperatorID, byte[]> snapshots, Map<OperatorID, JobVertexID> operatorToJobVertex)
            throws Exception {
        for (Map.Entry<OperatorID, byte[]> entry : snapshots.entrySet()) {
            OperatorID operatorId = entry.getKey();
            JobVertexID jobVertexId = checkNotNull(operatorToJobVertex.get(operatorId));
            ExecutionJobVertex jobVertex = getExecutionJobVertex(jobVertexId);
            log.info("Restore operator coordinators of {} from job event.", jobVertex.getName());

            for (OperatorCoordinatorHolder holder : jobVertex.getOperatorCoordinators()) {
                if (holder.coordinator().supportsBatchSnapshot()) {
                    byte[] snapshot = snapshots.get(holder.operatorId());
                    holder.resetToCheckpoint(NO_CHECKPOINT, snapshot);
                }
            }
        }

        determineVerticesForResetAfterRestoreOpCoordinator();
    }

    @Override
    public void startReplay() {
        // do nothing.
    }

    @Override
    public void replayOneEvent(JobEvent jobEvent) {
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
        // recover job vertex initialization info and update topology
        long currentTimeMillis = System.currentTimeMillis();
        final List<ExecutionJobVertex> initializedJobVertices = new ArrayList<>();
        for (ExecutionJobVertexInitializedEvent event : jobVertexInitializedEvents) {
            final ExecutionJobVertex jobVertex = getExecutionJobVertex(event.getJobVertexId());
            context.initializeJobVertex(
                    jobVertex,
                    event.getParallelism(),
                    event.getJobVertexInputInfos(),
                    currentTimeMillis);
            initializedJobVertices.add(jobVertex);
        }
        context.updateTopology(initializedJobVertices);

        // Because we will take operator coordinator and shuffle master snapshots and persisted
        // externally periodically. As a result, any events in the final batch that do not have an
        // associated snapshot are redundant and can be disregarded.
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
            ExecutionJobVertex jobVertex = context.getExecutionGraph().getJobVertex(jobVertexId);
            checkState(jobVertex.isInitialized());

            int subTaskIndex = event.getExecutionVertexId().getSubtaskIndex();
            Execution execution =
                    jobVertex.getTaskVertices()[subTaskIndex].getCurrentExecutionAttempt();
            // recover execution info.
            execution.recoverExecution(
                    event.getExecutionAttemptId(),
                    event.getTaskManagerLocation(),
                    event.getUserAccumulators(),
                    event.getIOMetrics());

            // recover operator coordinator
            for (Map.Entry<OperatorID, CompletableFuture<byte[]>> entry :
                    event.getOperatorCoordinatorSnapshotFutures().entrySet()) {
                checkState(entry.getValue().isDone());
                operatorCoordinatorSnapshots.put(entry.getKey(), entry.getValue().get());
            }

            // recover shuffle master
            if (event.getShuffleMasterSnapshotFuture() != null) {
                checkState(event.getShuffleMasterSnapshotFuture().isDone());

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
        for (ExecutionJobVertex jobVertex : context.getExecutionGraph().getAllVertices().values()) {
            if (!jobVertex.isInitialized()) {
                continue;
            }

            for (OperatorCoordinatorHolder holder : jobVertex.getOperatorCoordinators()) {
                operatorToJobVertex.put(holder.operatorId(), jobVertex.getJobVertexId());
            }
        }

        try {
            restoreOperatorCoordinators(operatorCoordinatorSnapshots, operatorToJobVertex);
        } catch (Exception exception) {
            log.warn("Restore coordinator operator failed.", exception);
            throw exception;
        }

        // restore shuffle master
        restoreShuffleMaster(shuffleMasterSnapshots);
    }

    @Override
    public void onExecutionVertexReset(Collection<ExecutionVertexID> vertices) {
        // write execute vertex reset event.
        checkState(!isRecovering());
        jobEventManager.writeEvent(new ExecutionVertexResetEvent(new ArrayList<>(vertices)), false);
    }

    @Override
    public void onExecutionJobVertexInitialization(
            JobVertexID jobVertexId,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos) {
        // write execution job vertex initialized event.
        checkState(!isRecovering());
        jobEventManager.writeEvent(
                new ExecutionJobVertexInitializedEvent(
                        jobVertexId, parallelism, jobVertexInputInfos),
                false);
    }

    @Override
    public void onExecutionFinished(ExecutionVertexID executionVertexId) {
        checkState(!isRecovering());

        Execution execution = getExecutionVertex(executionVertexId).getCurrentExecutionAttempt();

        // check whether the job vertex is finished.
        ExecutionJobVertex jobVertex = execution.getVertex().getJobVertex();
        boolean jobVertexFinished = jobVertex.getAggregateState() == ExecutionState.FINISHED;

        // snapshot operator coordinators and shuffle master if needed.
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

    private Map<OperatorID, CompletableFuture<byte[]>> snapshotOperatorCoordinators() {

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

    private CompletableFuture<ShuffleMasterSnapshot> snapshotShuffleMaster() {

        checkState(context.getShuffleMaster().supportsBatchSnapshot());
        CompletableFuture<ShuffleMasterSnapshot> shuffleMasterSnapshotFuture =
                new CompletableFuture<>();
        context.getShuffleMaster()
                .snapshotState(
                        shuffleMasterSnapshotFuture, new DefaultShuffleMasterSnapshotContext());
        return shuffleMasterSnapshotFuture;
    }

    private void determineVerticesForResetAfterRestoreOpCoordinator() throws Exception {
        Set<ExecutionVertexID> verticesToReset = new HashSet<>();

        for (ExecutionJobVertex jobVertex : context.getExecutionGraph().getAllVertices().values()) {
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
                verticesToReset.addAll(unfinishedTasks);
            } else if (unfinishedTasks.isEmpty()) {
                log.info(
                        "JobVertex {} is finished, but not all of its operator coordinators support "
                                + "batch snapshot. Therefore, if any single task within it requires "
                                + "a restart in the future, all tasks associated with this JobVertex "
                                + "need to be restarted as well.",
                        jobVertex.getName());
                jobVerticesWithUnRecoveredCoordinators.add(jobVertex.getJobVertexId());
            } else {
                log.info(
                        "Restart all tasks of jobVertex {} because it has not been finished and not "
                                + "all of its operator coordinators support batch snapshot.",
                        jobVertex.getName());
                verticesToReset.addAll(
                        Arrays.stream(jobVertex.getTaskVertices())
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toSet()));
            }
        }

        resetVerticesInRecovering(verticesToReset, false);
    }

    private void resetVerticesInRecovering(
            Set<ExecutionVertexID> nextVertices, boolean baseOnResultPartitionConsumable)
            throws Exception {
        checkState(isRecovering());

        Set<ExecutionVertexID> verticesToRestart = new HashSet<>();
        while (!nextVertices.isEmpty()) {
            for (ExecutionVertexID executionVertexId : nextVertices) {
                if (!verticesToRestart.contains(executionVertexId)) {
                    verticesToRestart.addAll(
                            context.getTasksNeedingRestart(
                                    executionVertexId, baseOnResultPartitionConsumable));
                }
            }

            Set<JobVertexID> extraNeedToRestartJobVertices =
                    verticesToRestart.stream()
                            .map(ExecutionVertexID::getJobVertexId)
                            .filter(jobVerticesWithUnRecoveredCoordinators::contains)
                            .collect(Collectors.toSet());
            jobVerticesWithUnRecoveredCoordinators.removeAll(extraNeedToRestartJobVertices);

            nextVertices =
                    extraNeedToRestartJobVertices.stream()
                            .flatMap(
                                    jobVertexId -> {
                                        ExecutionJobVertex jobVertex =
                                                getExecutionJobVertex(jobVertexId);
                                        return Arrays.stream(jobVertex.getTaskVertices())
                                                .map(ExecutionVertex::getID);
                                    })
                            .collect(Collectors.toSet());
        }

        // we only reset tasks which are not CREATED.
        Set<ExecutionVertexID> verticesToReset =
                verticesToRestart.stream()
                        .filter(
                                executionVertexID ->
                                        getExecutionVertex(executionVertexID).getExecutionState()
                                                != ExecutionState.CREATED)
                        .collect(Collectors.toSet());

        context.resetVerticesInRecovering(verticesToReset);
    }

    private void recoverFailed() {
        String message =
                String.format(
                        "Job %s recover failed from JM failover, fail global.",
                        context.getExecutionGraph().getJobID());
        log.warn(message);
        context.getExecutionGraph().transitionState(JobStatus.RECONCILING, JobStatus.RUNNING);

        // clear job events and restart job event manager.
        jobEventManager.stop(true);
        try {
            jobEventManager.start();
        } catch (Throwable throwable) {
            context.failJob(
                    throwable,
                    System.currentTimeMillis(),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            return;
        }

        context.onRecoveringFailed();
    }

    private void recoverFinished() {
        log.info(
                "Job {} successfully recovered from JM failover",
                context.getExecutionGraph().getJobID());

        context.getExecutionGraph().transitionState(JobStatus.RECONCILING, JobStatus.RUNNING);
        checkExecutionGraphState();
        context.onRecoveringFinished(jobVerticesWithUnRecoveredCoordinators);
    }

    private void checkExecutionGraphState() {
        for (ExecutionVertex executionVertex :
                context.getExecutionGraph().getAllExecutionVertices()) {
            ExecutionState state = executionVertex.getExecutionState();
            checkState(state == ExecutionState.CREATED || state == ExecutionState.FINISHED);
        }
    }

    private CompletableFuture<Void> recoverPartitions() {
        context.getMainThreadExecutor().assertRunningInMainThread();

        CompletableFuture<Tuple2<ReconcileResult, Collection<PartitionWithMetrics>>>
                reconcilePartitionsFuture = reconcilePartitions();

        return reconcilePartitionsFuture.thenAccept(
                tuple2 -> {
                    ReconcileResult reconcileResult = tuple2.f0;
                    Collection<PartitionWithMetrics> partitionWithMetrics = tuple2.f1;

                    log.info(
                            "Partitions to be released: {}, missed partitions: {}, partitions to be reserved: {}.",
                            reconcileResult.partitionsToRelease,
                            reconcileResult.partitionsMissing,
                            reconcileResult.partitionsToReserve);

                    // release partitions which is no more needed.
                    ((InternalExecutionGraphAccessor) context.getExecutionGraph())
                            .getPartitionTracker()
                            .stopTrackingAndReleasePartitions(reconcileResult.partitionsToRelease);

                    // start tracking all partitions should be reserved
                    Map<IntermediateResultPartitionID, ResultPartitionBytes>
                            availablePartitionBytes = new HashMap<>();
                    partitionWithMetrics.stream()
                            .filter(
                                    partitionAndMetric ->
                                            reconcileResult.partitionsToReserve.contains(
                                                    partitionAndMetric
                                                            .getPartition()
                                                            .getResultPartitionID()))
                            .forEach(
                                    partitionAndMetric -> {
                                        ShuffleDescriptor shuffleDescriptor =
                                                partitionAndMetric.getPartition();

                                        // we cannot get the producer id when using remote shuffle
                                        ResourceID producerTaskExecutorId = UNKNOWN_PRODUCER;
                                        if (shuffleDescriptor
                                                .storesLocalResourcesOn()
                                                .isPresent()) {
                                            producerTaskExecutorId =
                                                    shuffleDescriptor
                                                            .storesLocalResourcesOn()
                                                            .get();
                                        }
                                        IntermediateResultPartition partition =
                                                context.getExecutionGraph()
                                                        .getResultPartitionOrThrow(
                                                                shuffleDescriptor
                                                                        .getResultPartitionID()
                                                                        .getPartitionId());
                                        ((InternalExecutionGraphAccessor)
                                                        context.getExecutionGraph())
                                                .getPartitionTracker()
                                                .startTrackingPartition(
                                                        producerTaskExecutorId,
                                                        Execution
                                                                .createResultPartitionDeploymentDescriptor(
                                                                        partition,
                                                                        shuffleDescriptor));

                                        availablePartitionBytes.put(
                                                shuffleDescriptor
                                                        .getResultPartitionID()
                                                        .getPartitionId(),
                                                partitionAndMetric
                                                        .getPartitionMetrics()
                                                        .getPartitionBytes());
                                    });

                    // recover the produced partitions for executions
                    Map<
                                    ExecutionVertexID,
                                    Map<
                                            IntermediateResultPartitionID,
                                            ResultPartitionDeploymentDescriptor>>
                            allDescriptors = new HashMap<>();
                    ((InternalExecutionGraphAccessor) context.getExecutionGraph())
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
                    context.updateResultPartitionBytesMetrics(availablePartitionBytes);

                    // restart all producers of missing partitions
                    Set<ExecutionVertexID> missingPartitionVertices =
                            reconcileResult.partitionsMissing.stream()
                                    .map(ResultPartitionID::getPartitionId)
                                    .map(this::getProducer)
                                    .map(ExecutionVertex::getID)
                                    .collect(Collectors.toSet());

                    try {
                        resetVerticesInRecovering(missingPartitionVertices, true);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private CompletableFuture<Tuple2<ReconcileResult, Collection<PartitionWithMetrics>>>
            reconcilePartitions() {
        List<IntermediateResultPartition> partitions =
                context.getExecutionGraph().getAllIntermediateResults().values().stream()
                        .flatMap(result -> Arrays.stream(result.getPartitions()))
                        .collect(Collectors.toList());

        Set<ResultPartitionID> partitionsToReserve = new HashSet<>();
        Set<ResultPartitionID> partitionsToRelease = new HashSet<>();
        for (IntermediateResultPartition partition : partitions) {
            PartitionReservationStatus reserveStatus = getPartitionReservationStatus(partition);

            if (reserveStatus.equals(PartitionReservationStatus.RESERVE)) {
                partitionsToReserve.add(createResultPartitionId(partition.getPartitionId()));
            } else if (reserveStatus.equals(PartitionReservationStatus.RELEASE)) {
                partitionsToRelease.add(createResultPartitionId(partition.getPartitionId()));
            }
        }

        CompletableFuture<Collection<PartitionWithMetrics>> fetchPartitionsFuture =
                context.getShuffleMaster()
                        .getPartitionWithMetrics(
                                context.getExecutionGraph().getJobID(),
                                previousWorkerRecoveryTimeout,
                                partitionsToReserve);

        return fetchPartitionsFuture.thenApplyAsync(
                partitionWithMetrics -> {
                    Set<ResultPartitionID> actualPartitions =
                            partitionWithMetrics.stream()
                                    .map(PartitionWithMetrics::getPartition)
                                    .map(ShuffleDescriptor::getResultPartitionID)
                                    .collect(Collectors.toSet());

                    Set<ResultPartitionID> actualpartitionsToRelease =
                            Sets.intersection(partitionsToRelease, actualPartitions);
                    Set<ResultPartitionID> actualpartitionsMissing =
                            Sets.difference(partitionsToReserve, actualPartitions);
                    Set<ResultPartitionID> actualpartitionsToReserve =
                            Sets.intersection(partitionsToReserve, actualPartitions);

                    return Tuple2.of(
                            new ReconcileResult(
                                    actualpartitionsToRelease,
                                    actualpartitionsMissing,
                                    actualpartitionsToReserve),
                            partitionWithMetrics);
                },
                context.getMainThreadExecutor());
    }

    private ResultPartitionID createResultPartitionId(IntermediateResultPartitionID partitionId) {
        final Execution producer = getProducer(partitionId).getPartitionProducer();
        return new ResultPartitionID(partitionId, producer.getAttemptId());
    }

    private ExecutionVertex getProducer(IntermediateResultPartitionID partitionId) {
        return context.getExecutionGraph().getResultPartitionOrThrow(partitionId).getProducer();
    }

    private PartitionReservationStatus getPartitionReservationStatus(
            IntermediateResultPartition partition) {
        // 1. Check if the producer of this partition is finished.
        ExecutionVertex producer = getProducer(partition.getPartitionId());
        boolean isProducerFinished = producer.getExecutionState() == ExecutionState.FINISHED;
        if (!isProducerFinished) {
            return PartitionReservationStatus.RELEASE;
        }

        // 2. Check if not all the consumer vertices for this partition are initialized.
        boolean allConsumersInitialized =
                partition.getIntermediateResult().getConsumerVertices().stream()
                        .allMatch(
                                jobVertexId -> getExecutionJobVertex(jobVertexId).isInitialized());

        if (!allConsumersInitialized) {
            return PartitionReservationStatus.RESERVE;
        }

        // 3. If all downstream vertices are finished, we need reserve the partitions. Otherwise, we
        // could reserve them if fetched from shuffle master.
        return getConsumers(partition.getPartitionId()).stream()
                        .anyMatch(vertex -> vertex.getExecutionState() != ExecutionState.FINISHED)
                ? PartitionReservationStatus.RESERVE
                : PartitionReservationStatus.OPTIONAL;
    }

    /** Enum that specifies the reservation status of a partition. */
    private enum PartitionReservationStatus {
        // Indicates the partition should be released.
        RELEASE,

        // Indicates the partition should be reserved.
        RESERVE,

        // Indicates the partition's reservation is preferred but not mandatory.
        OPTIONAL
    }

    private List<ExecutionVertex> getConsumers(IntermediateResultPartitionID partitionId) {
        List<ConsumerVertexGroup> consumerVertexGroups =
                context.getExecutionGraph()
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

    private ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
        return context.getExecutionGraph()
                .getAllVertices()
                .get(executionVertexId.getJobVertexId())
                .getTaskVertices()[executionVertexId.getSubtaskIndex()];
    }

    private ExecutionJobVertex getExecutionJobVertex(final JobVertexID jobVertexId) {
        return context.getExecutionGraph().getAllVertices().get(jobVertexId);
    }

    private static class ReconcileResult {
        private final Set<ResultPartitionID> partitionsToRelease;
        private final Set<ResultPartitionID> partitionsMissing;
        private final Set<ResultPartitionID> partitionsToReserve;

        ReconcileResult(
                Set<ResultPartitionID> partitionsToRelease,
                Set<ResultPartitionID> partitionsMissing,
                Set<ResultPartitionID> partitionsToReserve) {
            this.partitionsToRelease = checkNotNull(partitionsToRelease);
            this.partitionsMissing = checkNotNull(partitionsMissing);
            this.partitionsToReserve = checkNotNull(partitionsToReserve);
        }
    }
}
