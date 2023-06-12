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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.failure.DefaultFailureEnricherContext;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The future default scheduler. */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

    protected final Logger log;

    private final ClassLoader userCodeLoader;

    protected final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionFailureHandler executionFailureHandler;

    private final ScheduledExecutor delayExecutor;

    private final SchedulingStrategy schedulingStrategy;

    private final ExecutionOperations executionOperations;

    private final Set<ExecutionVertexID> verticesWaitingForRestart;

    private final ShuffleMaster<?> shuffleMaster;

    private final Map<AllocationID, Long> reservedAllocationRefCounters;

    // once an execution vertex is assigned an allocation/slot, it will reserve the allocation
    // until it is assigned a new allocation, or it finishes and does not need the allocation
    // anymore. The reserved allocation information is needed for local recovery.
    private final Map<ExecutionVertexID, AllocationID> reservedAllocationByExecutionVertex;

    protected final ExecutionDeployer executionDeployer;

    protected DefaultScheduler(
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
            final VertexParallelismStore vertexParallelismStore,
            final ExecutionDeployer.Factory executionDeployerFactory)
            throws Exception {

        super(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                executionVertexVersioner,
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                executionGraphFactory,
                vertexParallelismStore);

        this.log = log;

        this.delayExecutor = checkNotNull(delayExecutor);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.executionOperations = checkNotNull(executionOperations);
        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.reservedAllocationRefCounters = new HashMap<>();
        this.reservedAllocationByExecutionVertex = new HashMap<>();

        final FailoverStrategy failoverStrategy =
                failoverStrategyFactory.create(
                        getSchedulingTopology(), getResultPartitionAvailabilityChecker());
        log.info(
                "Using failover strategy {} for {} ({}).",
                failoverStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        final Context taskFailureCtx =
                DefaultFailureEnricherContext.forTaskFailure(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        jobManagerJobMetricGroup,
                        ioExecutor,
                        userCodeLoader);

        final Context globalFailureCtx =
                DefaultFailureEnricherContext.forGlobalFailure(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        jobManagerJobMetricGroup,
                        ioExecutor,
                        userCodeLoader);

        this.executionFailureHandler =
                new ExecutionFailureHandler(
                        getSchedulingTopology(),
                        failoverStrategy,
                        restartBackoffTimeStrategy,
                        mainThreadExecutor,
                        failureEnrichers,
                        taskFailureCtx,
                        globalFailureCtx);

        this.schedulingStrategy =
                schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

        this.executionSlotAllocator =
                checkNotNull(executionSlotAllocatorFactory)
                        .createInstance(new DefaultExecutionSlotAllocationContext());

        this.verticesWaitingForRestart = new HashSet<>();
        startUpAction.accept(mainThreadExecutor);

        this.executionDeployer =
                executionDeployerFactory.createInstance(
                        log,
                        executionSlotAllocator,
                        executionOperations,
                        executionVertexVersioner,
                        rpcTimeout,
                        this::startReserveAllocation,
                        mainThreadExecutor);
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    protected long getNumberOfRestarts() {
        return executionFailureHandler.getNumberOfRestarts();
    }

    @Override
    protected void cancelAllPendingSlotRequestsInternal() {
        getSchedulingTopology()
                .getVertices()
                .forEach(ev -> cancelAllPendingSlotRequestsForVertex(ev.getId()));
    }

    @Override
    protected void startSchedulingInternal() {
        log.info(
                "Starting scheduling with scheduling strategy [{}]",
                schedulingStrategy.getClass().getName());
        transitionToRunning();
        schedulingStrategy.startScheduling();
    }

    @Override
    protected void onTaskFinished(final Execution execution, final IOMetrics ioMetrics) {
        checkState(execution.getState() == ExecutionState.FINISHED);

        final ExecutionVertexID executionVertexId = execution.getVertex().getID();
        // once a task finishes, it will release the assigned allocation/slot and no longer
        // needs it. Therefore, it should stop reserving the slot so that other tasks are
        // possible to use the slot. Ideally, the `stopReserveAllocation` should happen
        // along with the release slot process. However, that process is hidden in the depth
        // of the ExecutionGraph, so we currently do it in DefaultScheduler after that process
        // is done.
        stopReserveAllocation(executionVertexId);

        schedulingStrategy.onExecutionStateChange(executionVertexId, ExecutionState.FINISHED);
    }

    @Override
    protected void onTaskFailed(final Execution execution) {
        checkState(execution.getState() == ExecutionState.FAILED);
        checkState(execution.getFailureInfo().isPresent());

        final Throwable error =
                execution.getFailureInfo().get().getException().deserializeError(userCodeLoader);
        handleTaskFailure(
                execution,
                maybeTranslateToClusterDatasetException(error, execution.getVertex().getID()));
    }

    protected void handleTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        maybeRestartTasks(recordTaskFailure(failedExecution, error));
    }

    protected FailureHandlingResult recordTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);
        notifyCoordinatorsAboutTaskFailure(failedExecution, error);

        return executionFailureHandler.getFailureHandlingResult(failedExecution, error, timestamp);
    }

    private Throwable maybeTranslateToClusterDatasetException(
            @Nullable Throwable cause, ExecutionVertexID failedVertex) {
        if (!(cause instanceof PartitionException)) {
            return cause;
        }

        final List<IntermediateDataSetID> intermediateDataSetIdsToConsume =
                getExecutionJobVertex(failedVertex.getJobVertexId())
                        .getJobVertex()
                        .getIntermediateDataSetIdsToConsume();
        final IntermediateResultPartitionID failedPartitionId =
                ((PartitionException) cause).getPartitionId().getPartitionId();

        if (!intermediateDataSetIdsToConsume.contains(
                failedPartitionId.getIntermediateDataSetID())) {
            return cause;
        }

        return new ClusterDatasetCorruptedException(
                cause, Collections.singletonList(failedPartitionId.getIntermediateDataSetID()));
    }

    private void notifyCoordinatorsAboutTaskFailure(
            final Execution execution, @Nullable final Throwable error) {
        final ExecutionJobVertex jobVertex = execution.getVertex().getJobVertex();
        final int subtaskIndex = execution.getParallelSubtaskIndex();
        final int attemptNumber = execution.getAttemptNumber();

        jobVertex
                .getOperatorCoordinators()
                .forEach(c -> c.executionAttemptFailed(subtaskIndex, attemptNumber, error));
    }

    @Override
    public void handleGlobalFailure(final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);

        log.info("Trying to recover from a global failure.", error);
        final FailureHandlingResult failureHandlingResult =
                executionFailureHandler.getGlobalFailureHandlingResult(error, timestamp);
        maybeRestartTasks(failureHandlingResult);
    }

    private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        if (failureHandlingResult.canRestart()) {
            restartTasksWithDelay(failureHandlingResult);
        } else {
            failJob(
                    failureHandlingResult.getError(),
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels());
        }
    }

    private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
        final Set<ExecutionVertexID> verticesToRestart =
                failureHandlingResult.getVerticesToRestart();

        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(verticesToRestart)
                                .values());
        final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

        if (globalRecovery) {
            log.info(
                    "{} tasks will be restarted to recover from a global failure.",
                    verticesToRestart.size());
        } else {
            checkArgument(failureHandlingResult.getFailedExecution().isPresent());
            log.info(
                    "{} tasks will be restarted to recover the failed task {}.",
                    verticesToRestart.size(),
                    failureHandlingResult.getFailedExecution().get().getAttemptId());
        }

        addVerticesToRestartPending(verticesToRestart);

        final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

        final FailureHandlingResultSnapshot failureHandlingResultSnapshot =
                createFailureHandlingResultSnapshot(failureHandlingResult);
        delayExecutor.schedule(
                () ->
                        FutureUtils.assertNoException(
                                cancelFuture.thenRunAsync(
                                        () -> {
                                            archiveFromFailureHandlingResult(
                                                    failureHandlingResultSnapshot);
                                            restartTasks(executionVertexVersions, globalRecovery);
                                        },
                                        getMainThreadExecutor())),
                failureHandlingResult.getRestartDelayMS(),
                TimeUnit.MILLISECONDS);
    }

    protected FailureHandlingResultSnapshot createFailureHandlingResultSnapshot(
            final FailureHandlingResult failureHandlingResult) {
        return FailureHandlingResultSnapshot.create(
                failureHandlingResult, id -> getExecutionVertex(id).getCurrentExecutions());
    }

    private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.addAll(verticesToRestart);
        transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
    }

    private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.removeAll(verticesToRestart);
        if (verticesWaitingForRestart.isEmpty()) {
            transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
        }
    }

    private void restartTasks(
            final Set<ExecutionVertexVersion> executionVertexVersions,
            final boolean isGlobalRecovery) {
        final Set<ExecutionVertexID> verticesToRestart =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        if (verticesToRestart.isEmpty()) {
            return;
        }

        removeVerticesFromRestartPending(verticesToRestart);

        resetForNewExecutions(verticesToRestart);

        try {
            restoreState(verticesToRestart, isGlobalRecovery);
        } catch (Throwable t) {
            handleGlobalFailure(t);
            return;
        }

        schedulingStrategy.restartTasks(verticesToRestart);
    }

    private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
        // clean up all the related pending requests to avoid that immediately returned slot
        // is used to fulfill the pending requests of these tasks
        cancelAllPendingSlotRequestsForVertices(verticesToRestart);

        final List<CompletableFuture<?>> cancelFutures =
                verticesToRestart.stream()
                        .map(this::cancelExecutionVertex)
                        .collect(Collectors.toList());

        return FutureUtils.combineAll(cancelFutures);
    }

    private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
        return FutureUtils.combineAll(
                getExecutionVertex(executionVertexId).getCurrentExecutions().stream()
                        .map(this::cancelExecution)
                        .collect(Collectors.toList()));
    }

    protected CompletableFuture<?> cancelExecution(final Execution execution) {
        notifyCoordinatorOfCancellation(execution);
        return executionOperations.cancel(execution);
    }

    private void cancelAllPendingSlotRequestsForVertices(
            final Set<ExecutionVertexID> executionVertices) {
        executionVertices.forEach(this::cancelAllPendingSlotRequestsForVertex);
    }

    protected void cancelAllPendingSlotRequestsForVertex(
            final ExecutionVertexID executionVertexId) {
        getExecutionVertex(executionVertexId)
                .getCurrentExecutions()
                .forEach(e -> executionSlotAllocator.cancel(e.getAttemptId()));
    }

    private Execution getCurrentExecutionOfVertex(ExecutionVertexID executionVertexId) {
        return getExecutionVertex(executionVertexId).getCurrentExecutionAttempt();
    }

    // ------------------------------------------------------------------------
    // SchedulerOperations
    // ------------------------------------------------------------------------

    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {
        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
                executionVertexVersioner.recordVertexModifications(verticesToDeploy);

        final List<Execution> executionsToDeploy =
                verticesToDeploy.stream()
                        .map(this::getCurrentExecutionOfVertex)
                        .collect(Collectors.toList());

        executionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex);
    }

    private void startReserveAllocation(
            ExecutionVertexID executionVertexId, AllocationID newAllocation) {

        // stop the previous allocation reservation if there is one
        stopReserveAllocation(executionVertexId);

        reservedAllocationByExecutionVertex.put(executionVertexId, newAllocation);
        reservedAllocationRefCounters.compute(
                newAllocation, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
    }

    private void stopReserveAllocation(ExecutionVertexID executionVertexId) {
        final AllocationID priorAllocation =
                reservedAllocationByExecutionVertex.remove(executionVertexId);
        if (priorAllocation != null) {
            reservedAllocationRefCounters.compute(
                    priorAllocation, (ignored, oldCount) -> oldCount > 1 ? oldCount - 1 : null);
        }
    }

    private void notifyCoordinatorOfCancellation(Execution execution) {
        // this method makes a best effort to filter out duplicate notifications, meaning cases
        // where the coordinator was already notified for that specific task
        // we don't notify if the task is already FAILED, CANCELLING, or CANCELED
        final ExecutionState currentState = execution.getState();
        if (currentState == ExecutionState.FAILED
                || currentState == ExecutionState.CANCELING
                || currentState == ExecutionState.CANCELED) {
            return;
        }

        notifyCoordinatorsAboutTaskFailure(execution, null);
    }

    private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

        @Override
        public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getResourceProfile();
        }

        @Override
        public Optional<AllocationID> findPriorAllocationId(
                final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).findLastAllocation();
        }

        @Override
        public SchedulingTopology getSchedulingTopology() {
            return DefaultScheduler.this.getSchedulingTopology();
        }

        @Override
        public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
            return getJobGraph().getSlotSharingGroups();
        }

        @Override
        public Set<CoLocationGroup> getCoLocationGroups() {
            return getJobGraph().getCoLocationGroups();
        }

        @Override
        public Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getConsumedPartitionGroups(executionVertexId);
        }

        @Override
        public Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
                ConsumedPartitionGroup consumedPartitionGroup) {
            return inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                    consumedPartitionGroup);
        }

        @Override
        public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
        }

        @Override
        public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
            return stateLocationRetriever.getStateLocation(executionVertexId);
        }

        @Override
        public Set<AllocationID> getReservedAllocations() {
            return reservedAllocationRefCounters.keySet();
        }
    }
}
