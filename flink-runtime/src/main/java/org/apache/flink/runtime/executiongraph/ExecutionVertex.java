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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several
 * times, each of which time it spawns an {@link Execution}.
 */
public class ExecutionVertex
        implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

    private static final Logger LOG = DefaultExecutionGraph.LOG;

    public static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

    // --------------------------------------------------------------------------------------------

    private final ExecutionJobVertex jobVertex;

    private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

    private final int subTaskIndex;

    private final ExecutionVertexID executionVertexId;

    private final EvictingBoundedList<ArchivedExecution> priorExecutions;

    private final Time timeout;

    /** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
    private final String taskNameWithSubtask;

    /** The current or latest execution attempt of this vertex's task. */
    private Execution currentExecution; // this field must never be null

    private final ArrayList<InputSplit> inputSplits;

    // --------------------------------------------------------------------------------------------

    /**
     * Creates an ExecutionVertex.
     *
     * @param timeout The RPC timeout to use for deploy / cancel calls
     * @param createTimestamp The timestamp for the vertex creation, used to initialize the first
     *     Execution with.
     * @param maxPriorExecutionHistoryLength The number of prior Executions (= execution attempts)
     *     to keep.
     * @param initialAttemptCount The attempt number of the first execution of this vertex.
     */
    @VisibleForTesting
    public ExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            Time timeout,
            long createTimestamp,
            int maxPriorExecutionHistoryLength,
            int initialAttemptCount) {

        this.jobVertex = jobVertex;
        this.subTaskIndex = subTaskIndex;
        this.executionVertexId = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
        this.taskNameWithSubtask =
                String.format(
                        "%s (%d/%d)",
                        jobVertex.getJobVertex().getName(),
                        subTaskIndex + 1,
                        jobVertex.getParallelism());

        this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

        for (IntermediateResult result : producedDataSets) {
            IntermediateResultPartition irp =
                    new IntermediateResultPartition(
                            result,
                            this,
                            subTaskIndex,
                            getExecutionGraphAccessor().getEdgeManager());
            result.setPartition(subTaskIndex, irp);

            resultPartitions.put(irp.getPartitionId(), irp);
        }

        this.priorExecutions = new EvictingBoundedList<>(maxPriorExecutionHistoryLength);

        this.currentExecution =
                new Execution(
                        getExecutionGraphAccessor().getFutureExecutor(),
                        this,
                        initialAttemptCount,
                        createTimestamp,
                        timeout);

        getExecutionGraphAccessor().registerExecution(currentExecution);

        this.timeout = timeout;
        this.inputSplits = new ArrayList<>();
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------

    public JobID getJobId() {
        return this.jobVertex.getJobId();
    }

    public ExecutionJobVertex getJobVertex() {
        return jobVertex;
    }

    public JobVertexID getJobvertexId() {
        return this.jobVertex.getJobVertexId();
    }

    public String getTaskName() {
        return this.jobVertex.getJobVertex().getName();
    }

    /**
     * Creates a simple name representation in the style 'taskname (x/y)', where 'taskname' is the
     * name as returned by {@link #getTaskName()}, 'x' is the parallel subtask index as returned by
     * {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total number of tasks, as
     * returned by {@link #getTotalNumberOfParallelSubtasks()}.
     *
     * @return A simple name representation in the form 'myTask (2/7)'
     */
    @Override
    public String getTaskNameWithSubtaskIndex() {
        return this.taskNameWithSubtask;
    }

    public int getTotalNumberOfParallelSubtasks() {
        return this.jobVertex.getParallelism();
    }

    public int getMaxParallelism() {
        return this.jobVertex.getMaxParallelism();
    }

    public ResourceProfile getResourceProfile() {
        return this.jobVertex.getResourceProfile();
    }

    @Override
    public int getParallelSubtaskIndex() {
        return this.subTaskIndex;
    }

    public ExecutionVertexID getID() {
        return executionVertexId;
    }

    public int getNumberOfInputs() {
        return getAllConsumedPartitionGroups().size();
    }

    public List<ConsumedPartitionGroup> getAllConsumedPartitionGroups() {
        return getExecutionGraphAccessor()
                .getEdgeManager()
                .getConsumedPartitionGroupsForVertex(executionVertexId);
    }

    public ConsumedPartitionGroup getConsumedPartitions(int input) {
        final List<ConsumedPartitionGroup> allConsumedPartitions = getAllConsumedPartitionGroups();

        if (input < 0 || input >= allConsumedPartitions.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Input %d is out of range [0..%d)",
                            input, allConsumedPartitions.size()));
        }

        return allConsumedPartitions.get(input);
    }

    public InputSplit getNextInputSplit(String host) {
        final int taskId = getParallelSubtaskIndex();
        synchronized (inputSplits) {
            final InputSplit nextInputSplit =
                    jobVertex.getSplitAssigner().getNextInputSplit(host, taskId);
            if (nextInputSplit != null) {
                inputSplits.add(nextInputSplit);
            }
            return nextInputSplit;
        }
    }

    @Override
    public Execution getCurrentExecutionAttempt() {
        return currentExecution;
    }

    @Override
    public ExecutionState getExecutionState() {
        return currentExecution.getState();
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return currentExecution.getStateTimestamp(state);
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return currentExecution.getFailureInfo();
    }

    public CompletableFuture<TaskManagerLocation> getCurrentTaskManagerLocationFuture() {
        return currentExecution.getTaskManagerLocationFuture();
    }

    public LogicalSlot getCurrentAssignedResource() {
        return currentExecution.getAssignedResource();
    }

    @Override
    public TaskManagerLocation getCurrentAssignedResourceLocation() {
        return currentExecution.getAssignedResourceLocation();
    }

    @Nullable
    @Override
    public ArchivedExecution getPriorExecutionAttempt(int attemptNumber) {
        synchronized (priorExecutions) {
            if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
                return priorExecutions.get(attemptNumber);
            } else {
                throw new IllegalArgumentException("attempt does not exist");
            }
        }
    }

    public ArchivedExecution getLatestPriorExecution() {
        synchronized (priorExecutions) {
            final int size = priorExecutions.size();
            if (size > 0) {
                return priorExecutions.get(size - 1);
            } else {
                return null;
            }
        }
    }

    /**
     * Gets the location where the latest completed/canceled/failed execution of the vertex's task
     * happened.
     *
     * @return The latest prior execution location, or null, if there is none, yet.
     */
    public TaskManagerLocation getLatestPriorLocation() {
        ArchivedExecution latestPriorExecution = getLatestPriorExecution();
        return latestPriorExecution != null
                ? latestPriorExecution.getAssignedResourceLocation()
                : null;
    }

    public AllocationID getLatestPriorAllocation() {
        ArchivedExecution latestPriorExecution = getLatestPriorExecution();
        return latestPriorExecution != null ? latestPriorExecution.getAssignedAllocationID() : null;
    }

    EvictingBoundedList<ArchivedExecution> getCopyOfPriorExecutionsList() {
        synchronized (priorExecutions) {
            return new EvictingBoundedList<>(priorExecutions);
        }
    }

    public final InternalExecutionGraphAccessor getExecutionGraphAccessor() {
        return this.jobVertex.getGraph();
    }

    public Map<IntermediateResultPartitionID, IntermediateResultPartition> getProducedPartitions() {
        return resultPartitions;
    }

    // --------------------------------------------------------------------------------------------
    //  Graph building
    // --------------------------------------------------------------------------------------------

    public void addConsumedPartitionGroup(ConsumedPartitionGroup consumedPartitions) {

        getExecutionGraphAccessor()
                .getEdgeManager()
                .connectVertexWithConsumedPartitionGroup(executionVertexId, consumedPartitions);
    }

    /**
     * Gets the preferred location to execute the current task execution attempt, based on the state
     * that the execution attempt will resume.
     */
    public Optional<TaskManagerLocation> getPreferredLocationBasedOnState() {
        if (currentExecution.getTaskRestore() != null) {
            return Optional.ofNullable(getLatestPriorLocation());
        }

        return Optional.empty();
    }

    // --------------------------------------------------------------------------------------------
    //   Actions
    // --------------------------------------------------------------------------------------------

    /** Archives the current Execution and creates a new Execution for this vertex. */
    public void resetForNewExecution() {
        resetForNewExecutionInternal(System.currentTimeMillis());
    }

    private void resetForNewExecutionInternal(final long timestamp) {
        final Execution oldExecution = currentExecution;
        final ExecutionState oldState = oldExecution.getState();

        if (oldState.isTerminal()) {
            if (oldState == FINISHED) {
                // pipelined partitions are released in Execution#cancel(), covering both job
                // failures and vertex resets
                // do not release pipelined partitions here to save RPC calls
                oldExecution.handlePartitionCleanup(false, true);
                getExecutionGraphAccessor()
                        .getPartitionReleaseStrategy()
                        .vertexUnfinished(executionVertexId);
            }

            priorExecutions.add(oldExecution.archive());

            final Execution newExecution =
                    new Execution(
                            getExecutionGraphAccessor().getFutureExecutor(),
                            this,
                            oldExecution.getAttemptNumber() + 1,
                            timestamp,
                            timeout);

            currentExecution = newExecution;

            synchronized (inputSplits) {
                InputSplitAssigner assigner = jobVertex.getSplitAssigner();
                if (assigner != null) {
                    assigner.returnInputSplit(inputSplits, getParallelSubtaskIndex());
                    inputSplits.clear();
                }
            }

            // register this execution at the execution graph, to receive call backs
            getExecutionGraphAccessor().registerExecution(newExecution);

            // if the execution was 'FINISHED' before, tell the ExecutionGraph that
            // we take one step back on the road to reaching global FINISHED
            if (oldState == FINISHED) {
                getExecutionGraphAccessor().vertexUnFinished();
            }

            // reset the intermediate results
            for (IntermediateResultPartition resultPartition : resultPartitions.values()) {
                resultPartition.resetForNewExecution();
            }
        } else {
            throw new IllegalStateException(
                    "Cannot reset a vertex that is in non-terminal state " + oldState);
        }
    }

    public void tryAssignResource(LogicalSlot slot) {
        if (!currentExecution.tryAssignResource(slot)) {
            throw new IllegalStateException(
                    "Could not assign resource "
                            + slot
                            + " to current execution "
                            + currentExecution
                            + '.');
        }
    }

    public void deploy() throws JobException {
        currentExecution.deploy();
    }

    @VisibleForTesting
    public void deployToSlot(LogicalSlot slot) throws JobException {
        if (currentExecution.tryAssignResource(slot)) {
            currentExecution.deploy();
        } else {
            throw new IllegalStateException(
                    "Could not assign resource "
                            + slot
                            + " to current execution "
                            + currentExecution
                            + '.');
        }
    }

    /**
     * Cancels this ExecutionVertex.
     *
     * @return A future that completes once the execution has reached its final state.
     */
    public CompletableFuture<?> cancel() {
        // to avoid any case of mixup in the presence of concurrent calls,
        // we copy a reference to the stack to make sure both calls go to the same Execution
        final Execution exec = currentExecution;
        exec.cancel();
        return exec.getReleaseFuture();
    }

    public CompletableFuture<?> suspend() {
        return currentExecution.suspend();
    }

    public void fail(Throwable t) {
        currentExecution.fail(t);
    }

    /**
     * This method marks the task as failed, but will make no attempt to remove task execution from
     * the task manager. It is intended for cases where the task is known not to be deployed yet.
     *
     * @param t The exception that caused the task to fail.
     */
    public void markFailed(Throwable t) {
        currentExecution.markFailed(t);
    }

    void notifyPartitionDataAvailable(ResultPartitionID partitionId) {
        checkArgument(partitionId.getProducerId().equals(currentExecution.getAttemptId()));

        final IntermediateResultPartition partition =
                resultPartitions.get(partitionId.getPartitionId());
        checkState(partition != null, "Unknown partition " + partitionId + ".");
        checkState(
                partition.getResultType().isPipelined(),
                "partition data available notification is "
                        + "only valid for pipelined partitions.");

        partition.markDataProduced();
    }

    void cachePartitionInfo(PartitionInfo partitionInfo) {
        getCurrentExecutionAttempt().cachePartitionInfo(partitionInfo);
    }

    /** Returns all blocking result partitions whose receivers can be scheduled/updated. */
    List<IntermediateResultPartition> finishAllBlockingPartitions() {
        List<IntermediateResultPartition> finishedBlockingPartitions = null;

        for (IntermediateResultPartition partition : resultPartitions.values()) {
            if (partition.getResultType().isBlocking() && partition.markFinished()) {
                if (finishedBlockingPartitions == null) {
                    finishedBlockingPartitions = new LinkedList<IntermediateResultPartition>();
                }

                finishedBlockingPartitions.add(partition);
            }
        }

        if (finishedBlockingPartitions == null) {
            return Collections.emptyList();
        } else {
            return finishedBlockingPartitions;
        }
    }

    // --------------------------------------------------------------------------------------------
    //   Notifications from the Execution Attempt
    // --------------------------------------------------------------------------------------------

    void executionFinished(Execution execution) {
        getExecutionGraphAccessor().vertexFinished();
    }

    // --------------------------------------------------------------------------------------------
    //   Miscellaneous
    // --------------------------------------------------------------------------------------------

    void notifyPendingDeployment(Execution execution) {
        // only forward this notification if the execution is still the current execution
        // otherwise we have an outdated execution
        if (isCurrentExecution(execution)) {
            getExecutionGraphAccessor()
                    .getExecutionDeploymentListener()
                    .onStartedDeployment(
                            execution.getAttemptId(),
                            execution.getAssignedResourceLocation().getResourceID());
        }
    }

    void notifyCompletedDeployment(Execution execution) {
        // only forward this notification if the execution is still the current execution
        // otherwise we have an outdated execution
        if (isCurrentExecution(execution)) {
            getExecutionGraphAccessor()
                    .getExecutionDeploymentListener()
                    .onCompletedDeployment(execution.getAttemptId());
        }
    }

    /** Simply forward this notification. */
    void notifyStateTransition(Execution execution, ExecutionState newState) {
        // only forward this notification if the execution is still the current execution
        // otherwise we have an outdated execution
        if (isCurrentExecution(execution)) {
            getExecutionGraphAccessor().notifyExecutionChange(execution, newState);
        }
    }

    private boolean isCurrentExecution(Execution execution) {
        return currentExecution == execution;
    }

    // --------------------------------------------------------------------------------------------
    //  Utilities
    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return getTaskNameWithSubtaskIndex();
    }

    @Override
    public ArchivedExecutionVertex archive() {
        return new ArchivedExecutionVertex(this);
    }
}
