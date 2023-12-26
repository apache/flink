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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The ExecutionVertex which supports speculative execution. */
public class SpeculativeExecutionVertex extends ExecutionVertex {

    private final Map<Integer, Execution> currentExecutions;

    private int originalAttemptNumber;

    final Map<Integer, Integer> nextInputSplitIndexToConsumeByAttempts;

    public SpeculativeExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            Time timeout,
            long createTimestamp,
            int executionHistorySizeLimit,
            int initialAttemptCount) {
        super(
                jobVertex,
                subTaskIndex,
                producedDataSets,
                timeout,
                createTimestamp,
                executionHistorySizeLimit,
                initialAttemptCount);

        this.currentExecutions = new LinkedHashMap<>();
        this.currentExecutions.put(currentExecution.getAttemptNumber(), currentExecution);
        this.originalAttemptNumber = currentExecution.getAttemptNumber();
        this.nextInputSplitIndexToConsumeByAttempts = new HashMap<>();
    }

    public boolean isSupportsConcurrentExecutionAttempts() {
        return getJobVertex().getJobVertex().isSupportsConcurrentExecutionAttempts();
    }

    public Execution createNewSpeculativeExecution(final long timestamp) {
        final Execution newExecution = createNewExecution(timestamp);
        getExecutionGraphAccessor().registerExecution(newExecution);
        currentExecutions.put(newExecution.getAttemptNumber(), newExecution);
        return newExecution;
    }

    /**
     * Returns whether the given attempt is the original execution attempt of the execution vertex,
     * i.e. it is created along with the creation of resetting of the execution vertex.
     */
    public boolean isOriginalAttempt(int attemptNumber) {
        return attemptNumber == originalAttemptNumber;
    }

    @Override
    public Collection<Execution> getCurrentExecutions() {
        return Collections.unmodifiableCollection(currentExecutions.values());
    }

    @Override
    public Execution getCurrentExecution(int attemptNumber) {
        return checkNotNull(currentExecutions.get(attemptNumber));
    }

    @Override
    public Execution getPartitionProducer() {
        return getCurrentExecutionAttempt();
    }

    @Override
    public CompletableFuture<?> cancel() {
        final List<CompletableFuture<?>> cancelResultFutures =
                new ArrayList<>(currentExecutions.size());
        for (Execution execution : currentExecutions.values()) {
            execution.cancel();
            cancelResultFutures.add(execution.getReleaseFuture());
        }
        return FutureUtils.combineAll(cancelResultFutures);
    }

    @Override
    public CompletableFuture<?> suspend() {
        return FutureUtils.combineAll(
                currentExecutions.values().stream()
                        .map(Execution::suspend)
                        .collect(Collectors.toList()));
    }

    @Override
    public void fail(Throwable t) {
        currentExecutions.values().forEach(e -> e.fail(t));
    }

    @Override
    public void markFailed(Throwable t) {
        currentExecutions.values().forEach(e -> e.markFailed(t));
    }

    @Override
    CompletableFuture<?> getTerminationFuture() {
        final List<CompletableFuture<?>> terminationFutures =
                currentExecutions.values().stream()
                        .map(Execution::getTerminalStateFuture)
                        .collect(Collectors.toList());
        return FutureUtils.waitForAll(terminationFutures);
    }

    @Override
    public void resetForNewExecution() {
        super.resetForNewExecution();

        currentExecutions.clear();
        currentExecutions.put(currentExecution.getAttemptNumber(), currentExecution);
        originalAttemptNumber = currentExecution.getAttemptNumber();
        nextInputSplitIndexToConsumeByAttempts.clear();
    }

    @Override
    void resetExecutionsInternal() {
        for (Execution execution : currentExecutions.values()) {
            resetExecution(execution);
        }
    }

    /**
     * Remove execution from currentExecutions if it is failed. It is needed to make room for
     * possible future speculative executions.
     *
     * @param executionAttemptId attemptID of the execution to be removed
     */
    public void archiveFailedExecution(ExecutionAttemptID executionAttemptId) {
        if (this.currentExecutions.size() <= 1) {
            // Leave the last execution because currentExecutions should never be empty. This should
            // happen only if all current executions have FAILED. A vertex reset will happen soon
            // and will archive the remaining execution.
            return;
        }

        final Execution removedExecution =
                this.currentExecutions.remove(executionAttemptId.getAttemptNumber());
        nextInputSplitIndexToConsumeByAttempts.remove(executionAttemptId.getAttemptNumber());
        checkNotNull(
                removedExecution,
                "Cannot remove execution %s which does not exist.",
                executionAttemptId);
        checkState(
                removedExecution.getState() == FAILED,
                "Cannot remove execution %s which is not FAILED.",
                executionAttemptId);

        executionHistory.add(removedExecution.archive());
        if (removedExecution == this.currentExecution) {
            this.currentExecution = this.currentExecutions.values().iterator().next();
        }
    }

    @Override
    public Execution getCurrentExecutionAttempt() {
        // returns the execution which is most likely to reach FINISHED state
        Execution currentExecution = this.currentExecution;
        for (Execution execution : currentExecutions.values()) {
            if (getStatePriority(execution.getState())
                    < getStatePriority(currentExecution.getState())) {
                currentExecution = execution;
            }
        }
        return currentExecution;
    }

    private int getStatePriority(ExecutionState state) {
        // the more likely to reach FINISHED state, the higher priority, the smaller value
        switch (state) {
                // CREATED/SCHEDULED/INITIALIZING/RUNNING/FINISHED are healthy states with an
                // increasing priority
            case FINISHED:
                return 0;
            case RUNNING:
                return 1;
            case INITIALIZING:
                return 2;
            case DEPLOYING:
                return 3;
            case SCHEDULED:
                return 4;
            case CREATED:
                return 5;
                // if the vertex is not in a healthy state, shows its CANCELING state unless it is
                // fully FAILED or CANCELED
            case CANCELING:
                return 6;
            case FAILED:
                return 7;
            case CANCELED:
                return 8;
            default:
                throw new IllegalStateException("Execution state " + state + " is not supported.");
        }
    }

    @Override
    public Optional<InputSplit> getNextInputSplit(String host, int attemptNumber) {
        final int index = nextInputSplitIndexToConsumeByAttempts.getOrDefault(attemptNumber, 0);
        checkState(index <= inputSplits.size());

        if (index < inputSplits.size()) {
            nextInputSplitIndexToConsumeByAttempts.put(attemptNumber, index + 1);
            return Optional.of(inputSplits.get(index));
        } else {
            final Optional<InputSplit> split = super.getNextInputSplit(host, attemptNumber);
            if (split.isPresent()) {
                nextInputSplitIndexToConsumeByAttempts.put(attemptNumber, index + 1);
            }
            return split;
        }
    }

    @Override
    void notifyPendingDeployment(Execution execution) {
        getExecutionGraphAccessor()
                .getExecutionDeploymentListener()
                .onStartedDeployment(
                        execution.getAttemptId(),
                        execution.getAssignedResourceLocation().getResourceID());
    }

    @Override
    void notifyCompletedDeployment(Execution execution) {
        getExecutionGraphAccessor()
                .getExecutionDeploymentListener()
                .onCompletedDeployment(execution.getAttemptId());
    }

    @Override
    void notifyStateTransition(
            Execution execution, ExecutionState previousState, ExecutionState newState) {
        getExecutionGraphAccessor().notifyExecutionChange(execution, previousState, newState);
    }

    @Override
    public ArchivedExecutionVertex archive() {
        return new ArchivedExecutionVertex(this);
    }

    @Override
    void cachePartitionInfo(PartitionInfo partitionInfo) {
        getCurrentExecutionAttempt().cachePartitionInfo(partitionInfo);
    }

    @Override
    public void tryAssignResource(LogicalSlot slot) {
        throw new UnsupportedOperationException(
                "Method is not supported in SpeculativeExecutionVertex.");
    }

    @Override
    public void deploy() {
        throw new UnsupportedOperationException(
                "Method is not supported in SpeculativeExecutionVertex.");
    }

    @Override
    public void deployToSlot(LogicalSlot slot) {
        throw new UnsupportedOperationException(
                "Method is not supported in SpeculativeExecutionVertex.");
    }

    @Override
    public Optional<TaskManagerLocation> getPreferredLocationBasedOnState() {
        throw new UnsupportedOperationException(
                "Method is not supported in SpeculativeExecutionVertex.");
    }
}
