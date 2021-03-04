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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.KvStateHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Abstract state class which contains an {@link ExecutionGraph} and the required handlers to
 * execute common operations.
 */
abstract class StateWithExecutionGraph implements State {
    private final Context context;

    private final ExecutionGraph executionGraph;

    private final ExecutionGraphHandler executionGraphHandler;

    private final OperatorCoordinatorHandler operatorCoordinatorHandler;

    private final KvStateHandler kvStateHandler;

    private final Logger logger;

    StateWithExecutionGraph(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger) {
        this.context = context;
        this.executionGraph = executionGraph;
        this.executionGraphHandler = executionGraphHandler;
        this.operatorCoordinatorHandler = operatorCoordinatorHandler;
        this.kvStateHandler = new KvStateHandler(executionGraph);
        this.logger = logger;
        Preconditions.checkState(
                executionGraph.getState() == JobStatus.RUNNING, "Assuming running execution graph");

        FutureUtils.assertNoException(
                executionGraph
                        .getTerminationFuture()
                        .thenAcceptAsync(
                                jobStatus -> {
                                    if (jobStatus.isGloballyTerminalState()) {
                                        context.runIfState(
                                                this, () -> onGloballyTerminalState(jobStatus));
                                    }
                                },
                                context.getMainThreadExecutor()));
    }

    @VisibleForTesting
    ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    protected OperatorCoordinatorHandler getOperatorCoordinatorHandler() {
        return operatorCoordinatorHandler;
    }

    protected ExecutionGraphHandler getExecutionGraphHandler() {
        return executionGraphHandler;
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (!StateWithExecutionGraph.class.isAssignableFrom(newState)) {
            // we are leaving the StateWithExecutionGraph --> we need to dispose temporary services
            operatorCoordinatorHandler.disposeAllOperatorCoordinators();
        }
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return ArchivedExecutionGraph.createFrom(executionGraph, getJobStatus());
    }

    @Override
    public void suspend(Throwable cause) {
        executionGraph.suspend(cause);
        Preconditions.checkState(executionGraph.getState().isTerminalState());
        context.goToFinished(ArchivedExecutionGraph.createFrom(executionGraph));
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    void notifyPartitionDataAvailable(ResultPartitionID partitionID) {
        executionGraph.notifyPartitionDataAvailable(partitionID);
    }

    SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        return executionGraphHandler.requestNextInputSplit(vertexID, executionAttempt);
    }

    ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        return executionGraphHandler.requestPartitionState(intermediateResultId, resultPartitionId);
    }

    void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {

        executionGraphHandler.acknowledgeCheckpoint(
                jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
    }

    void declineCheckpoint(DeclineCheckpoint decline) {
        executionGraphHandler.declineCheckpoint(decline);
    }

    void reportCheckpointMetrics(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {
        executionGraphHandler.reportCheckpointMetrics(
                executionAttemptID, checkpointId, checkpointMetrics);
    }

    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        executionGraph.updateAccumulators(accumulatorSnapshot);
    }

    KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws FlinkJobNotFoundException, UnknownKvStateLocation {
        return kvStateHandler.requestKvStateLocation(jobId, registrationName);
    }

    void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        kvStateHandler.notifyKvStateRegistered(
                jobId,
                jobVertexId,
                keyGroupRange,
                registrationName,
                kvStateId,
                kvStateServerAddress);
    }

    void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {
        kvStateHandler.notifyKvStateUnregistered(
                jobId, jobVertexId, keyGroupRange, registrationName);
    }

    CompletableFuture<String> triggerSavepoint(String targetDirectory, boolean cancelJob) {
        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(
                    String.format("Job %s is not a streaming job.", executionGraph.getJobID()));
        } else if (targetDirectory == null
                && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
            logger.info(
                    "Trying to cancel job {} with savepoint, but no savepoint directory configured.",
                    executionGraph.getJobID());

            throw new IllegalStateException(
                    "No savepoint directory configured. You can either specify a directory "
                            + "while cancelling via -s :targetDirectory or configure a cluster-wide "
                            + "default via key '"
                            + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                            + "'.");
        }

        logger.info(
                "Triggering {}savepoint for job {}.",
                cancelJob ? "cancel-with-" : "",
                executionGraph.getJobID());

        if (cancelJob) {
            checkpointCoordinator.stopCheckpointScheduler();
        }

        return checkpointCoordinator
                .triggerSavepoint(targetDirectory)
                .thenApply(CompletedCheckpoint::getExternalPointer)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                if (cancelJob && context.isState(this)) {
                                    startCheckpointScheduler(checkpointCoordinator);
                                }
                                throw new CompletionException(throwable);
                            } else if (cancelJob && context.isState(this)) {
                                logger.info(
                                        "Savepoint stored in {}. Now cancelling {}.",
                                        path,
                                        executionGraph.getJobID());
                                cancel();
                            }
                            return path;
                        },
                        context.getMainThreadExecutor());
    }

    CompletableFuture<String> stopWithSavepoint(String targetDirectory, boolean terminate) {
        throw new UnsupportedOperationException(
                "This will be implemented as part of https://issues.apache.org/jira/browse/FLINK-21333");
    }

    private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
        if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
            try {
                checkpointCoordinator.startCheckpointScheduler();
            } catch (IllegalStateException ignored) {
                // Concurrent shut down of the coordinator
            }
        }
    }

    void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecutionId, OperatorID operatorId, OperatorEvent evt)
            throws FlinkException {
        operatorCoordinatorHandler.deliverOperatorEventToCoordinator(
                taskExecutionId, operatorId, evt);
    }

    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId, CoordinationRequest request) throws FlinkException {
        return operatorCoordinatorHandler.deliverCoordinationRequestToCoordinator(
                operatorId, request);
    }

    /**
     * Updates the execution graph with the given task execution state transition.
     *
     * @param taskExecutionStateTransition taskExecutionStateTransition to update the ExecutionGraph
     *     with
     * @return {@code true} if the update was successful; otherwise {@code false}
     */
    abstract boolean updateTaskExecutionState(
            TaskExecutionStateTransition taskExecutionStateTransition);

    /**
     * Callback which is called once the execution graph reaches a globally terminal state.
     *
     * @param globallyTerminalState globally terminal state which the execution graph reached
     */
    abstract void onGloballyTerminalState(JobStatus globallyTerminalState);

    /** Context of the {@link StateWithExecutionGraph} state. */
    interface Context {

        /**
         * Run the given action if the current state equals the expected state.
         *
         * @param expectedState expectedState is the expected state
         * @param action action to run if the current state equals the expected state
         */
        void runIfState(State expectedState, Runnable action);

        /**
         * Checks whether the current state is the expected state.
         *
         * @param expectedState expectedState is the expected state
         * @return {@code true} if the current state equals the expected state; otherwise {@code
         *     false}
         */
        boolean isState(State expectedState);

        /**
         * Gets the main thread executor.
         *
         * @return the main thread executor
         */
        Executor getMainThreadExecutor();

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph which is passed to the {@link
         *     Finished} state
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);
    }
}
