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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;

/** Handler for the {@link ExecutionGraph} which offers some common operations. */
public class ExecutionGraphHandler {

    private final ExecutionGraph executionGraph;

    private final Logger log;

    private final Executor ioExecutor;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    public ExecutionGraphHandler(
            ExecutionGraph executionGraph,
            Logger log,
            Executor ioExecutor,
            ComponentMainThreadExecutor mainThreadExecutor) {
        this.executionGraph = executionGraph;
        this.log = log;
        this.ioExecutor = ioExecutor;
        this.mainThreadExecutor = mainThreadExecutor;
    }

    public void reportCheckpointMetrics(
            ExecutionAttemptID attemptId, long id, CheckpointMetrics metrics) {
        processCheckpointCoordinatorMessage(
                "ReportCheckpointStats",
                coordinator -> coordinator.reportStats(id, attemptId, metrics));
    }

    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            final TaskStateSnapshot checkpointState) {
        processCheckpointCoordinatorMessage(
                "AcknowledgeCheckpoint",
                coordinator ->
                        coordinator.receiveAcknowledgeMessage(
                                new AcknowledgeCheckpoint(
                                        jobID,
                                        executionAttemptID,
                                        checkpointId,
                                        checkpointMetrics,
                                        checkpointState),
                                retrieveTaskManagerLocation(executionAttemptID)));
    }

    public void declineCheckpoint(final DeclineCheckpoint decline) {
        processCheckpointCoordinatorMessage(
                "DeclineCheckpoint",
                coordinator ->
                        coordinator.receiveDeclineMessage(
                                decline,
                                retrieveTaskManagerLocation(decline.getTaskExecutionId())));
    }

    private void processCheckpointCoordinatorMessage(
            String messageType, ThrowingConsumer<CheckpointCoordinator, Exception> process) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            ioExecutor.execute(
                    () -> {
                        try {
                            process.accept(checkpointCoordinator);
                        } catch (Exception t) {
                            log.warn("Error while processing " + messageType + " message", t);
                        }
                    });
        } else {
            String errorMessage =
                    "Received " + messageType + " message for job {} with no CheckpointCoordinator";
            if (executionGraph.getState() == JobStatus.RUNNING) {
                log.error(errorMessage, executionGraph.getJobID());
            } else {
                log.debug(errorMessage, executionGraph.getJobID());
            }
        }
    }

    private String retrieveTaskManagerLocation(ExecutionAttemptID executionAttemptID) {
        final Optional<Execution> currentExecution =
                Optional.ofNullable(
                        executionGraph.getRegisteredExecutions().get(executionAttemptID));

        return currentExecution
                .map(Execution::getAssignedResourceLocation)
                .map(TaskManagerLocation::toString)
                .orElse("Unknown location");
    }

    public ExecutionState requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {

        final Execution execution =
                executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
        if (execution != null) {
            return execution.getState();
        } else {
            final IntermediateResult intermediateResult =
                    executionGraph.getAllIntermediateResults().get(intermediateResultId);

            if (intermediateResult != null) {
                // Try to find the producing execution
                Execution producerExecution =
                        intermediateResult
                                .getPartitionById(resultPartitionId.getPartitionId())
                                .getProducer()
                                .getCurrentExecutionAttempt();

                if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
                    return producerExecution.getState();
                } else {
                    throw new PartitionProducerDisposedException(resultPartitionId);
                }
            } else {
                throw new IllegalArgumentException(
                        "Intermediate data set with ID " + intermediateResultId + " not found.");
            }
        }
    }

    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {

        final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
        if (execution == null) {
            // can happen when JobManager had already unregistered this execution upon on task
            // failure,
            // but TaskManager get some delay to aware of that situation
            if (log.isDebugEnabled()) {
                log.debug("Can not find Execution for attempt {}.", executionAttempt);
            }
            // but we should TaskManager be aware of this
            throw new IllegalArgumentException(
                    "Can not find Execution for attempt " + executionAttempt);
        }

        final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
        if (vertex == null) {
            throw new IllegalArgumentException(
                    "Cannot find execution vertex for vertex ID " + vertexID);
        }

        if (vertex.getSplitAssigner() == null) {
            throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
        }

        final InputSplit nextInputSplit = execution.getNextInputSplit();

        if (log.isDebugEnabled()) {
            log.debug("Send next input split {}.", nextInputSplit);
        }

        try {
            final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
            return new SerializedInputSplit(serializedInputSplit);
        } catch (Exception ex) {
            IOException reason =
                    new IOException(
                            "Could not serialize the next input split of class "
                                    + nextInputSplit.getClass()
                                    + ".",
                            ex);
            vertex.fail(reason);
            throw reason;
        }
    }
}
