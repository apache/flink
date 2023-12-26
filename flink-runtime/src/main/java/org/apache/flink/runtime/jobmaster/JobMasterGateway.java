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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.blocklist.BlocklistListener;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** {@link JobMaster} rpc gateway interface. */
public interface JobMasterGateway
        extends CheckpointCoordinatorGateway,
                FencedRpcGateway<JobMasterId>,
                KvStateLocationOracle,
                KvStateRegistryGateway,
                JobMasterOperatorEventGateway,
                BlocklistListener {

    /**
     * Cancels the currently executed job.
     *
     * @param timeout of this operation
     * @return Future acknowledge of the operation
     */
    CompletableFuture<Acknowledge> cancel(@RpcTimeout Time timeout);

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Future flag of the task execution state update result
     */
    CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState);

    /**
     * Requests the next input split for the {@link ExecutionJobVertex}. The next input split is
     * sent back to the sender as a {@link SerializedInputSplit} message.
     *
     * @param vertexID The job vertex id
     * @param executionAttempt The execution attempt id
     * @return The future of the input split. If there is no further input split, will return an
     *     empty object.
     */
    CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt);

    /**
     * Requests the current state of the partition. The state of a partition is currently bound to
     * the state of the producing execution.
     *
     * @param intermediateResultId The execution attempt ID of the task requesting the partition
     *     state.
     * @param partitionId The partition ID of the partition to request the state of.
     * @return The future of the partition state
     */
    CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId, final ResultPartitionID partitionId);

    /**
     * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
     * {@link JobMaster}.
     *
     * @param resourceID identifying the TaskManager to disconnect
     * @param cause for the disconnection of the TaskManager
     * @return Future acknowledge once the JobMaster has been disconnected from the TaskManager
     */
    CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause);

    /**
     * Disconnects the resource manager from the job manager because of the given cause.
     *
     * @param resourceManagerId identifying the resource manager leader id
     * @param cause of the disconnect
     */
    void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause);

    /**
     * Offers the given slots to the job manager. The response contains the set of accepted slots.
     *
     * @param taskManagerId identifying the task manager
     * @param slots to offer to the job manager
     * @param timeout for the rpc call
     * @return Future set of accepted slots.
     */
    CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId,
            final Collection<SlotOffer> slots,
            @RpcTimeout final Time timeout);

    /**
     * Fails the slot with the given allocation id and cause.
     *
     * @param taskManagerId identifying the task manager
     * @param allocationId identifying the slot to fail
     * @param cause of the failing
     */
    void failSlot(
            final ResourceID taskManagerId, final AllocationID allocationId, final Exception cause);

    /**
     * Registers the task manager at the job manager.
     *
     * @param jobId jobId specifying the job for which the JobMaster should be responsible
     * @param taskManagerRegistrationInformation the information for registering a task manager at
     *     the job manager
     * @param timeout for the rpc call
     * @return Future registration response indicating whether the registration was successful or
     *     not
     */
    CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            @RpcTimeout final Time timeout);

    /**
     * Sends the heartbeat to job manager from task manager.
     *
     * @param resourceID unique id of the task manager
     * @param payload report payload
     * @return future which is completed exceptionally if the operation fails
     */
    CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, final TaskExecutorToJobManagerHeartbeatPayload payload);

    /**
     * Sends heartbeat request from the resource manager.
     *
     * @param resourceID unique id of the resource manager
     * @return future which is completed exceptionally if the operation fails
     */
    CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID);

    /**
     * Request the details of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future details of the executed job
     */
    CompletableFuture<JobDetails> requestJobDetails(@RpcTimeout Time timeout);

    /**
     * Requests the current job status.
     *
     * @param timeout for the rpc call
     * @return Future containing the current job status
     */
    CompletableFuture<JobStatus> requestJobStatus(@RpcTimeout Time timeout);

    /**
     * Requests the {@link ExecutionGraphInfo} of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the {@link ExecutionGraphInfo} of the executed job
     */
    CompletableFuture<ExecutionGraphInfo> requestJob(@RpcTimeout Time timeout);

    /**
     * Requests the {@link CheckpointStatsSnapshot} of the job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the {@link CheckpointStatsSnapshot} of the job
     */
    CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(@RpcTimeout Time timeout);

    /**
     * Triggers taking a savepoint of the executed job.
     *
     * @param targetDirectory to which to write the savepoint data or null if the default savepoint
     *     directory should be used
     * @param formatType binary format for the savepoint
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint path once completed
     */
    CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            @RpcTimeout final Time timeout);

    /**
     * Triggers taking a checkpoint of the executed job.
     *
     * @param checkpointType to determine how checkpoint should be taken
     * @param timeout for the rpc call
     * @return Future which is completed with the CompletedCheckpoint once completed
     */
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, @RpcTimeout final Time timeout);

    /**
     * Triggers taking a checkpoint of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the checkpoint path once completed
     */
    default CompletableFuture<String> triggerCheckpoint(@RpcTimeout final Time timeout) {
        return triggerCheckpoint(CheckpointType.DEFAULT, timeout)
                .thenApply(CompletedCheckpoint::getExternalPointer);
    }

    /**
     * Stops the job with a savepoint.
     *
     * @param targetDirectory to which to write the savepoint data or null if the default savepoint
     *     directory should be used
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint path once completed
     */
    CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            @RpcTimeout final Time timeout);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements of a
     * job.
     *
     * @param acquiredResources the resources that have been acquired for the job
     */
    void notifyNotEnoughResourcesAvailable(Collection<ResourceRequirement> acquiredResources);

    /**
     * Update the aggregate and return the new value.
     *
     * @param aggregateName The name of the aggregate to update
     * @param aggregand The value to add to the aggregate
     * @param serializedAggregationFunction The function to apply to the current aggregate and
     *     aggregand to obtain the new aggregate value, this should be of type {@link
     *     AggregateFunction}
     * @return The updated aggregate
     */
    CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregationFunction);

    /**
     * Deliver a coordination request to a specified coordinator and return the response.
     *
     * @param operatorId identifying the coordinator to receive the request
     * @param serializedRequest serialized request to deliver
     * @return A future containing the response. The response will fail with a {@link
     *     org.apache.flink.util.FlinkException} if the task is not running, or no
     *     operator/coordinator exists for the given ID, or the coordinator cannot handle client
     *     events.
     */
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            @RpcTimeout Time timeout);

    /**
     * Notifies the {@link org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker}
     * to stop tracking the target result partitions and release the locally occupied resources on
     * {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}s if any.
     */
    CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds);

    /**
     * Read current {@link JobResourceRequirements job resource requirements}.
     *
     * @return Future which that contains current resource requirements.
     */
    CompletableFuture<JobResourceRequirements> requestJobResourceRequirements();

    /**
     * Update {@link JobResourceRequirements job resource requirements}.
     *
     * @param jobResourceRequirements new resource requirements
     * @return Future which is completed successfully when requirements are updated
     */
    CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements);

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttempt The execution attempt id.
     */
    void notifyEndOfData(final ExecutionAttemptID executionAttempt);
}
