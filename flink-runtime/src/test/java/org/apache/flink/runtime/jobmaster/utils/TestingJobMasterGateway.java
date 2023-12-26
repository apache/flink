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

package org.apache.flink.runtime.jobmaster.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.TaskManagerRegistrationInformation;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;

/** {@link JobMasterGateway} implementation for testing purposes. */
public class TestingJobMasterGateway implements JobMasterGateway {

    @Nonnull private final String address;

    @Nonnull private final String hostname;

    @Nonnull private final Supplier<CompletableFuture<Acknowledge>> cancelFunction;

    @Nonnull
    private final Function<TaskExecutionState, CompletableFuture<Acknowledge>>
            updateTaskExecutionStateFunction;

    @Nonnull
    private final BiFunction<
                    JobVertexID, ExecutionAttemptID, CompletableFuture<SerializedInputSplit>>
            requestNextInputSplitFunction;

    @Nonnull
    private final BiFunction<
                    IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>>
            requestPartitionStateFunction;

    @Nonnull
    private final Function<ResourceID, CompletableFuture<Acknowledge>>
            disconnectTaskManagerFunction;

    @Nonnull private final Consumer<ResourceManagerId> disconnectResourceManagerConsumer;

    @Nonnull
    private final BiFunction<
                    ResourceID, Collection<SlotOffer>, CompletableFuture<Collection<SlotOffer>>>
            offerSlotsFunction;

    @Nonnull private final TriConsumer<ResourceID, AllocationID, Throwable> failSlotConsumer;

    @Nonnull
    private final BiFunction<
                    JobID,
                    TaskManagerRegistrationInformation,
                    CompletableFuture<RegistrationResponse>>
            registerTaskManagerFunction;

    @Nonnull
    private final BiFunction<
                    ResourceID, TaskExecutorToJobManagerHeartbeatPayload, CompletableFuture<Void>>
            taskManagerHeartbeatFunction;

    @Nonnull
    private final Function<ResourceID, CompletableFuture<Void>> resourceManagerHeartbeatFunction;

    @Nonnull private final Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier;

    @Nonnull private final Supplier<CompletableFuture<ExecutionGraphInfo>> requestJobSupplier;

    @Nonnull
    private final Supplier<CompletableFuture<CheckpointStatsSnapshot>>
            checkpointStatsSnapshotSupplier;

    @Nonnull
    private final TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
            triggerSavepointFunction;

    @Nonnull
    private final Function<CheckpointType, CompletableFuture<CompletedCheckpoint>>
            triggerCheckpointFunction;

    @Nonnull
    private final TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
            stopWithSavepointFunction;

    @Nonnull
    private final Consumer<
                    Tuple5<JobID, ExecutionAttemptID, Long, CheckpointMetrics, TaskStateSnapshot>>
            acknowledgeCheckpointConsumer;

    @Nonnull private final Consumer<DeclineCheckpoint> declineCheckpointConsumer;

    @Nonnull private final Supplier<JobMasterId> fencingTokenSupplier;

    @Nonnull
    private final BiFunction<JobID, String, CompletableFuture<KvStateLocation>>
            requestKvStateLocationFunction;

    @Nonnull
    private final Function<
                    Tuple6<JobID, JobVertexID, KeyGroupRange, String, KvStateID, InetSocketAddress>,
                    CompletableFuture<Acknowledge>>
            notifyKvStateRegisteredFunction;

    @Nonnull
    private final Function<
                    Tuple4<JobID, JobVertexID, KeyGroupRange, String>,
                    CompletableFuture<Acknowledge>>
            notifyKvStateUnregisteredFunction;

    @Nonnull TriFunction<String, Object, byte[], CompletableFuture<Object>> updateAggregateFunction;

    @Nonnull
    private final TriFunction<
                    ExecutionAttemptID,
                    OperatorID,
                    SerializedValue<OperatorEvent>,
                    CompletableFuture<Acknowledge>>
            operatorEventSender;

    @Nonnull
    private final BiFunction<
                    OperatorID,
                    SerializedValue<CoordinationRequest>,
                    CompletableFuture<CoordinationResponse>>
            deliverCoordinationRequestFunction;

    private final Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer;

    private final Function<Collection<BlockedNode>, CompletableFuture<Acknowledge>>
            notifyNewBlockedNodesFunction;

    private final Supplier<CompletableFuture<JobResourceRequirements>>
            requestJobResourceRequirementsSupplier;
    private final Function<JobResourceRequirements, CompletableFuture<Acknowledge>>
            updateJobResourceRequirementsFunction;

    public TestingJobMasterGateway(
            @Nonnull String address,
            @Nonnull String hostname,
            @Nonnull Supplier<CompletableFuture<Acknowledge>> cancelFunction,
            @Nonnull
                    Function<TaskExecutionState, CompletableFuture<Acknowledge>>
                            updateTaskExecutionStateFunction,
            @Nonnull
                    BiFunction<
                                    JobVertexID,
                                    ExecutionAttemptID,
                                    CompletableFuture<SerializedInputSplit>>
                            requestNextInputSplitFunction,
            @Nonnull
                    BiFunction<
                                    IntermediateDataSetID,
                                    ResultPartitionID,
                                    CompletableFuture<ExecutionState>>
                            requestPartitionStateFunction,
            @Nonnull
                    Function<ResourceID, CompletableFuture<Acknowledge>>
                            disconnectTaskManagerFunction,
            @Nonnull Consumer<ResourceManagerId> disconnectResourceManagerConsumer,
            @Nonnull
                    BiFunction<
                                    ResourceID,
                                    Collection<SlotOffer>,
                                    CompletableFuture<Collection<SlotOffer>>>
                            offerSlotsFunction,
            @Nonnull TriConsumer<ResourceID, AllocationID, Throwable> failSlotConsumer,
            @Nonnull
                    BiFunction<
                                    JobID,
                                    TaskManagerRegistrationInformation,
                                    CompletableFuture<RegistrationResponse>>
                            registerTaskManagerFunction,
            @Nonnull
                    BiFunction<
                                    ResourceID,
                                    TaskExecutorToJobManagerHeartbeatPayload,
                                    CompletableFuture<Void>>
                            taskManagerHeartbeatFunction,
            @Nonnull Function<ResourceID, CompletableFuture<Void>> resourceManagerHeartbeatFunction,
            @Nonnull Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier,
            @Nonnull Supplier<CompletableFuture<ExecutionGraphInfo>> requestJobSupplier,
            @Nonnull
                    Supplier<CompletableFuture<CheckpointStatsSnapshot>>
                            checkpointStatsSnapshotSupplier,
            @Nonnull
                    TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
                            triggerSavepointFunction,
            @Nonnull
                    Function<CheckpointType, CompletableFuture<CompletedCheckpoint>>
                            triggerCheckpointFunction,
            @Nonnull
                    TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
                            stopWithSavepointFunction,
            @Nonnull
                    Consumer<
                                    Tuple5<
                                            JobID,
                                            ExecutionAttemptID,
                                            Long,
                                            CheckpointMetrics,
                                            TaskStateSnapshot>>
                            acknowledgeCheckpointConsumer,
            @Nonnull Consumer<DeclineCheckpoint> declineCheckpointConsumer,
            @Nonnull Supplier<JobMasterId> fencingTokenSupplier,
            @Nonnull
                    BiFunction<JobID, String, CompletableFuture<KvStateLocation>>
                            requestKvStateLocationFunction,
            @Nonnull
                    Function<
                                    Tuple6<
                                            JobID,
                                            JobVertexID,
                                            KeyGroupRange,
                                            String,
                                            KvStateID,
                                            InetSocketAddress>,
                                    CompletableFuture<Acknowledge>>
                            notifyKvStateRegisteredFunction,
            @Nonnull
                    Function<
                                    Tuple4<JobID, JobVertexID, KeyGroupRange, String>,
                                    CompletableFuture<Acknowledge>>
                            notifyKvStateUnregisteredFunction,
            @Nonnull
                    TriFunction<String, Object, byte[], CompletableFuture<Object>>
                            updateAggregateFunction,
            @Nonnull
                    TriFunction<
                                    ExecutionAttemptID,
                                    OperatorID,
                                    SerializedValue<OperatorEvent>,
                                    CompletableFuture<Acknowledge>>
                            operatorEventSender,
            @Nonnull
                    BiFunction<
                                    OperatorID,
                                    SerializedValue<CoordinationRequest>,
                                    CompletableFuture<CoordinationResponse>>
                            deliverCoordinationRequestFunction,
            @Nonnull Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer,
            @Nonnull
                    Function<Collection<BlockedNode>, CompletableFuture<Acknowledge>>
                            notifyNewBlockedNodesFunction,
            @Nonnull
                    Supplier<CompletableFuture<JobResourceRequirements>>
                            requestJobResourceRequirementsSupplier,
            @Nonnull
                    Function<JobResourceRequirements, CompletableFuture<Acknowledge>>
                            updateJobResourceRequirementsFunction) {
        this.address = address;
        this.hostname = hostname;
        this.cancelFunction = cancelFunction;
        this.updateTaskExecutionStateFunction = updateTaskExecutionStateFunction;
        this.requestNextInputSplitFunction = requestNextInputSplitFunction;
        this.requestPartitionStateFunction = requestPartitionStateFunction;
        this.disconnectTaskManagerFunction = disconnectTaskManagerFunction;
        this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
        this.offerSlotsFunction = offerSlotsFunction;
        this.failSlotConsumer = failSlotConsumer;
        this.registerTaskManagerFunction = registerTaskManagerFunction;
        this.taskManagerHeartbeatFunction = taskManagerHeartbeatFunction;
        this.resourceManagerHeartbeatFunction = resourceManagerHeartbeatFunction;
        this.requestJobDetailsSupplier = requestJobDetailsSupplier;
        this.requestJobSupplier = requestJobSupplier;
        this.checkpointStatsSnapshotSupplier = checkpointStatsSnapshotSupplier;
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.triggerCheckpointFunction = triggerCheckpointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        this.acknowledgeCheckpointConsumer = acknowledgeCheckpointConsumer;
        this.declineCheckpointConsumer = declineCheckpointConsumer;
        this.fencingTokenSupplier = fencingTokenSupplier;
        this.requestKvStateLocationFunction = requestKvStateLocationFunction;
        this.notifyKvStateRegisteredFunction = notifyKvStateRegisteredFunction;
        this.notifyKvStateUnregisteredFunction = notifyKvStateUnregisteredFunction;
        this.updateAggregateFunction = updateAggregateFunction;
        this.operatorEventSender = operatorEventSender;
        this.deliverCoordinationRequestFunction = deliverCoordinationRequestFunction;
        this.notifyNotEnoughResourcesConsumer = notifyNotEnoughResourcesConsumer;
        this.notifyNewBlockedNodesFunction = notifyNewBlockedNodesFunction;
        this.requestJobResourceRequirementsSupplier = requestJobResourceRequirementsSupplier;
        this.updateJobResourceRequirementsFunction = updateJobResourceRequirementsFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        return cancelFunction.get();
    }

    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            TaskExecutionState taskExecutionState) {
        return updateTaskExecutionStateFunction.apply(taskExecutionState);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) {
        return requestNextInputSplitFunction.apply(vertexID, executionAttempt);
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID partitionId) {
        return requestPartitionStateFunction.apply(intermediateResultId, partitionId);
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            ResourceID resourceID, Exception cause) {
        return disconnectTaskManagerFunction.apply(resourceID);
    }

    @Override
    public void disconnectResourceManager(ResourceManagerId resourceManagerId, Exception cause) {
        disconnectResourceManagerConsumer.accept(resourceManagerId);
    }

    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            ResourceID taskManagerId, Collection<SlotOffer> slots, Time timeout) {
        return offerSlotsFunction.apply(taskManagerId, slots);
    }

    @Override
    public void failSlot(ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
        failSlotConsumer.accept(taskManagerId, allocationId, cause);
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            JobID jobId,
            TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            Time timeout) {
        return registerTaskManagerFunction.apply(jobId, taskManagerRegistrationInformation);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatFunction.apply(resourceID, payload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(ResourceID resourceID) {
        return resourceManagerHeartbeatFunction.apply(resourceID);
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJobDetailsSupplier.get();
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJobDetailsSupplier.get().thenApply(JobDetails::getStatus);
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        return requestJobSupplier.get();
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(Time timeout) {
        return checkpointStatsSnapshotSupplier.get();
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Time timeout) {
        return triggerSavepointFunction.apply(targetDirectory, cancelJob, formatType);
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            CheckpointType checkpointType, Time timeout) {
        return triggerCheckpointFunction.apply(checkpointType);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Time timeout) {
        return stopWithSavepointFunction.apply(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        notifyNotEnoughResourcesConsumer.accept(acquiredResources);
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            SerializedValue<TaskStateSnapshot> subtaskState) {
        acknowledgeCheckpointConsumer.accept(
                Tuple5.of(
                        jobID,
                        executionAttemptID,
                        checkpointId,
                        checkpointMetrics,
                        deserializeTaskStateSnapshot(subtaskState, getClass().getClassLoader())));
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {
        declineCheckpointConsumer.accept(declineCheckpoint);
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {}

    @Override
    public JobMasterId getFencingToken() {
        return fencingTokenSupplier.get();
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            JobID jobId, String registrationName) {
        return requestKvStateLocationFunction.apply(jobId, registrationName);
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress) {
        return notifyKvStateRegisteredFunction.apply(
                Tuple6.of(
                        jobId,
                        jobVertexId,
                        keyGroupRange,
                        registrationName,
                        kvStateId,
                        kvStateServerAddress));
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        return notifyKvStateUnregisteredFunction.apply(
                Tuple4.of(jobId, jobVertexId, keyGroupRange, registrationName));
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {
        return updateAggregateFunction.apply(aggregateName, aggregand, serializedAggregateFunction);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event) {
        return operatorEventSender.apply(task, operatorID, event);
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> request) {
        return deliverCoordinationRequestFunction.apply(operatorID, request);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return deliverCoordinationRequestFunction.apply(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        return notifyNewBlockedNodesFunction.apply(newNodes);
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements() {
        return requestJobResourceRequirementsSupplier.get();
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        return updateJobResourceRequirementsFunction.apply(jobResourceRequirements);
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttempt) {}
}
