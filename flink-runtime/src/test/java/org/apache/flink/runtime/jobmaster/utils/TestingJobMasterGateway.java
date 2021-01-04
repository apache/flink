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
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
    private final Function<ResultPartitionID, CompletableFuture<Acknowledge>>
            notifyPartitionDataAvailableFunction;

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
                    String, UnresolvedTaskManagerLocation, CompletableFuture<RegistrationResponse>>
            registerTaskManagerFunction;

    @Nonnull
    private final BiConsumer<ResourceID, TaskExecutorToJobManagerHeartbeatPayload>
            taskManagerHeartbeatConsumer;

    @Nonnull private final Consumer<ResourceID> resourceManagerHeartbeatConsumer;

    @Nonnull private final Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier;

    @Nonnull private final Supplier<CompletableFuture<ArchivedExecutionGraph>> requestJobSupplier;

    @Nonnull
    private final BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction;

    @Nonnull
    private final BiFunction<String, Boolean, CompletableFuture<String>> stopWithSavepointFunction;

    @Nonnull
    private final Function<JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
            requestOperatorBackPressureStatsFunction;

    @Nonnull private final BiConsumer<AllocationID, Throwable> notifyAllocationFailureConsumer;

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
                    Function<ResultPartitionID, CompletableFuture<Acknowledge>>
                            notifyPartitionDataAvailableFunction,
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
                                    String,
                                    UnresolvedTaskManagerLocation,
                                    CompletableFuture<RegistrationResponse>>
                            registerTaskManagerFunction,
            @Nonnull
                    BiConsumer<ResourceID, TaskExecutorToJobManagerHeartbeatPayload>
                            taskManagerHeartbeatConsumer,
            @Nonnull Consumer<ResourceID> resourceManagerHeartbeatConsumer,
            @Nonnull Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier,
            @Nonnull Supplier<CompletableFuture<ArchivedExecutionGraph>> requestJobSupplier,
            @Nonnull
                    BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction,
            @Nonnull
                    BiFunction<String, Boolean, CompletableFuture<String>>
                            stopWithSavepointFunction,
            @Nonnull
                    Function<JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
                            requestOperatorBackPressureStatsFunction,
            @Nonnull BiConsumer<AllocationID, Throwable> notifyAllocationFailureConsumer,
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
            @Nonnull Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer) {
        this.address = address;
        this.hostname = hostname;
        this.cancelFunction = cancelFunction;
        this.updateTaskExecutionStateFunction = updateTaskExecutionStateFunction;
        this.requestNextInputSplitFunction = requestNextInputSplitFunction;
        this.requestPartitionStateFunction = requestPartitionStateFunction;
        this.notifyPartitionDataAvailableFunction = notifyPartitionDataAvailableFunction;
        this.disconnectTaskManagerFunction = disconnectTaskManagerFunction;
        this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
        this.offerSlotsFunction = offerSlotsFunction;
        this.failSlotConsumer = failSlotConsumer;
        this.registerTaskManagerFunction = registerTaskManagerFunction;
        this.taskManagerHeartbeatConsumer = taskManagerHeartbeatConsumer;
        this.resourceManagerHeartbeatConsumer = resourceManagerHeartbeatConsumer;
        this.requestJobDetailsSupplier = requestJobDetailsSupplier;
        this.requestJobSupplier = requestJobSupplier;
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        this.requestOperatorBackPressureStatsFunction = requestOperatorBackPressureStatsFunction;
        this.notifyAllocationFailureConsumer = notifyAllocationFailureConsumer;
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
    public CompletableFuture<Acknowledge> notifyPartitionDataAvailable(
            ResultPartitionID partitionID, Time timeout) {
        return notifyPartitionDataAvailableFunction.apply(partitionID);
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
            String taskManagerRpcAddress,
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
            Time timeout) {
        return registerTaskManagerFunction.apply(
                taskManagerRpcAddress, unresolvedTaskManagerLocation);
    }

    @Override
    public void heartbeatFromTaskManager(
            ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        taskManagerHeartbeatConsumer.accept(resourceID, payload);
    }

    @Override
    public void heartbeatFromResourceManager(ResourceID resourceID) {
        resourceManagerHeartbeatConsumer.accept(resourceID);
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
    public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
        return requestJobSupplier.get();
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory, final boolean cancelJob, final Time timeout) {
        return triggerSavepointFunction.apply(targetDirectory, cancelJob);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final boolean advanceToEndOfEventTime,
            final Time timeout) {
        return stopWithSavepointFunction.apply(targetDirectory, advanceToEndOfEventTime);
    }

    @Override
    public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
            JobVertexID jobVertexId) {
        return requestOperatorBackPressureStatsFunction.apply(jobVertexId);
    }

    @Override
    public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
        notifyAllocationFailureConsumer.accept(allocationID, cause);
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
            TaskStateSnapshot subtaskState) {
        acknowledgeCheckpointConsumer.accept(
                Tuple5.of(
                        jobID, executionAttemptID, checkpointId, checkpointMetrics, subtaskState));
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {
        declineCheckpointConsumer.accept(declineCheckpoint);
    }

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
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return deliverCoordinationRequestFunction.apply(operatorId, serializedRequest);
    }
}
