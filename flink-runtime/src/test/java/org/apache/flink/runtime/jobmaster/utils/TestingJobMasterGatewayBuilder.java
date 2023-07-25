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
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
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
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Builder for {@link TestingJobMasterGateway}. */
public class TestingJobMasterGatewayBuilder {
    private static final ResourceID RESOURCE_MANAGER_ID = ResourceID.generate();
    private static final JobMasterId JOB_MASTER_ID = JobMasterId.generate();

    private String address = "pekko.tcp://flink@localhost:6130/user/jobmanager";
    private String hostname = "localhost";
    private Supplier<CompletableFuture<Acknowledge>> cancelFunction =
            () -> CompletableFuture.completedFuture(Acknowledge.get());
    private Function<TaskExecutionState, CompletableFuture<Acknowledge>>
            updateTaskExecutionStateFunction =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private BiFunction<JobVertexID, ExecutionAttemptID, CompletableFuture<SerializedInputSplit>>
            requestNextInputSplitFunction =
                    (ignoredA, ignoredB) ->
                            CompletableFuture.completedFuture(new SerializedInputSplit(null));
    private BiFunction<IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>>
            requestPartitionStateFunction =
                    (ignoredA, ignoredB) ->
                            CompletableFuture.completedFuture(ExecutionState.RUNNING);
    private Function<ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction =
            ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private Consumer<ResourceManagerId> disconnectResourceManagerConsumer = ignored -> {};
    private BiFunction<ResourceID, Collection<SlotOffer>, CompletableFuture<Collection<SlotOffer>>>
            offerSlotsFunction =
                    (ignoredA, ignoredB) ->
                            CompletableFuture.completedFuture(Collections.emptyList());
    private TriConsumer<ResourceID, AllocationID, Throwable> failSlotConsumer =
            (ignoredA, ignoredB, ignoredC) -> {};
    private BiFunction<
                    JobID,
                    TaskManagerRegistrationInformation,
                    CompletableFuture<RegistrationResponse>>
            registerTaskManagerFunction =
                    (ignoredA, ignoredB) ->
                            CompletableFuture.completedFuture(
                                    new JMTMRegistrationSuccess(RESOURCE_MANAGER_ID));
    private BiFunction<
                    ResourceID, TaskExecutorToJobManagerHeartbeatPayload, CompletableFuture<Void>>
            taskManagerHeartbeatFunction =
                    (ignoredA, ignoredB) -> FutureUtils.completedVoidFuture();
    private Function<ResourceID, CompletableFuture<Void>> resourceManagerHeartbeatFunction =
            ignored -> FutureUtils.completedVoidFuture();
    private Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier =
            () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private Supplier<CompletableFuture<ExecutionGraphInfo>> requestJobSupplier =
            () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private Supplier<CompletableFuture<CheckpointStatsSnapshot>> checkpointStatsSnapshotSupplier =
            () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
            triggerSavepointFunction =
                    (targetDirectory, ignoredB, formatType) ->
                            CompletableFuture.completedFuture(
                                    targetDirectory != null
                                            ? targetDirectory
                                            : UUID.randomUUID().toString());
    private Function<CheckpointType, CompletableFuture<CompletedCheckpoint>>
            triggerCheckpointFunction = (prop) -> new CompletableFuture<>();
    private TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
            stopWithSavepointFunction =
                    (targetDirectory, ignoredB, formatType) ->
                            CompletableFuture.completedFuture(
                                    targetDirectory != null
                                            ? targetDirectory
                                            : UUID.randomUUID().toString());
    private Consumer<Tuple5<JobID, ExecutionAttemptID, Long, CheckpointMetrics, TaskStateSnapshot>>
            acknowledgeCheckpointConsumer = ignored -> {};
    private Consumer<DeclineCheckpoint> declineCheckpointConsumer = ignored -> {};
    private Supplier<JobMasterId> fencingTokenSupplier = () -> JOB_MASTER_ID;
    private BiFunction<JobID, String, CompletableFuture<KvStateLocation>>
            requestKvStateLocationFunction =
                    (ignoredA, registrationName) ->
                            FutureUtils.completedExceptionally(
                                    new UnknownKvStateLocation(registrationName));
    private Function<
                    Tuple6<JobID, JobVertexID, KeyGroupRange, String, KvStateID, InetSocketAddress>,
                    CompletableFuture<Acknowledge>>
            notifyKvStateRegisteredFunction =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private Function<
                    Tuple4<JobID, JobVertexID, KeyGroupRange, String>,
                    CompletableFuture<Acknowledge>>
            notifyKvStateUnregisteredFunction =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private TriFunction<String, Object, byte[], CompletableFuture<Object>> updateAggregateFunction =
            (a, b, c) -> CompletableFuture.completedFuture(new Object());
    private TriFunction<
                    ExecutionAttemptID,
                    OperatorID,
                    SerializedValue<OperatorEvent>,
                    CompletableFuture<Acknowledge>>
            operatorEventSender = (a, b, c) -> CompletableFuture.completedFuture(Acknowledge.get());
    private BiFunction<
                    OperatorID,
                    SerializedValue<CoordinationRequest>,
                    CompletableFuture<CoordinationResponse>>
            deliverCoordinationRequestFunction =
                    (a, b) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer =
            ignored -> {};

    private Function<Collection<BlockedNode>, CompletableFuture<Acknowledge>>
            notifyNewBlockedNodesFunction =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());

    private Supplier<CompletableFuture<Map<JobVertexID, Integer>>> maxParallelismPerVertexSupplier =
            () -> CompletableFuture.completedFuture(Collections.emptyMap());

    private Supplier<CompletableFuture<JobResourceRequirements>>
            requestJobResourceRequirementsSupplier =
                    () -> CompletableFuture.completedFuture(JobResourceRequirements.empty());

    private Function<JobResourceRequirements, CompletableFuture<Acknowledge>>
            updateJobResourceRequirementsFunction =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());

    public TestingJobMasterGatewayBuilder setAddress(String address) {
        this.address = address;
        return this;
    }

    public TestingJobMasterGatewayBuilder setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public TestingJobMasterGatewayBuilder setCancelFunction(
            Supplier<CompletableFuture<Acknowledge>> cancelFunction) {
        this.cancelFunction = cancelFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setUpdateTaskExecutionStateFunction(
            Function<TaskExecutionState, CompletableFuture<Acknowledge>>
                    updateTaskExecutionStateFunction) {
        this.updateTaskExecutionStateFunction = updateTaskExecutionStateFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestNextInputSplitFunction(
            BiFunction<JobVertexID, ExecutionAttemptID, CompletableFuture<SerializedInputSplit>>
                    requestNextInputSplitFunction) {
        this.requestNextInputSplitFunction = requestNextInputSplitFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestPartitionStateFunction(
            BiFunction<IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>>
                    requestPartitionStateFunction) {
        this.requestPartitionStateFunction = requestPartitionStateFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setDisconnectTaskManagerFunction(
            Function<ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction) {
        this.disconnectTaskManagerFunction = disconnectTaskManagerFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setDisconnectResourceManagerConsumer(
            Consumer<ResourceManagerId> disconnectResourceManagerConsumer) {
        this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
        return this;
    }

    public TestingJobMasterGatewayBuilder setOfferSlotsFunction(
            BiFunction<ResourceID, Collection<SlotOffer>, CompletableFuture<Collection<SlotOffer>>>
                    offerSlotsFunction) {
        this.offerSlotsFunction = offerSlotsFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setFailSlotConsumer(
            TriConsumer<ResourceID, AllocationID, Throwable> failSlotConsumer) {
        this.failSlotConsumer = failSlotConsumer;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRegisterTaskManagerFunction(
            BiFunction<
                            JobID,
                            TaskManagerRegistrationInformation,
                            CompletableFuture<RegistrationResponse>>
                    registerTaskManagerFunction) {
        this.registerTaskManagerFunction = registerTaskManagerFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setTaskManagerHeartbeatFunction(
            BiFunction<
                            ResourceID,
                            TaskExecutorToJobManagerHeartbeatPayload,
                            CompletableFuture<Void>>
                    taskManagerHeartbeatFunction) {
        this.taskManagerHeartbeatFunction = taskManagerHeartbeatFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setResourceManagerHeartbeatFunction(
            Function<ResourceID, CompletableFuture<Void>> resourceManagerHeartbeatFunction) {
        this.resourceManagerHeartbeatFunction = resourceManagerHeartbeatFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestJobDetailsSupplier(
            Supplier<CompletableFuture<JobDetails>> requestJobDetailsSupplier) {
        this.requestJobDetailsSupplier = requestJobDetailsSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestJobSupplier(
            Supplier<CompletableFuture<ExecutionGraphInfo>> requestJobSupplier) {
        this.requestJobSupplier = requestJobSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setCheckpointStatsSnapshotSupplier(
            Supplier<CompletableFuture<CheckpointStatsSnapshot>> checkpointStatsSnapshotSupplier) {
        this.checkpointStatsSnapshotSupplier = checkpointStatsSnapshotSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setTriggerSavepointFunction(
            TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
                    triggerSavepointFunction) {
        this.triggerSavepointFunction = triggerSavepointFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setTriggerCheckpointFunction(
            Function<CheckpointType, CompletableFuture<CompletedCheckpoint>>
                    triggerCheckpointFunction) {
        this.triggerCheckpointFunction = triggerCheckpointFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setStopWithSavepointSupplier(
            TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>>
                    stopWithSavepointFunction) {
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setNotifyNotEnoughResourcesConsumer(
            Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer) {
        this.notifyNotEnoughResourcesConsumer = notifyNotEnoughResourcesConsumer;
        return this;
    }

    public TestingJobMasterGatewayBuilder setAcknowledgeCheckpointConsumer(
            Consumer<Tuple5<JobID, ExecutionAttemptID, Long, CheckpointMetrics, TaskStateSnapshot>>
                    acknowledgeCheckpointConsumer) {
        this.acknowledgeCheckpointConsumer = acknowledgeCheckpointConsumer;
        return this;
    }

    public TestingJobMasterGatewayBuilder setDeclineCheckpointConsumer(
            Consumer<DeclineCheckpoint> declineCheckpointConsumer) {
        this.declineCheckpointConsumer = declineCheckpointConsumer;
        return this;
    }

    public TestingJobMasterGatewayBuilder setFencingTokenSupplier(
            Supplier<JobMasterId> fencingTokenSupplier) {
        this.fencingTokenSupplier = fencingTokenSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestKvStateLocationFunction(
            BiFunction<JobID, String, CompletableFuture<KvStateLocation>>
                    requestKvStateLocationFunction) {
        this.requestKvStateLocationFunction = requestKvStateLocationFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setNotifyKvStateRegisteredFunction(
            Function<
                            Tuple6<
                                    JobID,
                                    JobVertexID,
                                    KeyGroupRange,
                                    String,
                                    KvStateID,
                                    InetSocketAddress>,
                            CompletableFuture<Acknowledge>>
                    notifyKvStateRegisteredFunction) {
        this.notifyKvStateRegisteredFunction = notifyKvStateRegisteredFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setNotifyKvStateUnregisteredFunction(
            Function<
                            Tuple4<JobID, JobVertexID, KeyGroupRange, String>,
                            CompletableFuture<Acknowledge>>
                    notifyKvStateUnregisteredFunction) {
        this.notifyKvStateUnregisteredFunction = notifyKvStateUnregisteredFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setUpdateAggregateFunction(
            TriFunction<String, Object, byte[], CompletableFuture<Object>>
                    updateAggregateFunction) {
        this.updateAggregateFunction = updateAggregateFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setOperatorEventSender(
            TriFunction<
                            ExecutionAttemptID,
                            OperatorID,
                            SerializedValue<OperatorEvent>,
                            CompletableFuture<Acknowledge>>
                    operatorEventSender) {
        this.operatorEventSender = operatorEventSender;
        return this;
    }

    public TestingJobMasterGatewayBuilder setDeliverCoordinationRequestFunction(
            BiFunction<
                            OperatorID,
                            SerializedValue<CoordinationRequest>,
                            CompletableFuture<CoordinationResponse>>
                    deliverCoordinationRequestFunction) {
        this.deliverCoordinationRequestFunction = deliverCoordinationRequestFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setNotifyNewBlockedNodesFunction(
            Function<Collection<BlockedNode>, CompletableFuture<Acknowledge>>
                    notifyNewBlockedNodesFunction) {
        this.notifyNewBlockedNodesFunction = notifyNewBlockedNodesFunction;
        return this;
    }

    public TestingJobMasterGatewayBuilder setMaxParallelismPerVertexSupplier(
            Supplier<CompletableFuture<Map<JobVertexID, Integer>>>
                    maxParallelismPerVertexSupplier) {
        this.maxParallelismPerVertexSupplier = maxParallelismPerVertexSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setRequestJobResourceRequirementsSupplier(
            Supplier<CompletableFuture<JobResourceRequirements>>
                    requestJobResourceRequirementsSupplier) {
        this.requestJobResourceRequirementsSupplier = requestJobResourceRequirementsSupplier;
        return this;
    }

    public TestingJobMasterGatewayBuilder setUpdateJobResourceRequirementsFunction(
            Function<JobResourceRequirements, CompletableFuture<Acknowledge>>
                    updateJobResourceRequirementsFunction) {
        this.updateJobResourceRequirementsFunction = updateJobResourceRequirementsFunction;
        return this;
    }

    public TestingJobMasterGateway build() {
        return new TestingJobMasterGateway(
                address,
                hostname,
                cancelFunction,
                updateTaskExecutionStateFunction,
                requestNextInputSplitFunction,
                requestPartitionStateFunction,
                disconnectTaskManagerFunction,
                disconnectResourceManagerConsumer,
                offerSlotsFunction,
                failSlotConsumer,
                registerTaskManagerFunction,
                taskManagerHeartbeatFunction,
                resourceManagerHeartbeatFunction,
                requestJobDetailsSupplier,
                requestJobSupplier,
                checkpointStatsSnapshotSupplier,
                triggerSavepointFunction,
                triggerCheckpointFunction,
                stopWithSavepointFunction,
                acknowledgeCheckpointConsumer,
                declineCheckpointConsumer,
                fencingTokenSupplier,
                requestKvStateLocationFunction,
                notifyKvStateRegisteredFunction,
                notifyKvStateUnregisteredFunction,
                updateAggregateFunction,
                operatorEventSender,
                deliverCoordinationRequestFunction,
                notifyNotEnoughResourcesConsumer,
                notifyNewBlockedNodesFunction,
                requestJobResourceRequirementsSupplier,
                updateJobResourceRequirementsFunction);
    }
}
