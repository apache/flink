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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.QuadFunction;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Builder for a {@link TestingTaskExecutorGateway}. */
public class TestingTaskExecutorGatewayBuilder {

    private static final BiFunction<ResourceID, AllocatedSlotReport, CompletableFuture<Void>>
            NOOP_HEARTBEAT_JOBMANAGER_FUNCTION =
                    (ignoredA, ignoredB) -> FutureUtils.completedVoidFuture();
    private static final BiConsumer<JobID, Throwable> NOOP_DISCONNECT_JOBMANAGER_CONSUMER =
            (ignoredA, ignoredB) -> {};
    private static final BiFunction<
                    TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>>
            NOOP_SUBMIT_TASK_CONSUMER =
                    (ignoredA, ignoredB) -> CompletableFuture.completedFuture(Acknowledge.get());
    private static final Function<
                    Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>,
                    CompletableFuture<Acknowledge>>
            NOOP_REQUEST_SLOT_FUNCTION =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private static final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>>
            NOOP_FREE_SLOT_FUNCTION =
                    (ignoredA, ignoredB) -> CompletableFuture.completedFuture(Acknowledge.get());
    private static final Consumer<JobID> NOOP_FREE_INACTIVE_SLOTS_CONSUMER = ignored -> {};
    private static final Function<ResourceID, CompletableFuture<Void>>
            NOOP_HEARTBEAT_RESOURCE_MANAGER_FUNCTION = ignored -> FutureUtils.completedVoidFuture();
    private static final Consumer<Exception> NOOP_DISCONNECT_RESOURCE_MANAGER_CONSUMER =
            ignored -> {};
    private static final Function<ExecutionAttemptID, CompletableFuture<Acknowledge>>
            NOOP_CANCEL_TASK_FUNCTION =
                    ignored -> CompletableFuture.completedFuture(Acknowledge.get());
    private static final TriConsumer<JobID, Set<ResultPartitionID>, Set<ResultPartitionID>>
            NOOP_RELEASE_PARTITIONS_CONSUMER = (ignoredA, ignoredB, ignoredC) -> {};
    private static final TriFunction<
                    ExecutionAttemptID,
                    OperatorID,
                    SerializedValue<OperatorEvent>,
                    CompletableFuture<Acknowledge>>
            DEFAULT_OPERATOR_EVENT_HANDLER =
                    (a, b, c) -> CompletableFuture.completedFuture(Acknowledge.get());
    private static final Supplier<CompletableFuture<ThreadDumpInfo>> DEFAULT_THREAD_DUMP_SUPPLIER =
            () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private static final Supplier<CompletableFuture<TaskThreadInfoResponse>>
            DEFAULT_THREAD_INFO_SAMPLES_SUPPLIER =
                    () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    private static final QuadFunction<
                    ExecutionAttemptID,
                    Long,
                    Long,
                    CheckpointOptions,
                    CompletableFuture<Acknowledge>>
            NOOP_TRIGGER_CHECKPOINT_FUNCTION =
                    ((executionAttemptId, checkpointId, checkpointTimestamp, checkpointOptions) ->
                            CompletableFuture.completedFuture(Acknowledge.get()));
    private static final TriFunction<ExecutionAttemptID, Long, Long, CompletableFuture<Acknowledge>>
            NOOP_CONFIRM_CHECKPOINT_FUNCTION =
                    ((executionAttemptId, checkpointId, checkpointTimestamp) ->
                            CompletableFuture.completedFuture(Acknowledge.get()));

    private String address = "foobar:1234";
    private String hostname = "foobar";
    private BiFunction<ResourceID, AllocatedSlotReport, CompletableFuture<Void>>
            heartbeatJobManagerFunction = NOOP_HEARTBEAT_JOBMANAGER_FUNCTION;
    private BiConsumer<JobID, Throwable> disconnectJobManagerConsumer =
            NOOP_DISCONNECT_JOBMANAGER_CONSUMER;
    private BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>>
            submitTaskConsumer = NOOP_SUBMIT_TASK_CONSUMER;
    private Function<
                    Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>,
                    CompletableFuture<Acknowledge>>
            requestSlotFunction = NOOP_REQUEST_SLOT_FUNCTION;
    private BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction =
            NOOP_FREE_SLOT_FUNCTION;
    private Consumer<JobID> freeInactiveSlotsConsumer = NOOP_FREE_INACTIVE_SLOTS_CONSUMER;
    private Function<ResourceID, CompletableFuture<Void>> heartbeatResourceManagerFunction =
            NOOP_HEARTBEAT_RESOURCE_MANAGER_FUNCTION;
    private Consumer<Exception> disconnectResourceManagerConsumer =
            NOOP_DISCONNECT_RESOURCE_MANAGER_CONSUMER;
    private Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction =
            NOOP_CANCEL_TASK_FUNCTION;
    private Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier =
            () -> CompletableFuture.completedFuture(true);
    private TriConsumer<JobID, Set<ResultPartitionID>, Set<ResultPartitionID>>
            releaseOrPromotePartitionsConsumer = NOOP_RELEASE_PARTITIONS_CONSUMER;
    private Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer =
            ignored -> {};
    private TriFunction<
                    ExecutionAttemptID,
                    OperatorID,
                    SerializedValue<OperatorEvent>,
                    CompletableFuture<Acknowledge>>
            operatorEventHandler = DEFAULT_OPERATOR_EVENT_HANDLER;
    private Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier =
            DEFAULT_THREAD_DUMP_SUPPLIER;

    private Supplier<CompletableFuture<TaskThreadInfoResponse>> requestThreadInfoSamplesSupplier =
            DEFAULT_THREAD_INFO_SAMPLES_SUPPLIER;

    private QuadFunction<
                    ExecutionAttemptID,
                    Long,
                    Long,
                    CheckpointOptions,
                    CompletableFuture<Acknowledge>>
            triggerCheckpointFunction = NOOP_TRIGGER_CHECKPOINT_FUNCTION;

    private TriFunction<ExecutionAttemptID, Long, Long, CompletableFuture<Acknowledge>>
            confirmCheckpointFunction = NOOP_CONFIRM_CHECKPOINT_FUNCTION;

    public TestingTaskExecutorGatewayBuilder setAddress(String address) {
        this.address = address;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setHeartbeatJobManagerFunction(
            BiFunction<ResourceID, AllocatedSlotReport, CompletableFuture<Void>>
                    heartbeatJobManagerFunction) {
        this.heartbeatJobManagerFunction = heartbeatJobManagerFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setDisconnectJobManagerConsumer(
            BiConsumer<JobID, Throwable> disconnectJobManagerConsumer) {
        this.disconnectJobManagerConsumer = disconnectJobManagerConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setSubmitTaskConsumer(
            BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>>
                    submitTaskConsumer) {
        this.submitTaskConsumer = submitTaskConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setRequestSlotFunction(
            Function<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>,
                            CompletableFuture<Acknowledge>>
                    requestSlotFunction) {
        this.requestSlotFunction = requestSlotFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setFreeSlotFunction(
            BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction) {
        this.freeSlotFunction = freeSlotFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setFreeInactiveSlotsConsumer(
            Consumer<JobID> freeInactiveSlotsConsumer) {
        this.freeInactiveSlotsConsumer = freeInactiveSlotsConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setHeartbeatResourceManagerFunction(
            Function<ResourceID, CompletableFuture<Void>> heartbeatResourceManagerFunction) {
        this.heartbeatResourceManagerFunction = heartbeatResourceManagerFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setDisconnectResourceManagerConsumer(
            Consumer<Exception> disconnectResourceManagerConsumer) {
        this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setCancelTaskFunction(
            Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction) {
        this.cancelTaskFunction = cancelTaskFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setCanBeReleasedSupplier(
            Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier) {
        this.canBeReleasedSupplier = canBeReleasedSupplier;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setReleaseOrPromotePartitionsConsumer(
            TriConsumer<JobID, Set<ResultPartitionID>, Set<ResultPartitionID>>
                    releasePartitionsConsumer) {
        this.releaseOrPromotePartitionsConsumer = releasePartitionsConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setReleaseClusterPartitionsConsumer(
            Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer) {
        this.releaseClusterPartitionsConsumer = releaseClusterPartitionsConsumer;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setOperatorEventHandler(
            TriFunction<
                            ExecutionAttemptID,
                            OperatorID,
                            SerializedValue<OperatorEvent>,
                            CompletableFuture<Acknowledge>>
                    operatorEventHandler) {
        this.operatorEventHandler = operatorEventHandler;
        return this;
    }

    public void setRequestThreadDumpSupplier(
            Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier) {
        this.requestThreadDumpSupplier = requestThreadDumpSupplier;
    }

    public TestingTaskExecutorGatewayBuilder setRequestThreadInfoSamplesSupplier(
            Supplier<CompletableFuture<TaskThreadInfoResponse>> requestThreadInfoSamplesSupplier) {
        this.requestThreadInfoSamplesSupplier = requestThreadInfoSamplesSupplier;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setTriggerCheckpointFunction(
            QuadFunction<
                            ExecutionAttemptID,
                            Long,
                            Long,
                            CheckpointOptions,
                            CompletableFuture<Acknowledge>>
                    triggerCheckpointFunction) {
        this.triggerCheckpointFunction = triggerCheckpointFunction;
        return this;
    }

    public TestingTaskExecutorGatewayBuilder setConfirmCheckpointFunction(
            TriFunction<ExecutionAttemptID, Long, Long, CompletableFuture<Acknowledge>>
                    confirmCheckpointFunction) {
        this.confirmCheckpointFunction = confirmCheckpointFunction;
        return this;
    }

    public TestingTaskExecutorGateway createTestingTaskExecutorGateway() {
        return new TestingTaskExecutorGateway(
                address,
                hostname,
                heartbeatJobManagerFunction,
                disconnectJobManagerConsumer,
                submitTaskConsumer,
                requestSlotFunction,
                freeSlotFunction,
                freeInactiveSlotsConsumer,
                heartbeatResourceManagerFunction,
                disconnectResourceManagerConsumer,
                cancelTaskFunction,
                canBeReleasedSupplier,
                releaseOrPromotePartitionsConsumer,
                releaseClusterPartitionsConsumer,
                operatorEventHandler,
                requestThreadDumpSupplier,
                requestThreadInfoSamplesSupplier,
                triggerCheckpointFunction,
                confirmCheckpointFunction);
    }
}
