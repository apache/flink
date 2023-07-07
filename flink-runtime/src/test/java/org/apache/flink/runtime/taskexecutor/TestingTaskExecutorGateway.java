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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.QuadFunction;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Simple {@link TaskExecutorGateway} implementation for testing purposes. */
public class TestingTaskExecutorGateway implements TaskExecutorGateway {

    private final String address;

    private final String hostname;

    private final BiFunction<ResourceID, AllocatedSlotReport, CompletableFuture<Void>>
            heartbeatJobManagerFunction;

    private final BiConsumer<JobID, Throwable> disconnectJobManagerConsumer;

    private final BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>>
            submitTaskConsumer;

    private final Function<
                    Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>,
                    CompletableFuture<Acknowledge>>
            requestSlotFunction;

    private final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>>
            freeSlotFunction;

    private final Consumer<JobID> freeInactiveSlotsConsumer;

    private final Function<ResourceID, CompletableFuture<Void>> heartbeatResourceManagerFunction;

    private final Consumer<Exception> disconnectResourceManagerConsumer;

    private final Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction;

    private final Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier;

    private BiConsumer<JobID, Set<ResultPartitionID>> releasePartitionsConsumer;
    private BiConsumer<JobID, Set<ResultPartitionID>> promotePartitionsConsumer;
    private final Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer;

    private final TriFunction<
                    ExecutionAttemptID,
                    OperatorID,
                    SerializedValue<OperatorEvent>,
                    CompletableFuture<Acknowledge>>
            operatorEventHandler;

    private final Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier;

    private final Supplier<CompletableFuture<TaskThreadInfoResponse>>
            requestThreadInfoSamplesSupplier;

    private final QuadFunction<
                    ExecutionAttemptID,
                    Long,
                    Long,
                    CheckpointOptions,
                    CompletableFuture<Acknowledge>>
            triggerCheckpointFunction;

    private final TriFunction<ExecutionAttemptID, Long, Long, CompletableFuture<Acknowledge>>
            confirmCheckpointFunction;

    TestingTaskExecutorGateway(
            String address,
            String hostname,
            BiFunction<ResourceID, AllocatedSlotReport, CompletableFuture<Void>>
                    heartbeatJobManagerFunction,
            BiConsumer<JobID, Throwable> disconnectJobManagerConsumer,
            BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>>
                    submitTaskConsumer,
            Function<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>,
                            CompletableFuture<Acknowledge>>
                    requestSlotFunction,
            BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction,
            Consumer<JobID> freeInactiveSlotsConsumer,
            Function<ResourceID, CompletableFuture<Void>> heartbeatResourceManagerFunction,
            Consumer<Exception> disconnectResourceManagerConsumer,
            Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction,
            Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier,
            BiConsumer<JobID, Set<ResultPartitionID>> releasePartitionsConsumer,
            BiConsumer<JobID, Set<ResultPartitionID>> promotePartitionsConsumer,
            Consumer<Collection<IntermediateDataSetID>> releaseClusterPartitionsConsumer,
            TriFunction<
                            ExecutionAttemptID,
                            OperatorID,
                            SerializedValue<OperatorEvent>,
                            CompletableFuture<Acknowledge>>
                    operatorEventHandler,
            Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier,
            Supplier<CompletableFuture<TaskThreadInfoResponse>> requestThreadInfoSamplesSupplier,
            QuadFunction<
                            ExecutionAttemptID,
                            Long,
                            Long,
                            CheckpointOptions,
                            CompletableFuture<Acknowledge>>
                    triggerCheckpointFunction,
            TriFunction<ExecutionAttemptID, Long, Long, CompletableFuture<Acknowledge>>
                    confirmCheckpointFunction) {

        this.address = Preconditions.checkNotNull(address);
        this.hostname = Preconditions.checkNotNull(hostname);
        this.heartbeatJobManagerFunction = Preconditions.checkNotNull(heartbeatJobManagerFunction);
        this.disconnectJobManagerConsumer =
                Preconditions.checkNotNull(disconnectJobManagerConsumer);
        this.submitTaskConsumer = Preconditions.checkNotNull(submitTaskConsumer);
        this.requestSlotFunction = Preconditions.checkNotNull(requestSlotFunction);
        this.freeSlotFunction = Preconditions.checkNotNull(freeSlotFunction);
        this.freeInactiveSlotsConsumer = Preconditions.checkNotNull(freeInactiveSlotsConsumer);
        this.heartbeatResourceManagerFunction = heartbeatResourceManagerFunction;
        this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
        this.cancelTaskFunction = cancelTaskFunction;
        this.canBeReleasedSupplier = canBeReleasedSupplier;
        this.releasePartitionsConsumer = releasePartitionsConsumer;
        this.promotePartitionsConsumer = promotePartitionsConsumer;
        this.releaseClusterPartitionsConsumer = releaseClusterPartitionsConsumer;
        this.operatorEventHandler = operatorEventHandler;
        this.requestThreadDumpSupplier = requestThreadDumpSupplier;
        this.requestThreadInfoSamplesSupplier = requestThreadInfoSamplesSupplier;
        this.triggerCheckpointFunction = triggerCheckpointFunction;
        this.confirmCheckpointFunction = confirmCheckpointFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            SlotID slotId,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            String targetAddress,
            ResourceManagerId resourceManagerId,
            Time timeout) {
        return requestSlotFunction.apply(
                Tuple6.of(
                        slotId,
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId));
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {
        return submitTaskConsumer.apply(tdd, jobMasterId);
    }

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
        releasePartitionsConsumer.accept(jobId, partitionIds);
    }

    @Override
    public CompletableFuture<Acknowledge> promotePartitions(
            JobID jobId, Set<ResultPartitionID> partitionIds) {
        promotePartitionsConsumer.accept(jobId, partitionIds);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
        releaseClusterPartitionsConsumer.accept(dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointID,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        return triggerCheckpointFunction.apply(
                executionAttemptID, checkpointID, checkpointTimestamp, checkpointOptions);
    }

    @Override
    public CompletableFuture<Acknowledge> triggerFlushEvent(
            ExecutionAttemptID executionAttemptID,
            long flushEventID,
            long flushEventTimestamp) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long checkpointTimestamp,
            long lastSubsumedCheckpointId) {
        return confirmCheckpointFunction.apply(
                executionAttemptID, checkpointId, checkpointTimestamp);
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long latestCompletedCheckpointId,
            long checkpointTimestamp) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        return cancelTaskFunction.apply(executionAttemptID);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(
            ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport) {
        return heartbeatJobManagerFunction.apply(heartbeatOrigin, allocatedSlotReport);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
        return heartbeatResourceManagerFunction.apply(heartbeatOrigin);
    }

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        disconnectJobManagerConsumer.accept(jobId, cause);
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        disconnectResourceManagerConsumer.accept(cause);
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        return freeSlotFunction.apply(allocationId, cause);
    }

    @Override
    public void freeInactiveSlots(JobID jobId, Time timeout) {
        freeInactiveSlotsConsumer.accept(jobId);
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType, Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName, Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(
            Time timeout) {
        return CompletableFuture.completedFuture(SerializableOptional.empty());
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return canBeReleasedSupplier.get();
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
        return operatorEventHandler.apply(task, operator, evt);
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        return requestThreadDumpSupplier.get();
    }

    @Override
    public CompletableFuture<Acknowledge> updateDelegationTokens(
            ResourceManagerId resourceManagerId, byte[] tokens) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            Collection<ExecutionAttemptID> taskExecutionAttemptIds,
            ThreadInfoSamplesRequest requestParams,
            Time timeout) {
        return requestThreadInfoSamplesSupplier.get();
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }
}
