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
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A class that decorates/forwards calls to a {@link TaskExecutorGateway}.
 *
 * <p>This class is meant as a base for custom decorators, to avoid having to maintain all the
 * method overrides in each decorator.
 */
public class TaskExecutorGatewayDecoratorBase implements TaskExecutorGateway {

    protected final TaskExecutorGateway originalGateway;

    protected TaskExecutorGatewayDecoratorBase(TaskExecutorGateway originalGateway) {
        this.originalGateway = originalGateway;
    }

    @Override
    public String getAddress() {
        return originalGateway.getAddress();
    }

    @Override
    public String getHostname() {
        return originalGateway.getHostname();
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
        return originalGateway.requestSlot(
                slotId,
                jobId,
                allocationId,
                resourceProfile,
                targetAddress,
                resourceManagerId,
                timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {
        return originalGateway.submitTask(tdd, jobMasterId, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        return originalGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
    }

    @Override
    public void releaseOrPromotePartitions(
            JobID jobId,
            Set<ResultPartitionID> partitionToRelease,
            Set<ResultPartitionID> partitionsToPromote) {
        originalGateway.releaseOrPromotePartitions(jobId, partitionToRelease, partitionsToPromote);
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
        return originalGateway.releaseClusterPartitions(dataSetsToRelease, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointID,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        return originalGateway.triggerCheckpoint(
                executionAttemptID, checkpointID, checkpointTimestamp, checkpointOptions);
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        return originalGateway.confirmCheckpoint(
                executionAttemptID, checkpointId, checkpointTimestamp);
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
        return originalGateway.abortCheckpoint(
                executionAttemptID, checkpointId, checkpointTimestamp);
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        return originalGateway.cancelTask(executionAttemptID, timeout);
    }

    @Override
    public void heartbeatFromJobManager(
            ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport) {
        originalGateway.heartbeatFromJobManager(heartbeatOrigin, allocatedSlotReport);
    }

    @Override
    public void heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
        originalGateway.heartbeatFromResourceManager(heartbeatOrigin);
    }

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        originalGateway.disconnectJobManager(jobId, cause);
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        originalGateway.disconnectResourceManager(cause);
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        return originalGateway.freeSlot(allocationId, cause, timeout);
    }

    @Override
    public void freeInactiveSlots(JobID jobId, Time timeout) {
        originalGateway.freeInactiveSlots(jobId, timeout);
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType, Time timeout) {
        return originalGateway.requestFileUploadByType(fileType, timeout);
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName, Time timeout) {
        return originalGateway.requestFileUploadByName(fileName, timeout);
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(
            Time timeout) {
        return originalGateway.requestMetricQueryServiceAddress(timeout);
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return originalGateway.canBeReleased();
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
        return originalGateway.requestLogList(timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
        return originalGateway.sendOperatorEventToTask(task, operator, evt);
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        return originalGateway.requestThreadDump(timeout);
    }

    @Override
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            ExecutionAttemptID taskExecutionAttemptId,
            ThreadInfoSamplesRequest requestParams,
            Time timeout) {
        return originalGateway.requestThreadInfoSamples(
                taskExecutionAttemptId, requestParams, timeout);
    }
}
