/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.TaskManagerRegistrationInformation;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.serializeTaskStateSnapshot;

/**
 * A testing utility, that simulates the desired interactions with {@link JobMasterGateway} RPC.
 * This is useful for light-weight e2e tests, eg. simulating specific fail-over scenario.
 */
public class JobMasterTester implements Closeable {

    private static final Time TIMEOUT = Time.minutes(1);

    private static TaskStateSnapshot createNonEmptyStateSnapshot(TaskInformation taskInformation) {
        final TaskStateSnapshot checkpointStateHandles = new TaskStateSnapshot();
        checkpointStateHandles.putSubtaskStateByOperatorID(
                OperatorID.fromJobVertexID(taskInformation.getJobVertexId()),
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                new OperatorStreamStateHandle(
                                        Collections.emptyMap(),
                                        new ByteStreamStateHandle("foobar", new byte[0])))
                        .build());
        return checkpointStateHandles;
    }

    private static class CheckpointCompletionHandler {

        private final Map<ExecutionAttemptID, CompletableFuture<Void>> completedAttemptFutures;
        private final CompletableFuture<Void> completedFuture;

        public CheckpointCompletionHandler(List<TaskDeploymentDescriptor> descriptors) {
            this.completedAttemptFutures =
                    descriptors.stream()
                            .map(TaskDeploymentDescriptor::getExecutionAttemptId)
                            .collect(
                                    Collectors.toMap(
                                            Function.identity(),
                                            ignored -> new CompletableFuture<>()));
            this.completedFuture = FutureUtils.completeAll(completedAttemptFutures.values());
        }

        void completeAttempt(ExecutionAttemptID executionAttemptId) {
            completedAttemptFutures.get(executionAttemptId).complete(null);
        }

        CompletableFuture<Void> getCompletedFuture() {
            return completedFuture;
        }
    }

    private final UnresolvedTaskManagerLocation taskManagerLocation =
            new LocalUnresolvedTaskManagerLocation();
    private final ConcurrentMap<ExecutionAttemptID, TaskDeploymentDescriptor> descriptors =
            new ConcurrentHashMap<>();

    private final TestingRpcService rpcService;
    private final JobID jobId;
    private final JobMasterGateway jobMasterGateway;
    private final TaskExecutorGateway taskExecutorGateway;

    private final CompletableFuture<List<TaskDeploymentDescriptor>> descriptorsFuture =
            new CompletableFuture<>();

    private final ConcurrentMap<Long, CheckpointCompletionHandler> checkpoints =
            new ConcurrentHashMap<>();

    public JobMasterTester(
            TestingRpcService rpcService, JobID jobId, JobMasterGateway jobMasterGateway) {
        this.rpcService = rpcService;
        this.jobId = jobId;
        this.jobMasterGateway = jobMasterGateway;
        this.taskExecutorGateway = createTaskExecutorGateway();
    }

    public CompletableFuture<Acknowledge> transitionTo(
            List<TaskDeploymentDescriptor> descriptors, ExecutionState state) {
        final List<CompletableFuture<Acknowledge>> futures =
                descriptors.stream()
                        .map(TaskDeploymentDescriptor::getExecutionAttemptId)
                        .map(
                                attemptId ->
                                        jobMasterGateway.updateTaskExecutionState(
                                                new TaskExecutionState(attemptId, state)))
                        .collect(Collectors.toList());
        return FutureUtils.completeAll(futures).thenApply(ignored -> Acknowledge.get());
    }

    public CompletableFuture<List<TaskDeploymentDescriptor>> deployVertices(int numSlots) {
        return jobMasterGateway
                .registerTaskManager(
                        jobId,
                        TaskManagerRegistrationInformation.create(
                                taskExecutorGateway.getAddress(),
                                taskManagerLocation,
                                TestingUtils.zeroUUID()),
                        TIMEOUT)
                .thenCompose(ignored -> offerSlots(numSlots))
                .thenCompose(ignored -> descriptorsFuture);
    }

    public CompletableFuture<Void> getCheckpointFuture(long checkpointId) {
        return descriptorsFuture.thenCompose(
                descriptors ->
                        checkpoints
                                .computeIfAbsent(
                                        checkpointId,
                                        key -> new CheckpointCompletionHandler(descriptors))
                                .getCompletedFuture());
    }

    @Override
    public void close() throws IOException {
        rpcService.unregisterGateway(taskExecutorGateway.getAddress());
    }

    private TaskExecutorGateway createTaskExecutorGateway() {
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setSubmitTaskConsumer(this::onSubmitTaskConsumer)
                        .setTriggerCheckpointFunction(this::onTriggerCheckpoint)
                        .setConfirmCheckpointFunction(this::onConfirmCheckpoint)
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
        return taskExecutorGateway;
    }

    private CompletableFuture<TaskInformation> getTaskInformation(
            ExecutionAttemptID executionAttemptId) {
        return descriptorsFuture.thenApply(
                descriptors -> {
                    final TaskDeploymentDescriptor descriptor =
                            descriptors.stream()
                                    .filter(
                                            desc ->
                                                    executionAttemptId.equals(
                                                            desc.getExecutionAttemptId()))
                                    .findAny()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            String.format(
                                                                    "Task descriptor for %s not found.",
                                                                    executionAttemptId)));
                    try {
                        return descriptor
                                .getSerializedTaskInformation()
                                .deserializeValue(Thread.currentThread().getContextClassLoader());
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                String.format(
                                        "Unable to deserialize task information of %s.",
                                        executionAttemptId));
                    }
                });
    }

    private CompletableFuture<Acknowledge> onTriggerCheckpoint(
            ExecutionAttemptID executionAttemptId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        return getTaskInformation(executionAttemptId)
                .thenCompose(
                        taskInformation -> {
                            jobMasterGateway.acknowledgeCheckpoint(
                                    jobId,
                                    executionAttemptId,
                                    checkpointId,
                                    new CheckpointMetrics(),
                                    serializeTaskStateSnapshot(
                                            createNonEmptyStateSnapshot(taskInformation)));
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        });
    }

    private CompletableFuture<Acknowledge> onConfirmCheckpoint(
            ExecutionAttemptID executionAttemptId, long checkpointId, long checkpointTimestamp) {
        return getTaskInformation(executionAttemptId)
                .thenCompose(
                        taskInformation ->
                                completeAttemptCheckpoint(checkpointId, executionAttemptId));
    }

    private CompletableFuture<Acknowledge> onSubmitTaskConsumer(
            TaskDeploymentDescriptor taskDeploymentDescriptor, JobMasterId jobMasterId) {
        return jobMasterGateway
                .requestJob(TIMEOUT)
                .thenCompose(
                        executionGraphInfo -> {
                            final int numVertices =
                                    Iterables.size(
                                            executionGraphInfo
                                                    .getArchivedExecutionGraph()
                                                    .getAllExecutionVertices());
                            descriptors.put(
                                    taskDeploymentDescriptor.getExecutionAttemptId(),
                                    taskDeploymentDescriptor);
                            if (descriptors.size() == numVertices) {
                                descriptorsFuture.complete(new ArrayList<>(descriptors.values()));
                            }
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        });
    }

    private CompletableFuture<Acknowledge> completeAttemptCheckpoint(
            long checkpointId, ExecutionAttemptID executionAttemptId) {
        return descriptorsFuture
                .thenAccept(
                        descriptors ->
                                checkpoints
                                        .computeIfAbsent(
                                                checkpointId,
                                                key -> new CheckpointCompletionHandler(descriptors))
                                        .completeAttempt(executionAttemptId))
                .thenApply(ignored -> Acknowledge.get());
    }

    private CompletableFuture<Collection<SlotOffer>> offerSlots(int numSlots) {
        final List<SlotOffer> offers = new ArrayList<>();
        for (int idx = 0; idx < numSlots; idx++) {
            offers.add(new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY));
        }
        return jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), offers, TIMEOUT);
    }
}
