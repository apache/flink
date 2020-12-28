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
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.state.KeyGroupRange;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** Testing implementation of the {@link SchedulerNG}. */
public class TestingSchedulerNG implements SchedulerNG {
    private final CompletableFuture<Void> terminationFuture;
    private final Runnable startSchedulingRunnable;
    private final Consumer<Throwable> suspendConsumer;
    private final BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction;

    private TestingSchedulerNG(
            CompletableFuture<Void> terminationFuture,
            Runnable startSchedulingRunnable,
            Consumer<Throwable> suspendConsumer,
            BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction) {
        this.terminationFuture = terminationFuture;
        this.startSchedulingRunnable = startSchedulingRunnable;
        this.suspendConsumer = suspendConsumer;
        this.triggerSavepointFunction = triggerSavepointFunction;
    }

    @Override
    public void initialize(ComponentMainThreadExecutor mainThreadExecutor) {}

    @Override
    public void registerJobStatusListener(JobStatusListener jobStatusListener) {}

    @Override
    public void startScheduling() {
        startSchedulingRunnable.run();
    }

    private void failOperation() {
        throw new UnsupportedOperationException("This operation is not supported.");
    }

    @Override
    public void suspend(Throwable cause) {
        suspendConsumer.accept(cause);
    }

    @Override
    public void cancel() {}

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {}

    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        failOperation();
        return false;
    }

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        failOperation();
        return null;
    }

    @Override
    public ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        failOperation();
        return null;
    }

    @Override
    public void notifyPartitionDataAvailable(ResultPartitionID partitionID) {
        failOperation();
    }

    @Override
    public ArchivedExecutionGraph requestJob() {
        failOperation();
        return null;
    }

    @Override
    public JobStatus requestJobStatus() {
        return JobStatus.CREATED;
    }

    @Override
    public JobDetails requestJobDetails() {
        failOperation();
        return null;
    }

    @Override
    public KvStateLocation requestKvStateLocation(JobID jobId, String registrationName) {
        failOperation();
        return null;
    }

    @Override
    public void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress) {
        failOperation();
    }

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        failOperation();
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        failOperation();
    }

    @Override
    public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(
            JobVertexID jobVertexId) {
        failOperation();
        return Optional.empty();
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob) {
        return triggerSavepointFunction.apply(targetDirectory, cancelJob);
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {
        failOperation();
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        failOperation();
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            String targetDirectory, boolean advanceToEndOfEventTime) {
        failOperation();
        return null;
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt) {
        failOperation();
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) {
        failOperation();
        return null;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the TestingSchedulerNG. */
    public static final class Builder {
        private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        private Runnable startSchedulingRunnable = () -> {};
        private Consumer<Throwable> suspendConsumer = ignored -> {};
        private BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction =
                (ignoredA, ignoredB) -> new CompletableFuture<>();

        public Builder setTerminationFuture(CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
            return this;
        }

        public Builder setStartSchedulingRunnable(Runnable startSchedulingRunnable) {
            this.startSchedulingRunnable = startSchedulingRunnable;
            return this;
        }

        public Builder setSuspendConsumer(Consumer<Throwable> suspendConsumer) {
            this.suspendConsumer = suspendConsumer;
            return this;
        }

        public Builder setTriggerSavepointFunction(
                BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction) {
            this.triggerSavepointFunction = triggerSavepointFunction;
            return this;
        }

        public TestingSchedulerNG build() {
            return new TestingSchedulerNG(
                    terminationFuture,
                    startSchedulingRunnable,
                    suspendConsumer,
                    triggerSavepointFunction);
        }
    }
}
