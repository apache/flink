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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Testing implementation of the {@link SchedulerNG}. */
public class TestingSchedulerNG implements SchedulerNG {
    private final CompletableFuture<JobStatus> jobTerminationFuture;
    private final Runnable startSchedulingRunnable;
    private final Supplier<CompletableFuture<Void>> closeAsyncSupplier;
    private final BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction;
    private final Consumer<Throwable> handleGlobalFailureConsumer;

    private TestingSchedulerNG(
            CompletableFuture<JobStatus> jobTerminationFuture,
            Runnable startSchedulingRunnable,
            Supplier<CompletableFuture<Void>> closeAsyncSupplier,
            BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction,
            Consumer<Throwable> handleGlobalFailureConsumer) {
        this.jobTerminationFuture = jobTerminationFuture;
        this.startSchedulingRunnable = startSchedulingRunnable;
        this.closeAsyncSupplier = closeAsyncSupplier;
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.handleGlobalFailureConsumer = handleGlobalFailureConsumer;
    }

    @Override
    public void startScheduling() {
        startSchedulingRunnable.run();
    }

    private void failOperation() {
        throw new UnsupportedOperationException("This operation is not supported.");
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsyncSupplier.get();
    }

    @Override
    public void cancel() {}

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return jobTerminationFuture;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        handleGlobalFailureConsumer.accept(cause);
    }

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
    public ExecutionGraphInfo requestJob() {
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
    public CompletableFuture<String> stopWithSavepoint(String targetDirectory, boolean terminate) {
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

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {}

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the TestingSchedulerNG. */
    public static final class Builder {
        private CompletableFuture<JobStatus> jobTerminationFuture = new CompletableFuture<>();
        private Runnable startSchedulingRunnable = () -> {};
        private Supplier<CompletableFuture<Void>> closeAsyncSupplier =
                FutureUtils::completedVoidFuture;
        private BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction =
                (ignoredA, ignoredB) -> new CompletableFuture<>();
        private Consumer<Throwable> handleGlobalFailureConsumer = (ignored) -> {};

        public Builder setJobTerminationFuture(CompletableFuture<JobStatus> jobTerminationFuture) {
            this.jobTerminationFuture = jobTerminationFuture;
            return this;
        }

        public Builder setStartSchedulingRunnable(Runnable startSchedulingRunnable) {
            this.startSchedulingRunnable = startSchedulingRunnable;
            return this;
        }

        public Builder setCloseAsyncSupplier(Supplier<CompletableFuture<Void>> closeAsyncSupplier) {
            this.closeAsyncSupplier = closeAsyncSupplier;
            return this;
        }

        public Builder setTriggerSavepointFunction(
                BiFunction<String, Boolean, CompletableFuture<String>> triggerSavepointFunction) {
            this.triggerSavepointFunction = triggerSavepointFunction;
            return this;
        }

        public Builder setHandleGlobalFailureConsumer(
                Consumer<Throwable> handleGlobalFailureConsumer) {
            this.handleGlobalFailureConsumer = handleGlobalFailureConsumer;
            return this;
        }

        public TestingSchedulerNG build() {
            return new TestingSchedulerNG(
                    jobTerminationFuture,
                    startSchedulingRunnable,
                    closeAsyncSupplier,
                    triggerSavepointFunction,
                    handleGlobalFailureConsumer);
        }
    }
}
