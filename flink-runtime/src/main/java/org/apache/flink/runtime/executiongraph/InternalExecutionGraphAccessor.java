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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;

/**
 * This interface encapsulates all methods needed by ExecutionJobVertex / ExecutionVertices /
 * Execution from the DefaultExecutionGraph.
 */
public interface InternalExecutionGraphAccessor {

    ClassLoader getUserClassLoader();

    JobID getJobID();

    BlobWriter getBlobWriter();

    Either<SerializedValue<JobInformation>, PermanentBlobKey> getJobInformationOrBlobKey();

    TaskDeploymentDescriptorFactory.PartitionLocationConstraint getPartitionLocationConstraint();

    /**
     * Returns the ExecutionContext associated with this ExecutionGraph.
     *
     * @return ExecutionContext associated with this ExecutionGraph
     */
    Executor getFutureExecutor();

    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();

    ShuffleMaster<?> getShuffleMaster();

    JobMasterPartitionTracker getPartitionTracker();

    void registerExecution(Execution exec);

    void deregisterExecution(Execution exec);

    PartitionReleaseStrategy getPartitionReleaseStrategy();

    void vertexFinished();

    void vertexUnFinished();

    ExecutionDeploymentListener getExecutionDeploymentListener();

    /**
     * Fails the execution graph globally.
     *
     * <p>This global failure is meant to be triggered in cases where the consistency of the
     * execution graph' state cannot be guaranteed any more (for example when catching unexpected
     * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
     * safe way to get consistency back.
     *
     * @param t The exception that caused the failure.
     */
    void failGlobal(Throwable t);

    void notifyExecutionChange(final Execution execution, final ExecutionState newExecutionState);

    void notifySchedulerNgAboutInternalTaskFailure(
            ExecutionAttemptID attemptId,
            Throwable t,
            boolean cancelTask,
            boolean releasePartitions);

    EdgeManager getEdgeManager();

    ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id);

    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);
}
