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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/** Common interface for the runtime {@link ExecutionGraph} and {@link ArchivedExecutionGraph}. */
public interface AccessExecutionGraph extends JobStatusProvider {
    /**
     * Returns the job plan as a JSON string.
     *
     * @return job plan as a JSON string
     */
    String getJsonPlan();

    /**
     * Returns the {@link JobID} for this execution graph.
     *
     * @return job ID for this execution graph
     */
    JobID getJobID();

    /**
     * Returns the job name for the execution graph.
     *
     * @return job name for this execution graph
     */
    String getJobName();

    /**
     * Returns the current {@link JobStatus} for this execution graph.
     *
     * @return job status for this execution graph
     */
    JobStatus getState();

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     *
     * @return failure causing exception, or null
     */
    @Nullable
    ErrorInfo getFailureInfo();

    /**
     * Returns the job vertex for the given {@link JobVertexID}.
     *
     * @param id id of job vertex to be returned
     * @return job vertex for the given id, or {@code null}
     */
    @Nullable
    AccessExecutionJobVertex getJobVertex(JobVertexID id);

    /**
     * Returns a map containing all job vertices for this execution graph.
     *
     * @return map containing all job vertices for this execution graph
     */
    Map<JobVertexID, ? extends AccessExecutionJobVertex> getAllVertices();

    /**
     * Returns an iterable containing all job vertices for this execution graph in the order they
     * were created.
     *
     * @return iterable containing all job vertices for this execution graph in the order they were
     *     created
     */
    Iterable<? extends AccessExecutionJobVertex> getVerticesTopologically();

    /**
     * Returns an iterable containing all execution vertices for this execution graph.
     *
     * @return iterable containing all execution vertices for this execution graph
     */
    Iterable<? extends AccessExecutionVertex> getAllExecutionVertices();

    /**
     * Returns the timestamp for the given {@link JobStatus}.
     *
     * @param status status for which the timestamp should be returned
     * @return timestamp for the given job status
     */
    long getStatusTimestamp(JobStatus status);

    /**
     * Returns the {@link CheckpointCoordinatorConfiguration} or <code>null</code> if checkpointing
     * is disabled.
     *
     * @return JobCheckpointingConfiguration for this execution graph
     */
    @Nullable
    CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration();

    /**
     * Returns a snapshot of the checkpoint statistics or <code>null</code> if checkpointing is
     * disabled.
     *
     * @return Snapshot of the checkpoint statistics for this execution graph
     */
    @Nullable
    CheckpointStatsSnapshot getCheckpointStatsSnapshot();

    /**
     * Returns the {@link ArchivedExecutionConfig} for this execution graph.
     *
     * @return execution config summary for this execution graph, or null in case of errors
     */
    @Nullable
    ArchivedExecutionConfig getArchivedExecutionConfig();

    /**
     * Returns whether the job for this execution graph is stoppable.
     *
     * @return true, if all sources tasks are stoppable, false otherwise
     */
    boolean isStoppable();

    /**
     * Returns the aggregated user-defined accumulators as strings.
     *
     * @return aggregated user-defined accumulators as strings.
     */
    StringifiedAccumulatorResult[] getAccumulatorResultsStringified();

    /**
     * Returns a map containing the serialized values of user-defined accumulators.
     *
     * @return map containing serialized values of user-defined accumulators
     */
    Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized();

    /**
     * Returns whether this execution graph was archived.
     *
     * @return true, if the execution graph was archived, false otherwise
     */
    boolean isArchived();

    /**
     * Returns the state backend name for this ExecutionGraph.
     *
     * @return The state backend name, or an empty Optional in the case of batch jobs
     */
    Optional<String> getStateBackendName();
}
