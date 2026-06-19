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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Context for batch job recovery. */
public interface BatchJobRecoveryContext {

    /**
     * Provides the {@code ExecutionGraph} associated with the job.
     *
     * @return The execution graph.
     */
    ExecutionGraph getExecutionGraph();

    /**
     * Provides the {@code ShuffleMaster} associated with the job.
     *
     * @return The shuffle master.
     */
    ShuffleMaster<?> getShuffleMaster();

    /**
     * Provides the main thread executor.
     *
     * @return The main thread executor.
     */
    ComponentMainThreadExecutor getMainThreadExecutor();

    /**
     * Retrieves a set of vertices that need to be restarted. If result consumption is considered
     * (`basedOnResultConsumable` is true), the set will include all downstream vertices that have
     * finished and upstream vertices that have missed partitions. Otherwise, only include
     * downstream finished vertices.
     *
     * @param vertexId The ID of the vertex from which to compute the restart set.
     * @param considerResultConsumable Indicates whether to consider result partition consumption
     *     while computing the vertices needing restart.
     * @return A set of vertex IDs that need to be restarted.
     */
    Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID vertexId, boolean considerResultConsumable);

    /**
     * Resets vertices specified by their IDs during recovery process.
     *
     * @param verticesToReset The set of vertices that require resetting.
     */
    void resetVerticesInRecovering(Set<ExecutionVertexID> verticesToReset) throws Exception;

    /**
     * Updates the metrics related to the result partition sizes.
     *
     * @param resultPartitionBytes Mapping of partition IDs to their respective result partition
     *     bytes.
     */
    void updateResultPartitionBytesMetrics(
            Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes);

    /**
     * Initializes a given job vertex with the specified parallelism and input information.
     *
     * @param jobVertex The job vertex to initialize.
     * @param parallelism The parallelism to set for the job vertex.
     * @param jobVertexInputInfos The input information for the job vertex.
     * @param createTimestamp The timestamp marking the creation of the job vertex.
     */
    void initializeJobVertex(
            ExecutionJobVertex jobVertex,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos,
            long createTimestamp)
            throws JobException;

    /**
     * Updates the job topology with new job vertices that were initialized.
     *
     * @param newlyInitializedJobVertices List of job vertices that have been initialized.
     */
    void updateTopology(List<ExecutionJobVertex> newlyInitializedJobVertices);

    /**
     * Notifies the recovery finished.
     *
     * @param jobVerticesWithUnRecoveredCoordinators A set of job vertex Ids is associated with job
     *     vertices whose operatorCoordinators did not successfully recover their state. If any
     *     execution within these job vertices needs to be restarted in the future, all other
     *     executions within the same job vertex must also be restarted to ensure the consistency
     *     and correctness of the state.
     */
    void onRecoveringFinished(Set<JobVertexID> jobVerticesWithUnRecoveredCoordinators);

    /** Notifies the recovery failed. */
    void onRecoveringFailed();

    /** Trigger job failure. */
    void failJob(
            Throwable cause, long timestamp, CompletableFuture<Map<String, String>> failureLabels);
}
