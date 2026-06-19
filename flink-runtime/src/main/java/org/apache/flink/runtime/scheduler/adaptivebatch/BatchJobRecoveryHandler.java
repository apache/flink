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

import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Collection;
import java.util.Map;

/** Interface for handling batch job recovery. */
public interface BatchJobRecoveryHandler {

    /** Initializes the recovery handler with the batch job recovery context. */
    void initialize(BatchJobRecoveryContext batchJobRecoveryContext);

    /** Starts the recovery process. */
    void startRecovering();

    /**
     * Stops the job recovery handler and optionally clean up.
     *
     * @param cleanUp whether to clean up.
     */
    void stop(boolean cleanUp);

    /** Determines whether the job needs to undergo recovery. */
    boolean needRecover();

    /** Determines whether the job is recovering. */
    boolean isRecovering();

    /**
     * Handles the reset event for a collection of execution vertices and records the event for use
     * during future batch job recovery.
     *
     * @param vertices a collection of execution vertex IDs that have been reset.
     */
    void onExecutionVertexReset(Collection<ExecutionVertexID> vertices);

    /**
     * Records the job vertex initialization event for use during future batch job recovery.
     *
     * @param jobVertexId the id of the job vertex being initialized.
     * @param parallelism the parallelism of the job vertex.
     * @param jobVertexInputInfos a map of intermediate dataset IDs to job vertex input info.
     */
    void onExecutionJobVertexInitialization(
            JobVertexID jobVertexId,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos);

    /**
     * Records the execution vertex finished event for use during future batch job recovery.
     *
     * @param executionVertexId the id of the execution vertex is finished.
     */
    void onExecutionFinished(ExecutionVertexID executionVertexId);
}
