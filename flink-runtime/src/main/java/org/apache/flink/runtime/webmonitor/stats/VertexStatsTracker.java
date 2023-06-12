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

package org.apache.flink.runtime.webmonitor.stats;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.util.FlinkException;

import java.util.Optional;

/**
 * Interface for a tracker of statistics for {@link AccessExecutionJobVertex}.
 *
 * @param <T> Type of statistics to track
 */
public interface VertexStatsTracker<T extends Statistics> {

    /**
     * Returns statistics for a job vertex. Automatically triggers sampling request if statistics
     * are not available or outdated.
     *
     * @param jobId job the vertex belongs to
     * @param vertex Vertex to get the stats for.
     * @return Statistics for a job vertex. This interface is intended to be used for polling
     *     request and for the duration while the statistics are being gathered, the returned
     *     Optional can be empty.
     */
    Optional<T> getJobVertexStats(JobID jobId, AccessExecutionJobVertex vertex);

    /**
     * Returns statistics for a execution vertex. Automatically triggers sampling request if
     * statistics are not available or outdated.
     *
     * <p>Note: A single subtask may have multiple attempts, it will return the result of all
     * attempts.
     *
     * @param jobId job the vertex belongs to
     * @param vertex Vertex to get the stats for.
     * @param subtaskIndex SubtaskIndex to get the stats for.
     * @return Statistics for a execution vertex. This interface is intended to be used for polling
     *     request and for the duration while the statistics are being gathered, the returned
     *     Optional can be empty.
     */
    Optional<T> getExecutionVertexStats(
            JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex);

    /**
     * Shuts the {@link VertexStatsTracker} down.
     *
     * @throws FlinkException if the {@link VertexStatsTracker} could not be shut down
     */
    void shutDown() throws FlinkException;
}
