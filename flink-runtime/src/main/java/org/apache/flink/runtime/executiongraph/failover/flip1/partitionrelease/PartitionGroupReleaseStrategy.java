/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.List;

/**
 * Interface for strategies that decide when to release {@link ConsumedPartitionGroup
 * ConsumedPartitionGroups}.
 */
public interface PartitionGroupReleaseStrategy {

    /**
     * Calling this method informs the strategy that a vertex finished.
     *
     * @param finishedVertex Id of the vertex that finished the execution
     * @return A list of {@link ConsumedPartitionGroup ConsumedPartitionGroups} that can be released
     */
    List<ConsumedPartitionGroup> vertexFinished(ExecutionVertexID finishedVertex);

    /**
     * Calling this method informs the strategy that a vertex is no longer in finished state, e.g.,
     * when a vertex is re-executed.
     *
     * @param executionVertexID Id of the vertex that is no longer in finished state.
     */
    void vertexUnfinished(ExecutionVertexID executionVertexID);

    /** Factory for {@link PartitionGroupReleaseStrategy}. */
    interface Factory {
        PartitionGroupReleaseStrategy createInstance(SchedulingTopology schedulingStrategy);
    }
}
