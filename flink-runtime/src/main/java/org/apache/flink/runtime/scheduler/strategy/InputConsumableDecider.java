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

package org.apache.flink.runtime.scheduler.strategy;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link InputConsumableDecider} is responsible for determining whether the input of an
 * executionVertex or a consumed partition group is consumable.
 */
public interface InputConsumableDecider {
    /**
     * Determining whether the input of an execution vertex is consumable.
     *
     * @param executionVertex to be determined whether it's input is consumable.
     * @param verticesToSchedule vertices that are not yet scheduled but already decided to be
     *     scheduled.
     * @param consumableStatusCache a cache for {@link ConsumedPartitionGroup} consumable status.
     *     This is to avoid repetitive computation.
     */
    boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,
            Set<ExecutionVertexID> verticesToSchedule,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache);

    /**
     * Determining whether the consumed partition group is consumable based on finished producers.
     *
     * @param consumedPartitionGroup to be determined whether it is consumable.
     */
    boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup);

    /** Factory for {@link InputConsumableDecider}. */
    interface Factory {
        InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever);
    }
}
