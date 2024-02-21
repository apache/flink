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
 * {@link AllFinishedInputConsumableDecider} is a special {@link InputConsumableDecider}. The input
 * is considered to be consumable only when all producer partitions are finished.
 */
public class AllFinishedInputConsumableDecider implements InputConsumableDecider {

    @Override
    public boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,
            Set<ExecutionVertexID> verticesToDeploy,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                executionVertex.getConsumedPartitionGroups()) {

            if (!consumableStatusCache.computeIfAbsent(
                    consumedPartitionGroup, this::isConsumableBasedOnFinishedProducers)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup) {
        return consumedPartitionGroup.getNumberOfUnfinishedPartitions() == 0;
    }

    /** Factory for {@link AllFinishedInputConsumableDecider}. */
    public static class Factory implements InputConsumableDecider.Factory {

        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        @Override
        public InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever) {
            return new AllFinishedInputConsumableDecider();
        }
    }
}
