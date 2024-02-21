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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Default implementation of {@link InputConsumableDecider}. This decider will judge whether the
 * executionVertex's inputs are consumable as follows:
 *
 * <p>For blocking consumed partition group: Whether all result partitions in the group are
 * finished.
 *
 * <p>For canBePipelined consumed partition group: whether all result partitions in the group are
 * scheduled.
 */
public class DefaultInputConsumableDecider implements InputConsumableDecider {
    private final Function<IntermediateResultPartitionID, SchedulingResultPartition>
            resultPartitionRetriever;

    private final Function<ExecutionVertexID, Boolean> scheduledVertexRetriever;

    DefaultInputConsumableDecider(
            Function<ExecutionVertexID, Boolean> scheduledVertexRetriever,
            Function<IntermediateResultPartitionID, SchedulingResultPartition>
                    resultPartitionRetriever) {
        this.scheduledVertexRetriever = scheduledVertexRetriever;
        this.resultPartitionRetriever = resultPartitionRetriever;
    }

    @Override
    public boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,
            Set<ExecutionVertexID> verticesToSchedule,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                executionVertex.getConsumedPartitionGroups()) {

            if (!consumableStatusCache.computeIfAbsent(
                    consumedPartitionGroup,
                    (group) -> isConsumedPartitionGroupConsumable(group, verticesToSchedule))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            // For canBePipelined consumed partition group, whether it is consumable does not depend
            // on task finish. To optimize performance and avoid unnecessary computation, we simply
            // return false.
            return false;
        } else {
            return consumedPartitionGroup.areAllPartitionsFinished();
        }
    }

    private boolean isConsumedPartitionGroupConsumable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<ExecutionVertexID> verticesToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                ExecutionVertexID producerVertex =
                        resultPartitionRetriever.apply(partitionId).getProducer().getId();
                if (!verticesToSchedule.contains(producerVertex)
                        && !scheduledVertexRetriever.apply(producerVertex)) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (resultPartitionRetriever.apply(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Factory for {@link DefaultInputConsumableDecider}. */
    public static class Factory implements InputConsumableDecider.Factory {

        public static final InputConsumableDecider.Factory INSTANCE = new Factory();

        // disable public instantiation.
        private Factory() {}

        @Override
        public InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever) {
            return new DefaultInputConsumableDecider(
                    scheduledVertexRetriever, schedulingTopology::getResultPartition);
        }
    }
}
