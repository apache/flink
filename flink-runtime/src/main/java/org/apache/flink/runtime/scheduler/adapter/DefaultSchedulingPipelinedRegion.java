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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link SchedulingPipelinedRegion}. */
public class DefaultSchedulingPipelinedRegion implements SchedulingPipelinedRegion {

    private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVertices;

    private Set<ConsumedPartitionGroup> blockingConsumedPartitionGroups;

    private final Function<IntermediateResultPartitionID, DefaultResultPartition>
            resultPartitionRetriever;

    public DefaultSchedulingPipelinedRegion(
            Set<DefaultExecutionVertex> defaultExecutionVertices,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {

        Preconditions.checkNotNull(defaultExecutionVertices);

        this.executionVertices = new HashMap<>();
        for (DefaultExecutionVertex executionVertex : defaultExecutionVertices) {
            this.executionVertices.put(executionVertex.getId(), executionVertex);
        }

        this.resultPartitionRetriever = checkNotNull(resultPartitionRetriever);
    }

    @Override
    public Iterable<DefaultExecutionVertex> getVertices() {
        return Collections.unmodifiableCollection(executionVertices.values());
    }

    @Override
    public DefaultExecutionVertex getVertex(final ExecutionVertexID vertexId) {
        final DefaultExecutionVertex executionVertex = executionVertices.get(vertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException(
                    String.format("Execution vertex %s not found in pipelined region", vertexId));
        }
        return executionVertex;
    }

    private void initializeAllBlockingConsumedPartitionGroups() {
        final Set<ConsumedPartitionGroup> consumedPartitionGroupSet = new HashSet<>();
        for (DefaultExecutionVertex executionVertex : executionVertices.values()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    executionVertex.getConsumedPartitionGroups()) {
                SchedulingResultPartition consumedPartition =
                        resultPartitionRetriever.apply(consumedPartitionGroup.getFirst());

                checkState(
                        consumedPartition.getConsumerVertexGroups().size() <= 1,
                        "Currently there has to be exactly one consumer for each partition in real jobs.");

                if (consumedPartition.getResultType().isBlocking()) {
                    consumedPartitionGroupSet.add(consumedPartitionGroup);
                }
            }
        }

        this.blockingConsumedPartitionGroups =
                Collections.unmodifiableSet(consumedPartitionGroupSet);
    }

    @Override
    public Iterable<ConsumedPartitionGroup> getAllBlockingConsumedPartitionGroups() {
        if (blockingConsumedPartitionGroups == null) {
            initializeAllBlockingConsumedPartitionGroups();
        }
        return blockingConsumedPartitionGroups;
    }

    @Override
    public boolean contains(final ExecutionVertexID vertexId) {
        return executionVertices.containsKey(vertexId);
    }
}
