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

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.graph.AdaptiveGraphManager;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link AdaptiveExecutionPlanSchedulingContext} class implements the {@link
 * ExecutionPlanSchedulingContext} interface to provide a dynamic scheduling context that adapts
 * execution plans based on runtime conditions.
 */
public class AdaptiveExecutionPlanSchedulingContext implements ExecutionPlanSchedulingContext {

    private final AdaptiveGraphManager adaptiveGraphManager;
    private final int defaultMaxParallelism;

    public AdaptiveExecutionPlanSchedulingContext(
            AdaptiveGraphManager adaptiveGraphManager, int defaultMaxParallelism) {
        this.adaptiveGraphManager = checkNotNull(adaptiveGraphManager);
        this.defaultMaxParallelism = defaultMaxParallelism;
    }

    @Override
    public int getConsumersParallelism(
            Function<JobVertexID, Integer> executionJobVertexParallelismRetriever,
            IntermediateDataSet intermediateDataSet) {
        List<StreamEdge> consumerStreamEdges =
                adaptiveGraphManager.getOutputStreamEdges(intermediateDataSet.getId());
        List<JobEdge> consumerJobEdges = intermediateDataSet.getConsumers();

        Set<Integer> consumerParallelisms;
        if (consumerJobEdges.isEmpty()) {
            checkState(!consumerStreamEdges.isEmpty());
            consumerParallelisms =
                    consumerStreamEdges.stream()
                            .map(StreamEdge::getTargetId)
                            .map(this::getParallelism)
                            .collect(Collectors.toSet());
        } else {
            consumerParallelisms =
                    consumerJobEdges.stream()
                            .map(jobEdge -> jobEdge.getTarget().getID())
                            .map(executionJobVertexParallelismRetriever)
                            .collect(Collectors.toSet());
        }

        // sanity check, all consumer vertices must have the same parallelism:
        // 1. for vertices that are not assigned a parallelism initially (for example, dynamic
        // graph), the parallelisms will all be -1 (parallelism not decided yet)
        // 2. for vertices that are initially assigned a parallelism, the parallelisms must be the
        // same, which is guaranteed at compilation phase
        checkState(consumerParallelisms.size() == 1);
        return consumerParallelisms.iterator().next();
    }

    @Override
    public int getConsumersMaxParallelism(
            Function<JobVertexID, Integer> executionJobVertexMaxParallelismRetriever,
            IntermediateDataSet intermediateDataSet) {
        List<StreamEdge> consumerStreamEdges =
                adaptiveGraphManager.getOutputStreamEdges(intermediateDataSet.getId());
        List<JobEdge> consumerJobEdges = intermediateDataSet.getConsumers();

        Set<Integer> consumerMaxParallelisms;
        if (consumerJobEdges.isEmpty()) {
            checkState(!consumerStreamEdges.isEmpty());
            consumerMaxParallelisms =
                    consumerStreamEdges.stream()
                            .map(StreamEdge::getTargetId)
                            .map(this::getMaxParallelismOrDefault)
                            .collect(Collectors.toSet());
        } else {
            consumerMaxParallelisms =
                    consumerJobEdges.stream()
                            .map(jobEdge -> jobEdge.getTarget().getID())
                            .map(executionJobVertexMaxParallelismRetriever)
                            .collect(Collectors.toSet());
        }
        // sanity check, all consumer vertices must have the same max parallelism
        checkState(
                consumerMaxParallelisms.size() == 1,
                "Consumers must have the same max parallelism.");
        return consumerMaxParallelisms.iterator().next();
    }

    @Override
    public int getPendingOperatorCount() {
        return adaptiveGraphManager.getPendingOperatorsCount();
    }

    @Override
    public String getStreamGraphJson() {
        return adaptiveGraphManager.getStreamGraphJson();
    }

    private int getParallelism(int streamNodeId) {
        return adaptiveGraphManager
                .getStreamGraphContext()
                .getStreamGraph()
                .getStreamNode(streamNodeId)
                .getParallelism();
    }

    private int getMaxParallelismOrDefault(int streamNodeId) {
        ImmutableStreamNode streamNode =
                adaptiveGraphManager
                        .getStreamGraphContext()
                        .getStreamGraph()
                        .getStreamNode(streamNodeId);

        if (streamNode.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT) {
            return AdaptiveBatchScheduler.computeMaxParallelism(
                    streamNode.getParallelism(), defaultMaxParallelism);
        } else {
            return streamNode.getMaxParallelism();
        }
    }
}
