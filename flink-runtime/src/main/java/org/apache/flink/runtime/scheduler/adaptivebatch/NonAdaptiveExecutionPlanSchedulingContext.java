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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link NonAdaptiveExecutionPlanSchedulingContext} is a final class that implements a
 * scheduling context for execution plans that do not require adaptive changes.
 */
public final class NonAdaptiveExecutionPlanSchedulingContext
        implements ExecutionPlanSchedulingContext {

    public static final NonAdaptiveExecutionPlanSchedulingContext INSTANCE =
            new NonAdaptiveExecutionPlanSchedulingContext();

    private NonAdaptiveExecutionPlanSchedulingContext() {}

    @Override
    public int getConsumersParallelism(
            Function<JobVertexID, Integer> executionJobVertexParallelismRetriever,
            IntermediateDataSet intermediateDataSet) {
        List<JobEdge> consumers = intermediateDataSet.getConsumers();
        List<JobVertexID> consumerVertices =
                consumers.stream()
                        .map(JobEdge::getTarget)
                        .map(JobVertex::getID)
                        .collect(Collectors.toList());
        checkState(!consumers.isEmpty());

        int consumersParallelism =
                executionJobVertexParallelismRetriever.apply(consumers.get(0).getTarget().getID());
        if (consumers.size() == 1) {
            return consumersParallelism;
        }

        // sanity check, all consumer vertices must have the same parallelism:
        // 1. for vertices that are not assigned a parallelism initially (for example, dynamic
        // graph), the parallelisms will all be -1 (parallelism not decided yet)
        // 2. for vertices that are initially assigned a parallelism, the parallelisms must be the
        // same, which is guaranteed at compilation phase
        for (JobVertexID jobVertexId : consumerVertices) {
            checkState(
                    consumersParallelism
                            == executionJobVertexParallelismRetriever.apply(jobVertexId),
                    "Consumers must have the same parallelism.");
        }
        return consumersParallelism;
    }

    @Override
    public int getConsumersMaxParallelism(
            Function<JobVertexID, Integer> executionJobVertexMaxParallelismRetriever,
            IntermediateDataSet intermediateDataSet) {
        List<JobEdge> consumers = intermediateDataSet.getConsumers();
        List<JobVertexID> consumerVertices =
                consumers.stream()
                        .map(JobEdge::getTarget)
                        .map(JobVertex::getID)
                        .collect(Collectors.toList());

        checkState(!consumers.isEmpty());

        int consumersMaxParallelism =
                executionJobVertexMaxParallelismRetriever.apply(
                        consumers.get(0).getTarget().getID());
        if (consumers.size() == 1) {
            return consumersMaxParallelism;
        }

        // sanity check, all consumer vertices must have the same max parallelism
        for (JobVertexID jobVertexId : consumerVertices) {
            checkState(
                    consumersMaxParallelism
                            == executionJobVertexMaxParallelismRetriever.apply(jobVertexId),
                    "Consumers must have the same max parallelism.");
        }
        return consumersMaxParallelism;
    }

    @Override
    public int getPendingOperatorCount() {
        return 0;
    }

    @Override
    public String getStreamGraphJson() {
        return null;
    }
}
