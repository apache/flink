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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A simple implementation of {@link SchedulingPipelinedRegion} for testing. */
public class TestingSchedulingPipelinedRegion implements SchedulingPipelinedRegion {

    private final Map<ExecutionVertexID, TestingSchedulingExecutionVertex> regionVertices =
            new HashMap<>();

    private final Set<TestingSchedulingResultPartition> consumedPartitions = new HashSet<>();

    public TestingSchedulingPipelinedRegion(final Set<TestingSchedulingExecutionVertex> vertices) {
        for (TestingSchedulingExecutionVertex vertex : vertices) {
            regionVertices.put(vertex.getId(), vertex);

            for (TestingSchedulingResultPartition consumedPartition : vertex.getConsumedResults()) {
                if (!vertices.contains(consumedPartition.getProducer())) {
                    consumedPartitions.add(consumedPartition);
                }
            }
        }
    }

    @Override
    public Iterable<TestingSchedulingExecutionVertex> getVertices() {
        return Collections.unmodifiableCollection(regionVertices.values());
    }

    @Override
    public TestingSchedulingExecutionVertex getVertex(ExecutionVertexID vertexId) {
        final TestingSchedulingExecutionVertex executionVertex = regionVertices.get(vertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException(
                    String.format("Execution vertex %s not found in pipelined region", vertexId));
        }
        return executionVertex;
    }

    @Override
    public Iterable<TestingSchedulingResultPartition> getConsumedResults() {
        return Collections.unmodifiableSet(consumedPartitions);
    }
}
