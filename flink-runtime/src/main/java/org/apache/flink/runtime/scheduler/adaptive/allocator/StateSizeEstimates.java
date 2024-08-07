/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyedStateHandle;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/** Managed Keyed State size estimates used to make scheduling decisions. */
@Internal
public class StateSizeEstimates {
    private final Map<ExecutionVertexID, Long> stateSizes;

    public StateSizeEstimates() {
        this(emptyMap());
    }

    public StateSizeEstimates(Map<ExecutionVertexID, Long> stateSizes) {
        this.stateSizes = stateSizes;
    }

    public Optional<Long> estimate(ExecutionVertexID jobVertexId) {
        return Optional.ofNullable(stateSizes.get(jobVertexId));
    }

    static StateSizeEstimates empty() {
        return new StateSizeEstimates();
    }

    public static StateSizeEstimates fromGraph(@Nullable ExecutionGraph executionGraph) {
        return Optional.ofNullable(executionGraph)
                .flatMap(graph -> Optional.ofNullable(graph.getCheckpointCoordinator()))
                .flatMap(coordinator -> Optional.ofNullable(coordinator.getCheckpointStore()))
                .flatMap(store -> Optional.ofNullable(store.getLatestCheckpoint()))
                .map(
                        cp ->
                                new StateSizeEstimates(
                                        merge(
                                                fromCompletedCheckpoint(cp),
                                                mapVerticesToOperators(executionGraph))))
                .orElse(empty());
    }

    /**
     * Map {@link ExecutionVertexID}s to state sizes according to the supplied mappings of operators
     * to state sizes and {@link JobVertexID}s to {@link OperatorID}s.
     */
    private static Map<ExecutionVertexID, Long> merge(
            Map<OperatorID, Map<Integer, Long>> operatorsToSubtaskSizes,
            Map<JobVertexID, Set<OperatorID>> verticesToOperators) {
        Map<ExecutionVertexID, Long> result = new HashMap<>();
        for (Entry<JobVertexID, Set<OperatorID>> vertexAndOperators :
                verticesToOperators.entrySet()) {
            for (OperatorID operatorID : vertexAndOperators.getValue()) {
                for (Entry<Integer, Long> subtaskIdAndSize :
                        operatorsToSubtaskSizes.getOrDefault(operatorID, emptyMap()).entrySet()) {
                    result.merge(
                            new ExecutionVertexID(
                                    vertexAndOperators.getKey(), subtaskIdAndSize.getKey()),
                            subtaskIdAndSize.getValue(),
                            Long::sum);
                }
            }
        }
        return result;
    }

    private static Map<JobVertexID, Set<OperatorID>> mapVerticesToOperators(
            ExecutionGraph executionGraph) {
        return executionGraph.getAllVertices().entrySet().stream()
                .collect(toMap(Entry::getKey, e -> getOperatorIDS(e.getValue())));
    }

    private static Set<OperatorID> getOperatorIDS(ExecutionJobVertex v) {
        return v.getOperatorIDs().stream()
                .map(OperatorIDPair::getGeneratedOperatorID)
                .collect(Collectors.toSet());
    }

    private static Map<OperatorID, Map<Integer, Long>> fromCompletedCheckpoint(
            CompletedCheckpoint cp) {
        Map<OperatorID, Map<Integer, Long>> result = new HashMap<>();
        for (Entry<OperatorID, OperatorState> e : cp.getOperatorStates().entrySet()) {
            result.put(e.getKey(), calculateStateSizeInBytes(e.getValue()));
        }
        return result;
    }

    private static Map<Integer, Long> calculateStateSizeInBytes(OperatorState state) {
        Map<Integer, Long> sizesPerSubtask = new HashMap<>();
        for (Entry<Integer, OperatorSubtaskState> e : state.getSubtaskStates().entrySet()) {
            for (KeyedStateHandle handle : e.getValue().getManagedKeyedState()) {
                sizesPerSubtask.merge(e.getKey(), handle.getStateSize(), Long::sum);
            }
        }
        return sizesPerSubtask;
    }
}
