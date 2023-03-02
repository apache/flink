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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyedStateHandle;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/** Managed Keyed State size estimates used to make scheduling decisions. */
@Internal
public class StateSizeEstimates {
    private final Map<JobVertexID, Long> averages;

    public StateSizeEstimates() {
        this(Collections.emptyMap());
    }

    public StateSizeEstimates(Map<JobVertexID, Long> averages) {
        this.averages = averages;
    }

    public Optional<Long> estimate(JobVertexID jobVertexId) {
        return Optional.ofNullable(averages.get(jobVertexId));
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
                                build(
                                        fromCompletedCheckpoint(cp),
                                        mapVerticesToOperators(executionGraph)))
                .orElse(empty());
    }

    private static StateSizeEstimates build(
            Map<OperatorID, Long> sizePerOperator,
            Map<JobVertexID, Set<OperatorID>> verticesToOperators) {
        Map<JobVertexID, Long> verticesToSizes =
                verticesToOperators.entrySet().stream()
                        .collect(
                                toMap(Map.Entry::getKey, e -> size(e.getValue(), sizePerOperator)));
        return new StateSizeEstimates(verticesToSizes);
    }

    private static long size(Set<OperatorID> ids, Map<OperatorID, Long> sizes) {
        return ids.stream().mapToLong(key -> sizes.getOrDefault(key, 0L)).sum();
    }

    private static Map<JobVertexID, Set<OperatorID>> mapVerticesToOperators(
            ExecutionGraph executionGraph) {
        return executionGraph.getAllVertices().entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> getOperatorIDS(e.getValue())));
    }

    private static Set<OperatorID> getOperatorIDS(ExecutionJobVertex v) {
        return v.getOperatorIDs().stream()
                .map(OperatorIDPair::getGeneratedOperatorID)
                .collect(Collectors.toSet());
    }

    private static Map<OperatorID, Long> fromCompletedCheckpoint(CompletedCheckpoint cp) {
        Stream<Map.Entry<OperatorID, OperatorState>> states =
                cp.getOperatorStates().entrySet().stream();
        return states.collect(
                toMap(
                        Map.Entry::getKey,
                        e -> calculateAverageKeyGroupStateSizeInBytes(e.getValue())));
    }

    private static long calculateAverageKeyGroupStateSizeInBytes(OperatorState state) {
        Stream<KeyedStateHandle> handles =
                state.getSubtaskStates().values().stream()
                        .flatMap(s -> s.getManagedKeyedState().stream());
        Stream<Tuple2<Long, Integer>> sizeAndCount =
                handles.map(
                        h ->
                                Tuple2.of(
                                        h.getStateSize(),
                                        h.getKeyGroupRange().getNumberOfKeyGroups()));
        Optional<Tuple2<Long, Integer>> totalSizeAndCount =
                sizeAndCount.reduce(
                        (left, right) -> Tuple2.of(left.f0 + right.f0, left.f1 + right.f1));
        Optional<Long> average = totalSizeAndCount.filter(t2 -> t2.f1 > 0).map(t2 -> t2.f0 / t2.f1);
        return average.orElse(0L);
    }
}
