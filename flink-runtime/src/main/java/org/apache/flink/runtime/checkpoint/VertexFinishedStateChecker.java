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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class encapsulates the operation that checks if there are illegal modification to the
 * JobGraph when restoring from a checkpoint with partially or fully finished operator states.
 *
 * <p>As a whole, it ensures
 *
 * <ol>
 *   <li>All the operators inside a JobVertex have the same finished state.
 *   <li>The predecessors of a fully finished vertex must also be fully finished.
 *   <li>The predecessors of a partially finished vertex
 *       <ul>
 *         <li>If connected via ALL_TO_ALL edge, the predecessor must be fully finished.
 *         <li>If connected via POINTWISE edge, the predecessor must be partially finished or fully
 *             finished.
 *       </ul>
 * </ol>
 */
public class VertexFinishedStateChecker {

    private final Set<ExecutionJobVertex> vertices;

    private final Map<OperatorID, OperatorState> operatorStates;

    public VertexFinishedStateChecker(
            Set<ExecutionJobVertex> vertices, Map<OperatorID, OperatorState> operatorStates) {
        this.vertices = vertices;
        this.operatorStates = operatorStates;
    }

    public void validateOperatorsFinishedState() {
        VerticesFinishedStatusCache verticesFinishedCache =
                new VerticesFinishedStatusCache(operatorStates);
        for (ExecutionJobVertex vertex : vertices) {
            VertexFinishedState vertexFinishedState = verticesFinishedCache.getOrUpdate(vertex);

            if (vertexFinishedState == VertexFinishedState.FULLY_FINISHED) {
                checkPredecessorsOfFullyFinishedVertex(vertex, verticesFinishedCache);
            } else if (vertexFinishedState == VertexFinishedState.PARTIALLY_FINISHED) {
                checkPredecessorsOfPartiallyFinishedVertex(vertex, verticesFinishedCache);
            }
        }
    }

    private void checkPredecessorsOfFullyFinishedVertex(
            ExecutionJobVertex vertex, VerticesFinishedStatusCache verticesFinishedStatusCache) {
        boolean allPredecessorsFinished =
                vertex.getInputs().stream()
                        .map(IntermediateResult::getProducer)
                        .allMatch(
                                jobVertex ->
                                        verticesFinishedStatusCache.getOrUpdate(jobVertex)
                                                == VertexFinishedState.FULLY_FINISHED);

        if (!allPredecessorsFinished) {
            throw new FlinkRuntimeException(
                    "Illegal JobGraph modification. Cannot run a program with fully finished"
                            + " vertices predeceased with the ones not fully finished. Task vertex "
                            + vertex.getName()
                            + "("
                            + vertex.getJobVertexId()
                            + ")"
                            + " has a predecessor not fully finished");
        }
    }

    private void checkPredecessorsOfPartiallyFinishedVertex(
            ExecutionJobVertex vertex, VerticesFinishedStatusCache verticesFinishedStatusCache) {
        // Computes the distribution pattern from each predecessor. If there are multiple edges
        // from a single predecessor, ALL_TO_ALL edges would have a higher priority since it
        // implies stricter limitation (must be fully finished).
        Map<JobVertexID, DistributionPattern> predecessorDistribution = new HashMap<>();
        for (JobEdge jobEdge : vertex.getJobVertex().getInputs()) {
            predecessorDistribution.compute(
                    jobEdge.getSource().getProducer().getID(),
                    (k, v) ->
                            v == DistributionPattern.ALL_TO_ALL
                                    ? v
                                    : jobEdge.getDistributionPattern());
        }

        for (IntermediateResult dataset : vertex.getInputs()) {
            ExecutionJobVertex predecessor = dataset.getProducer();
            VertexFinishedState predecessorState =
                    verticesFinishedStatusCache.getOrUpdate(predecessor);
            DistributionPattern distribution =
                    predecessorDistribution.get(predecessor.getJobVertexId());

            if (distribution == DistributionPattern.ALL_TO_ALL
                    && predecessorState != VertexFinishedState.FULLY_FINISHED) {
                throw new FlinkRuntimeException(
                        "Illegal JobGraph modification. Cannot run a program with partially finished"
                                + " vertices predeceased with running or partially finished ones and"
                                + " connected via the ALL_TO_ALL edges. Task vertex "
                                + vertex.getName()
                                + "("
                                + vertex.getJobVertexId()
                                + ")"
                                + " has a "
                                + (predecessorState == VertexFinishedState.ALL_RUNNING
                                        ? "all running"
                                        : "partially finished")
                                + " predecessor");
            } else if (distribution == DistributionPattern.POINTWISE
                    && predecessorState == VertexFinishedState.ALL_RUNNING) {
                throw new FlinkRuntimeException(
                        "Illegal JobGraph modification. Cannot run a program with partially finished"
                                + " vertices predeceased with all running ones. Task vertex "
                                + vertex.getName()
                                + "("
                                + vertex.getJobVertexId()
                                + ")"
                                + " has a all running predecessor");
            }
        }
    }

    @VisibleForTesting
    enum VertexFinishedState {
        ALL_RUNNING,
        PARTIALLY_FINISHED,
        FULLY_FINISHED
    }

    private static class VerticesFinishedStatusCache {
        private final Map<OperatorID, OperatorState> operatorStates;
        private final Map<JobVertexID, VertexFinishedState> finishedCache = new HashMap<>();

        private VerticesFinishedStatusCache(Map<OperatorID, OperatorState> operatorStates) {
            this.operatorStates = operatorStates;
        }

        public VertexFinishedState getOrUpdate(ExecutionJobVertex vertex) {
            return finishedCache.computeIfAbsent(
                    vertex.getJobVertexId(),
                    ignored -> calculateFinishedState(vertex, operatorStates));
        }

        private VertexFinishedState calculateFinishedState(
                ExecutionJobVertex vertex, Map<OperatorID, OperatorState> operatorStates) {
            Set<VertexFinishedState> operatorFinishedStates =
                    vertex.getOperatorIDs().stream()
                            .map(idPair -> checkOperatorFinishedStatus(operatorStates, idPair))
                            .collect(Collectors.toSet());
            if (operatorFinishedStates.size() != 1) {
                throw new FlinkRuntimeException(
                        "Can not restore vertex "
                                + vertex.getName()
                                + "("
                                + vertex.getJobVertexId()
                                + ")"
                                + " which contain mixed operator finished state: "
                                + operatorFinishedStates.stream()
                                        .sorted()
                                        .collect(Collectors.toList()));
            }

            return operatorFinishedStates.iterator().next();
        }

        private VertexFinishedState checkOperatorFinishedStatus(
                Map<OperatorID, OperatorState> operatorStates, OperatorIDPair idPair) {
            OperatorID operatorId =
                    idPair.getUserDefinedOperatorID()
                            .filter(operatorStates::containsKey)
                            .orElse(idPair.getGeneratedOperatorID());
            return Optional.ofNullable(operatorStates.get(operatorId))
                    .map(
                            operatorState -> {
                                if (operatorState.isFullyFinished()) {
                                    return VertexFinishedState.FULLY_FINISHED;
                                }

                                boolean hasFinishedSubtasks =
                                        operatorState.getSubtaskStates().values().stream()
                                                .anyMatch(OperatorSubtaskState::isFinished);
                                return hasFinishedSubtasks
                                        ? VertexFinishedState.PARTIALLY_FINISHED
                                        : VertexFinishedState.ALL_RUNNING;
                            })
                    .orElse(VertexFinishedState.ALL_RUNNING);
        }
    }
}
