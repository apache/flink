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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.state.StateObjectID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Stream.empty;

/**
 * Finds shared state objects based on assignment of state to {@link ExecutionVertex}es. State
 * sharing may result either from rescaling or from using the same physical state object by
 * different operators (state backends).
 *
 * <p>More precisely, shared state is a state with more than one (conflicting) owners. An owner here
 * means TaskStateRegistry - because it eventually deletes the state.
 *
 * <p>At recovery time, the exact correspondence of states to TaskStateRegistries is not known.
 * Furthermore, it can change if a subtask is deployed to another TM as result of a failure (because
 * state backends might choose to share TaskStateRegistries in the same task or TM, so the
 * correspondence would change).
 *
 * <p>Assuming each parallel operator instance is an owner would be safe, but inefficient: even if
 * multiple operators in a chain share the same TaskStateRegistry, their states would still be
 * considered as shared.
 *
 * <p>Assuming each parallel operator <b>chain</b> instance is also safe because it can not be split
 * (without restarting the job). Therefore, each {@link ExecutionVertex} can be assumed a potential
 * state owner.
 */
@Internal
class SharedStateCollector {

    /**
     * NOTE: the state must be already assigned.
     *
     * @return a stream of tuples of {@link StateObjectID}s and a list of {@link ExecutionVertex}es
     *     using it.
     */
    static Set<StateObjectID> collect(Collection<ExecutionJobVertex> vertexAssignments) {
        return vertexAssignments.stream()
                .flatMap(ejv -> Arrays.stream(ejv.getTaskVertices()))
                .flatMap(SharedStateCollector::collect)
                .collect(groupingBy(stateIdAndVertex -> stateIdAndVertex.f0, VERTEX_COLLECTOR))
                .entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private static Stream<Tuple2<StateObjectID, ExecutionVertex>> collect(ExecutionVertex vertex) {
        JobManagerTaskRestore restore = vertex.getCurrentExecutionAttempt().getTaskRestore();
        return restore == null
                ? empty()
                : restore.getTaskStateSnapshot().getSubtaskStateMappings().stream()
                        .flatMap(stateEntry -> streamIDs(stateEntry.getValue(), vertex));
    }

    private static Stream<Tuple2<StateObjectID, ExecutionVertex>> streamIDs(
            OperatorSubtaskState state, ExecutionVertex usingVertex) {
        return getAllStateObjectIDs(state).stream().map(stateId -> Tuple2.of(stateId, usingVertex));
    }

    private static Collection<StateObjectID> getAllStateObjectIDs(OperatorSubtaskState state) {
        StateObjectIdCollectingVisitor visitor = new StateObjectIdCollectingVisitor();
        state.accept(visitor);
        return visitor.getStateObjectIDs();
    }

    /**
     * Collects {@link ExecutionVertex}es into a list when grouping them by {@link StateObjectID}.
     */
    private static final Collector<
                    Tuple2<StateObjectID, ExecutionVertex>,
                    List<ExecutionVertex>,
                    List<ExecutionVertex>>
            VERTEX_COLLECTOR =
                    Collector.of(
                            ArrayList::new,
                            (list, stateAndVertex) -> list.add(stateAndVertex.f1),
                            (list1, list2) -> {
                                list1.addAll(list2);
                                return list1;
                            });

    private SharedStateCollector() {}
}
