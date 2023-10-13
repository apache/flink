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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitorImpl;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link ExecNodeGraph}'s JSON representation.
 *
 * <p>An {@link ExecNodeGraph} can be converted to a {@link JsonPlanGraph} and be serialized to the
 * JSON plan, or a {@link JsonPlanGraph} can be deserialized from the JSON plan and be converted to
 * an {@link ExecNodeGraph}.
 *
 * <p>This model is used only during serialization/deserialization.
 */
@Internal
final class JsonPlanGraph {
    static final String FIELD_NAME_FLINK_VERSION = "flinkVersion";
    static final String FIELD_NAME_NODES = "nodes";
    static final String FIELD_NAME_EDGES = "edges";

    @JsonProperty(FIELD_NAME_FLINK_VERSION)
    private final FlinkVersion flinkVersion;

    @JsonProperty(FIELD_NAME_NODES)
    private final List<ExecNode<?>> nodes;

    @JsonProperty(FIELD_NAME_EDGES)
    private final List<JsonPlanEdge> edges;

    @JsonCreator
    JsonPlanGraph(
            @JsonProperty(FIELD_NAME_FLINK_VERSION) FlinkVersion flinkVersion,
            @JsonProperty(FIELD_NAME_NODES) List<ExecNode<?>> nodes,
            @JsonProperty(FIELD_NAME_EDGES) List<JsonPlanEdge> edges) {
        this.flinkVersion = flinkVersion;
        this.nodes = nodes;
        this.edges = edges;
    }

    static JsonPlanGraph fromExecNodeGraph(ExecNodeGraph execGraph) {
        final List<ExecNode<?>> allNodes = new ArrayList<>();
        final List<JsonPlanEdge> allEdges = new ArrayList<>();
        final Set<Integer> nodesIds = new HashSet<>();
        // for quick search
        final Set<ExecNode<?>> visitedNodes = Sets.newIdentityHashSet();

        // visit the nodes as topological ordering
        final ExecNodeVisitor visitor =
                new ExecNodeVisitorImpl() {
                    @Override
                    public void visit(ExecNode<?> node) {
                        if (visitedNodes.contains(node)) {
                            return;
                        }
                        super.visitInputs(node);

                        final int id = node.getId();
                        if (nodesIds.contains(id)) {
                            throw new TableException(
                                    String.format(
                                            "The id: %s is not unique for ExecNode: %s.\nplease check it.",
                                            id, node.getDescription()));
                        }

                        allNodes.add(node);
                        nodesIds.add(id);
                        visitedNodes.add(node);
                        for (ExecEdge execEdge : node.getInputEdges()) {
                            allEdges.add(JsonPlanEdge.fromExecEdge(execEdge));
                        }
                    }
                };

        execGraph.getRootNodes().forEach(visitor::visit);
        checkArgument(allNodes.size() == nodesIds.size());

        return new JsonPlanGraph(execGraph.getFlinkVersion(), allNodes, allEdges);
    }

    ExecNodeGraph convertToExecNodeGraph() {
        Map<Integer, ExecNode<?>> idToExecNodes = new HashMap<>();
        for (ExecNode<?> execNode : nodes) {
            int id = execNode.getId();
            if (idToExecNodes.containsKey(id)) {
                throw new TableException(
                        String.format(
                                "The id: %s is not unique for ExecNode: %s.\nplease check it.",
                                id, execNode.getDescription()));
            }
            idToExecNodes.put(id, execNode);
        }
        Map<Integer, List<ExecEdge>> idToInputEdges = new HashMap<>();
        Map<Integer, List<ExecEdge>> idToOutputEdges = new HashMap<>();
        for (JsonPlanEdge edge : edges) {
            ExecNode<?> source = idToExecNodes.get(edge.getSourceId());
            if (source == null) {
                throw new TableException(
                        String.format(
                                "Source node id: %s is not found in nodes.", edge.getSourceId()));
            }
            ExecNode<?> target = idToExecNodes.get(edge.getTargetId());
            if (target == null) {
                throw new TableException(
                        String.format(
                                "Target node id: %s is not found in nodes.", edge.getTargetId()));
            }
            ExecEdge execEdge =
                    ExecEdge.builder()
                            .source(source)
                            .target(target)
                            .shuffle(edge.getShuffle())
                            .exchangeMode(edge.getExchangeMode())
                            .build();
            idToInputEdges.computeIfAbsent(target.getId(), n -> new ArrayList<>()).add(execEdge);
            idToOutputEdges.computeIfAbsent(source.getId(), n -> new ArrayList<>()).add(execEdge);
        }

        List<ExecNode<?>> rootNodes = new ArrayList<>();
        for (Map.Entry<Integer, ExecNode<?>> entry : idToExecNodes.entrySet()) {
            int id = entry.getKey();
            ExecNode<?> node = entry.getValue();
            // connect input edges
            List<ExecEdge> inputEdges = idToInputEdges.getOrDefault(id, new ArrayList<>());
            node.setInputEdges(inputEdges);

            if (!idToOutputEdges.containsKey(id)) {
                // if the node has no output nodes, it's a root node
                rootNodes.add(node);
            }
        }
        return new ExecNodeGraph(flinkVersion, rootNodes);
    }
}
