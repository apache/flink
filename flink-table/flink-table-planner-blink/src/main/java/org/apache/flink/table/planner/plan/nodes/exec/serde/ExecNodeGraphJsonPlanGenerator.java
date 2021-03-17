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

import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitorImpl;
import org.apache.flink.table.planner.plan.utils.ReflectionsUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An utility class that can generate JSON plan based on the given {@link ExecNodeGraph} or generate
 * {@link ExecNodeGraph} based on the given JSON plan.
 */
public class ExecNodeGraphJsonPlanGenerator {

    /** Generate JSON plan based on the given {@link ExecNodeGraph}. */
    public static String generateJsonPlan(ExecNodeGraph execGraph, SerdeContext serdeCtx)
            throws IOException {
        validate(execGraph);
        final ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        final SimpleModule module = new SimpleModule();
        registerSerializers(module);
        mapper.registerModule(module);

        final StringWriter writer = new StringWriter(1024);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            JsonPlanGraph jsonPlanGraph = JsonPlanGraph.fromExecNodeGraph(execGraph);
            gen.writeObject(jsonPlanGraph);
        }

        return writer.toString();
    }

    /** Generate {@link ExecNodeGraph} based on the given JSON plan. */
    @SuppressWarnings({"rawtypes"})
    public static ExecNodeGraph generateExecNodeGraph(String jsonPlan, SerdeContext serdeCtx)
            throws IOException {
        final ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        final SimpleModule module = new SimpleModule();
        final Set<Class<? extends ExecNode>> nodeClasses =
                ReflectionsUtil.scanSubClasses("org.apache.flink", ExecNode.class);
        nodeClasses.forEach(c -> module.registerSubtypes(new NamedType(c, c.getName())));
        registerDeserializers(module);
        mapper.registerModule(module);

        final JsonPlanGraph jsonPlanGraph = mapper.readValue(jsonPlan, JsonPlanGraph.class);
        return jsonPlanGraph.convertToExecNodeGraph(serdeCtx);
    }

    private static void registerSerializers(SimpleModule module) {
        // ObjectIdentifierJsonSerializer is needed for LogicalType serialization
        module.addSerializer(new ObjectIdentifierJsonSerializer());
        // LogicalTypeJsonSerializer is needed for RelDataType serialization
        module.addSerializer(new LogicalTypeJsonSerializer());
        // RelDataTypeJsonSerializer is needed for RexNode serialization
        module.addSerializer(new RelDataTypeJsonSerializer());
        // RexNode is used in many exec nodes, so we register its serializer directly here
        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new AggregateCallJsonSerializer());
        module.addSerializer(new DurationJsonSerializer());
    }

    private static void registerDeserializers(SimpleModule module) {
        // ObjectIdentifierJsonDeserializer is needed for LogicalType deserialization
        module.addDeserializer(ObjectIdentifier.class, new ObjectIdentifierJsonDeserializer());
        // LogicalTypeJsonSerializer is needed for RelDataType serialization
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        // RelDataTypeJsonSerializer is needed for RexNode serialization
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        // RexNode is used in many exec nodes, so we register its deserializer directly here
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addDeserializer(RexLiteral.class, new RexLiteralJsonDeserializer());
        module.addDeserializer(AggregateCall.class, new AggregateCallJsonDeserializer());
        module.addDeserializer(Duration.class, new DurationJsonDeserializer());
    }

    /** Check whether the given {@link ExecNodeGraph} is completely legal. */
    private static void validate(ExecNodeGraph execGraph) {
        ExecNodeVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        if (!JsonSerdeUtil.hasJsonCreatorAnnotation(node.getClass())) {
                            throw new TableException(
                                    String.format(
                                            "%s does not implement @JsonCreator annotation on constructor.",
                                            node.getClass().getCanonicalName()));
                        }
                        super.visitInputs(node);
                    }
                };
        execGraph.getRootNodes().forEach(visitor::visit);
    }

    /**
     * The {@link ExecNodeGraph}'s JSON representation.
     *
     * <p>An {@link ExecNodeGraph} can be converted to a {@link JsonPlanGraph} and be serialized to
     * the JSON plan, or a {@link JsonPlanGraph} can be deserialized from the JSON plan and be
     * converted to an {@link ExecNodeGraph}.
     */
    public static class JsonPlanGraph {
        public static final String FIELD_NAME_FLINK_VERSION = "flinkVersion";
        public static final String FIELD_NAME_NODES = "nodes";
        public static final String FIELD_NAME_EDGES = "edges";

        @JsonProperty(FIELD_NAME_FLINK_VERSION)
        private final String flinkVersion;

        @JsonProperty(FIELD_NAME_NODES)
        private final List<ExecNode<?>> nodes;

        @JsonProperty(FIELD_NAME_EDGES)
        private final List<JsonPlanEdge> edges;

        @JsonCreator
        public JsonPlanGraph(
                @JsonProperty(FIELD_NAME_FLINK_VERSION) String flinkVersion,
                @JsonProperty(FIELD_NAME_NODES) List<ExecNode<?>> nodes,
                @JsonProperty(FIELD_NAME_EDGES) List<JsonPlanEdge> edges) {
            this.flinkVersion = flinkVersion;
            this.nodes = nodes;
            this.edges = edges;
        }

        public static JsonPlanGraph fromExecNodeGraph(ExecNodeGraph execGraph) {
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

        public ExecNodeGraph convertToExecNodeGraph(SerdeContext serdeCtx) {
            Map<Integer, ExecNode<?>> idToExecNodes = new HashMap<>();
            for (ExecNode<?> execNode : nodes) {
                int id = execNode.getId();
                if (idToExecNodes.containsKey(id)) {
                    throw new TableException(
                            String.format(
                                    "The id: %s is not unique for ExecNode: %s.\nplease check it.",
                                    id, execNode.getDescription()));
                }
                if (execNode instanceof CommonExecTableSourceScan) {
                    DynamicTableSourceSpec tableSourceSpec =
                            ((CommonExecTableSourceScan) execNode).getTableSourceSpec();
                    tableSourceSpec.setReadableConfig(serdeCtx.getConfiguration());
                    tableSourceSpec.setClassLoader(serdeCtx.getClassLoader());
                } else if (execNode instanceof CommonExecSink) {
                    DynamicTableSinkSpec tableSinkSpec =
                            ((CommonExecSink) execNode).getTableSinkSpec();
                    tableSinkSpec.setReadableConfig(serdeCtx.getConfiguration());
                    tableSinkSpec.setClassLoader(serdeCtx.getClassLoader());
                }
                idToExecNodes.put(id, execNode);
                if (execNode instanceof StreamExecTableSourceScan) {
                    ((StreamExecTableSourceScan) execNode)
                            .getTableSourceSpec()
                            .setReadableConfig(serdeCtx.getConfiguration());
                    ((StreamExecTableSourceScan) execNode)
                            .getTableSourceSpec()
                            .setClassLoader(serdeCtx.getClassLoader());
                }
            }
            Map<Integer, List<ExecEdge>> idToInputEdges = new HashMap<>();
            Map<Integer, List<ExecEdge>> idToOutputEdges = new HashMap<>();
            for (JsonPlanEdge edge : edges) {
                ExecNode<?> source = idToExecNodes.get(edge.sourceId);
                if (source == null) {
                    throw new TableException(
                            String.format(
                                    "Source node id: %s is not found in nodes.",
                                    edge.getSourceId()));
                }
                ExecNode<?> target = idToExecNodes.get(edge.getTargetId());
                if (target == null) {
                    throw new TableException(
                            String.format(
                                    "Target node id: %s is not found in nodes.",
                                    edge.getTargetId()));
                }
                ExecEdge execEdge =
                        ExecEdge.builder()
                                .source(source)
                                .target(target)
                                .shuffle(edge.getShuffle())
                                .shuffleMode(edge.getShuffleMode())
                                .build();
                idToInputEdges
                        .computeIfAbsent(target.getId(), n -> new ArrayList<>())
                        .add(execEdge);
                idToOutputEdges
                        .computeIfAbsent(source.getId(), n -> new ArrayList<>())
                        .add(execEdge);
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
            return new ExecNodeGraph(rootNodes);
        }
    }

    /**
     * The {@link ExecEdge}'s JSON representation.
     *
     * <p>Different from {@link ExecEdge}, {@link JsonPlanEdge} only stores the {@link ExecNode}'s
     * id instead of instance.
     */
    public static class JsonPlanEdge {
        public static final String FIELD_NAME_SOURCE = "source";
        public static final String FIELD_NAME_TARGET = "target";
        public static final String FIELD_NAME_SHUFFLE = "shuffle";
        public static final String FIELD_NAME_SHUFFLE_MODE = "shuffleMode";

        /** The source node id of this edge. */
        @JsonProperty(FIELD_NAME_SOURCE)
        private final int sourceId;
        /** The target node id of this edge. */
        @JsonProperty(FIELD_NAME_TARGET)
        private final int targetId;
        /** The {@link ExecEdge.Shuffle} on this edge from source to target. */
        @JsonProperty(FIELD_NAME_SHUFFLE)
        @JsonSerialize(using = ShuffleJsonSerializer.class)
        @JsonDeserialize(using = ShuffleJsonDeserializer.class)
        private final ExecEdge.Shuffle shuffle;
        /** The {@link ShuffleMode} defines the data exchange mode on this edge. */
        @JsonProperty(FIELD_NAME_SHUFFLE_MODE)
        private final ShuffleMode shuffleMode;

        @JsonCreator
        public JsonPlanEdge(
                @JsonProperty(FIELD_NAME_SOURCE) int sourceId,
                @JsonProperty(FIELD_NAME_TARGET) int targetId,
                @JsonProperty(FIELD_NAME_SHUFFLE) ExecEdge.Shuffle shuffle,
                @JsonProperty(FIELD_NAME_SHUFFLE_MODE) ShuffleMode shuffleMode) {
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.shuffle = shuffle;
            this.shuffleMode = shuffleMode;
        }

        @JsonIgnore
        public int getSourceId() {
            return sourceId;
        }

        @JsonIgnore
        public int getTargetId() {
            return targetId;
        }

        @JsonIgnore
        public ExecEdge.Shuffle getShuffle() {
            return shuffle;
        }

        @JsonIgnore
        public ShuffleMode getShuffleMode() {
            return shuffleMode;
        }

        /** Build {@link JsonPlanEdge} from an {@link ExecEdge}. */
        public static JsonPlanEdge fromExecEdge(ExecEdge execEdge) {
            return new JsonPlanEdge(
                    execEdge.getSource().getId(),
                    execEdge.getTarget().getId(),
                    execEdge.getShuffle(),
                    execEdge.getShuffleMode());
        }
    }
}
