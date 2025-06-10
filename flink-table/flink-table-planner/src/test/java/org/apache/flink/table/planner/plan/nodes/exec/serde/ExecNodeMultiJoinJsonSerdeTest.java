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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExecNodeMultiJoinJsonSerdeTest {
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    private final SerdeContext serdeContext = JsonSerdeTestUtil.configuredSerdeContext();

    @Test
    void testSerializingStreamExecMultiJoin() throws IOException {
        // Create test data
        final StreamExecMultiJoin execNode = createTestMultiJoinNode();
        final ExecNodeGraph graph =
                new ExecNodeGraph(FlinkVersion.v2_1, Collections.singletonList(execNode));

        // Test if we can serialize
        final String serializedGraph = JsonSerdeTestUtil.toJson(serdeContext, graph);
        assertThat(serializedGraph).isNotEmpty();

        // Test if we can deserialize
        final ExecNodeGraph deserializedGraph =
                JsonSerdeTestUtil.toObject(serdeContext, serializedGraph, ExecNodeGraph.class);

        // Verify some general value checks on the deserialized node
        assertThat(deserializedGraph.getRootNodes()).hasSize(1);
        final StreamExecMultiJoin deserializedMultiJoinNode =
                (StreamExecMultiJoin) deserializedGraph.getRootNodes().get(0);
        assertThat(deserializedMultiJoinNode.getDescription()).isEqualTo("test-multi-join");
        assertThat(deserializedMultiJoinNode.getOutputType())
                .isEqualTo(RowType.of(VarCharType.STRING_TYPE, new IntType()));
    }

    @Test
    void testSerializedJsonStructure() throws IOException {
        // Create test data
        final StreamExecMultiJoin execNode = createTestMultiJoinNode();
        final ExecNodeGraph graph =
                new ExecNodeGraph(FlinkVersion.v2_1, Collections.singletonList(execNode));

        // Serialize to JSON
        final String json = JsonSerdeTestUtil.toJson(serdeContext, graph);
        final JsonNode jsonNode = new ObjectMapper().readTree(json);

        // Verify JSON structure using JsonSerdeTestUtil assertions
        // Basic node structure
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "type");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "id");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "description");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "inputProperties");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "outputType");

        // MultiJoin specific fields
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "joinTypes");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "joinAttributeMap");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "inputUpsertKeys");
        JsonSerdeTestUtil.assertThatJsonContains(jsonNode, "nodes", "0", "joinConditions");

        // Verify specific field values
        JsonNode node = jsonNode.get("nodes").get(0);
        assertThat(node.get("type").asText()).isEqualTo("stream-exec-multi-join_1");
        assertThat(node.get("description").asText()).isEqualTo("test-multi-join");
        assertThat(node.get("joinTypes").isArray()).isTrue();
        assertThat(node.get("joinTypes"))
                .containsExactly(new TextNode("INNER"), new TextNode("INNER"));
        assertThat(node.get("joinAttributeMap").isObject()).isTrue();
        assertThat(node.get("inputUpsertKeys").isArray()).isTrue();
        assertThat(node.get("inputUpsertKeys")).hasSize(2);
        assertThat(node.get("joinConditions").isArray()).isTrue();
        assertThat(node.get("joinConditions")).hasSize(2);
        assertThat(node.get("inputProperties").isArray()).isTrue();
        assertThat(node.get("inputProperties")).hasSize(2);
    }

    private StreamExecMultiJoin createTestMultiJoinNode() {
        final FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE);
        final RexBuilder builder = new RexBuilder(typeFactory);
        final RelDataType varCharType =
                typeFactory.createFieldTypeFromLogicalType(new VarCharType());

        final RexNode condition =
                builder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        new RexInputRef(0, varCharType),
                        new RexInputRef(1, varCharType));

        final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap = createJoinAttributeMap();

        final var execNode =
                new StreamExecMultiJoin(
                        new Configuration(),
                        Arrays.asList(FlinkJoinType.INNER, FlinkJoinType.INNER),
                        Arrays.asList(null, condition),
                        null,
                        joinAttributeMap,
                        List.of(
                                List.of(new int[] {0}),
                                List.of(new int[] {0})), // left keys for each join
                        Collections.emptyMap(),
                        Arrays.asList(InputProperty.DEFAULT, InputProperty.DEFAULT),
                        RowType.of(VarCharType.STRING_TYPE, new IntType()),
                        "test-multi-join");

        execNode.setInputEdges(Collections.emptyList());
        return execNode;
    }

    private static Map<Integer, List<ConditionAttributeRef>> createJoinAttributeMap() {
        final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap = new HashMap<>();

        // Corresponds to a join between input 0 and 1 on their first fields.
        final List<ConditionAttributeRef> attributesForJoinWithInput1 =
                List.of(new ConditionAttributeRef(0, 0, 1, 0));
        joinAttributeMap.put(1, attributesForJoinWithInput1); // Key is the right-side input index.

        // Corresponds to a join between input 0 and 2 on their first fields.
        final List<ConditionAttributeRef> attributesForJoinWithInput2 =
                List.of(new ConditionAttributeRef(0, 0, 2, 0));
        joinAttributeMap.put(2, attributesForJoinWithInput2); // Key is the right-side input index.
        return joinAttributeMap;
    }
}
