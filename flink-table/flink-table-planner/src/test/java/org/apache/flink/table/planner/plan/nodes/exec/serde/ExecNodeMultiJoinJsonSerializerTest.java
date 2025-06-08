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
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

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

class ExecNodeMultiJoinJsonSerializerTest {
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    @Test
    void testSerializingStreamExecMultiJoin() throws IOException {
        final ObjectWriter objectWriter =
                CompiledPlanSerdeUtil.createJsonObjectWriter(
                        JsonSerdeTestUtil.configuredSerdeContext());

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

        final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                joinAttributeMap = createJoinAttributeMap();
        final var execNode =
                new StreamExecMultiJoin(
                        new Configuration(),
                        Arrays.asList(JoinType.INNER, JoinType.INNER),
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

        // Serialize the exec node
        final String serialized =
                objectWriter.writeValueAsString(
                        new ExecNodeGraph(FlinkVersion.v2_1, Collections.singletonList(execNode)));

        // Verify the serialized content contains expected fields
        assertThat(serialized)
                .contains("\"type\":\"stream-exec-multi-join_1\"")
                .contains("\"joinTypes\":[\"INNER\",\"INNER\"]")
                .contains("\"description\":\"test-multi-join\"")
                .contains(
                        "\"1\":[{\"leftInputId\":0,\"leftFieldIndex\":0,\"rightInputId\":1,\"rightFieldIndex\":0}]")
                .contains(
                        "\"2\":[{\"leftInputId\":0,\"leftFieldIndex\":0,\"rightInputId\":2,\"rightFieldIndex\":0}]");

        // Deserialize back
        final ObjectReader objectReader =
                CompiledPlanSerdeUtil.createJsonObjectReader(
                        JsonSerdeTestUtil.configuredSerdeContext());
        final ExecNodeGraph deserializedGraph =
                objectReader.readValue(serialized, ExecNodeGraph.class);

        // Some basic checks to verify the deserialized node matches the original
        assertThat(deserializedGraph.getRootNodes()).hasSize(1);
        final StreamExecMultiJoin deserializedMultiJoinNode =
                (StreamExecMultiJoin) deserializedGraph.getRootNodes().get(0);
        assertThat(deserializedMultiJoinNode.getDescription()).isEqualTo("test-multi-join");
        assertThat(deserializedMultiJoinNode.getOutputType())
                .isEqualTo(RowType.of(VarCharType.STRING_TYPE, new IntType()));
    }

    private static Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
            createJoinAttributeMap() {
        final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                joinAttributeMap = new HashMap<>();

        // Corresponds to a join between input 0 and 1 on their first fields.
        final List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>
                attributesForJoinWithInput1 =
                        List.of(
                                new AttributeBasedJoinKeyExtractor.ConditionAttributeRef(
                                        0, 0, 1, 0));
        joinAttributeMap.put(1, attributesForJoinWithInput1); // Key is the right-side input index.

        // Corresponds to a join between input 0 and 2 on their first fields.
        final List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>
                attributesForJoinWithInput2 =
                        List.of(
                                new AttributeBasedJoinKeyExtractor.ConditionAttributeRef(
                                        0, 0, 2, 0));
        joinAttributeMap.put(2, attributesForJoinWithInput2); // Key is the right-side input index.
        return joinAttributeMap;
    }
}
