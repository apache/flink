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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.expressions.RexNodeExpression;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link ResolvedSchema} serialization and deserialization. */
@Execution(CONCURRENT)
class ResolvedSchemaJsonSerdeTest {
    private static final FlinkTypeFactory FACTORY =
            new FlinkTypeFactory(
                    ResolvedSchemaJsonSerdeTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);
    private static final RexBuilder REX_BUILDER = new RexBuilder(FACTORY);

    private static final RexNode REX_NODE =
            REX_BUILDER.makeInputRef(FACTORY.createSqlType(SqlTypeName.TIMESTAMP), 1);
    private static final RexNodeExpression REX_NODE_EXPRESSION =
            new RexNodeExpression(REX_NODE, DataTypes.TIMESTAMP().notNull(), "$1", "$1");
    private static final ResolvedSchema FULL_RESOLVED_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.STRING()),
                            Column.physical("b", DataTypes.INT().notNull()),
                            Column.physical("c", DataTypes.BOOLEAN()),
                            Column.metadata("d", DataTypes.DOUBLE(), "d", true),
                            Column.metadata("e", DataTypes.DOUBLE(), null, false),
                            Column.computed("f", REX_NODE_EXPRESSION)),
                    Collections.singletonList(WatermarkSpec.of("b", REX_NODE_EXPRESSION)),
                    UniqueConstraint.primaryKey("myPrimaryKey", Arrays.asList("a", "c")),
                    Collections.singletonList(
                            DefaultIndex.newIndex("idx", Collections.singletonList("b"))));

    static Stream<ResolvedSchema> testResolvedSchemaJsonSerde() {
        return Stream.of(
                FULL_RESOLVED_SCHEMA,
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.STRING()),
                                Column.physical("b", DataTypes.INT().notNull()),
                                Column.physical("c", DataTypes.BOOLEAN()),
                                Column.metadata("d", DataTypes.DOUBLE(), "d", true),
                                Column.metadata("e", DataTypes.DOUBLE(), null, false),
                                Column.computed("f", REX_NODE_EXPRESSION)),
                        Collections.emptyList(),
                        null,
                        Collections.emptyList()));
    }

    @ParameterizedTest
    @MethodSource("testResolvedSchemaJsonSerde")
    void testResolvedSchemaJsonSerde(ResolvedSchema spec) throws IOException {
        testJsonRoundTrip(spec, ResolvedSchema.class);
    }
}
