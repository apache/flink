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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.RexNodeExpression;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonContains;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonDoesNotContain;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectReader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link ResolvedCatalogTable} serialization and deserialization. */
@Execution(CONCURRENT)
class ResolvedCatalogTableSerdeTest {

    private static final Map<String, String> OPTIONS = new HashMap<>();

    static {
        OPTIONS.put("a", "1");
        OPTIONS.put("b", "2");
        OPTIONS.put("c", "3");
    }

    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();
    private static final RexBuilder REX_BUILDER = new RexBuilder(FACTORY);

    private static final RexNode REX_NODE =
            REX_BUILDER.makeInputRef(FACTORY.createSqlType(SqlTypeName.TIMESTAMP), 1);
    private static final RexNodeExpression REX_NODE_EXPRESSION =
            new RexNodeExpression(REX_NODE, DataTypes.TIMESTAMP().notNull(), "$1", "$1");
    private static final ResolvedSchema FULL_RESOLVED_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.STRING()),
                            Column.physical("b", DataTypes.INT()),
                            Column.physical("c", DataTypes.BOOLEAN()),
                            Column.metadata("d", DataTypes.DOUBLE(), "d", true),
                            Column.metadata("e", DataTypes.DOUBLE(), null, false),
                            Column.computed("f", REX_NODE_EXPRESSION)),
                    Collections.singletonList(WatermarkSpec.of("b", REX_NODE_EXPRESSION)),
                    UniqueConstraint.primaryKey("myPrimaryKey", Arrays.asList("a", "c")));

    private static final ResolvedCatalogTable FULL_RESOLVED_CATALOG_TABLE =
            new ResolvedCatalogTable(
                    CatalogTable.of(
                            Schema.newBuilder().fromResolvedSchema(FULL_RESOLVED_SCHEMA).build(),
                            "my table",
                            Collections.singletonList("c"),
                            OPTIONS),
                    FULL_RESOLVED_SCHEMA);

    static Stream<ResolvedCatalogTable> testResolvedCatalogTableSerde() {
        ResolvedSchema withoutPartitionKey =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.STRING()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("c", DataTypes.BOOLEAN()),
                                Column.metadata("d", DataTypes.DOUBLE(), "d", true),
                                Column.metadata("e", DataTypes.DOUBLE(), null, false),
                                Column.computed("f", REX_NODE_EXPRESSION)),
                        Collections.emptyList(),
                        null);

        return Stream.of(
                FULL_RESOLVED_CATALOG_TABLE,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(withoutPartitionKey).build(),
                                null,
                                Collections.singletonList("c"),
                                OPTIONS),
                        withoutPartitionKey));
    }

    @ParameterizedTest
    @MethodSource("testResolvedCatalogTableSerde")
    void testResolvedCatalogTableSerde(ResolvedCatalogTable spec) throws IOException {
        testJsonRoundTrip(spec, ResolvedCatalogTable.class);
    }

    @Test
    void testDontSerializeOptions() throws IOException {
        SerdeContext serdeCtx = configuredSerdeContext();

        byte[] actualSerialized =
                JsonSerdeUtil.createObjectWriter(serdeCtx)
                        .withAttribute(ResolvedCatalogTableJsonSerializer.SERIALIZE_OPTIONS, false)
                        .writeValueAsBytes(FULL_RESOLVED_CATALOG_TABLE);

        final ObjectReader objectReader = createObjectReader(serdeCtx);
        JsonNode actualJson = objectReader.readTree(actualSerialized);
        assertThatJsonContains(actualJson, ResolvedCatalogTableJsonSerializer.RESOLVED_SCHEMA);
        assertThatJsonContains(actualJson, ResolvedCatalogTableJsonSerializer.PARTITION_KEYS);
        assertThatJsonDoesNotContain(actualJson, ResolvedCatalogTableJsonSerializer.OPTIONS);
        assertThatJsonDoesNotContain(actualJson, ResolvedCatalogTableJsonSerializer.COMMENT);

        ResolvedCatalogTable actual =
                objectReader.readValue(actualSerialized, ResolvedCatalogTable.class);

        assertThat(actual)
                .isEqualTo(
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(FULL_RESOLVED_SCHEMA)
                                                .build(),
                                        null,
                                        Collections.singletonList("c"),
                                        Collections.emptyMap()),
                                FULL_RESOLVED_SCHEMA));
    }

    @Test
    void testDontSerializeExternalInlineTable() {
        SerdeContext serdeCtx = configuredSerdeContext();
        ObjectWriter objectWriter = JsonSerdeUtil.createObjectWriter(serdeCtx);

        assertThatThrownBy(
                        () ->
                                objectWriter.writeValueAsString(
                                        new ResolvedCatalogTable(
                                                new ExternalCatalogTable(
                                                        Schema.newBuilder()
                                                                .fromResolvedSchema(
                                                                        FULL_RESOLVED_SCHEMA)
                                                                .build()),
                                                FULL_RESOLVED_SCHEMA)))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TableException.class,
                                "Cannot serialize the table as it's an external inline table"));
    }
}
