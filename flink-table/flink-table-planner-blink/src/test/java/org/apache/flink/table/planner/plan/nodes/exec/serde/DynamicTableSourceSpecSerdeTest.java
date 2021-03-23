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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.LimitPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests for {@link DynamicTableSourceSpec} serialization and deserialization. */
@RunWith(Parameterized.class)
public class DynamicTableSourceSpecSerdeTest {

    @Parameterized.Parameter public DynamicTableSourceSpec spec;

    @Test
    public void testDynamicTableSourceSpecSerde() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(
                                TableConfig.getDefault(),
                                null,
                                CatalogManagerMocks.createEmptyCatalogManager(),
                                null),
                        classLoader,
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);

        SimpleModule module = new SimpleModule();
        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(spec);
        }
        String json = writer.toString();
        DynamicTableSourceSpec actual = mapper.readValue(json, DynamicTableSourceSpec.class);
        assertEquals(spec, actual);
        assertNull(actual.getClassLoader());
        actual.setClassLoader(classLoader);
        assertNull(actual.getReadableConfig());
        actual.setReadableConfig(serdeCtx.getConfiguration());
        TableEnvironmentImpl tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(
                                EnvironmentSettings.newInstance()
                                        .inStreamingMode()
                                        .useBlinkPlanner()
                                        .build());
        assertNotNull(actual.getScanTableSource((PlannerBase) tableEnv.getPlanner()));
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<DynamicTableSourceSpec> testData() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("connector", "filesystem");
        properties1.put("format", "testcsv");
        properties1.put("path", "/tmp");
        properties1.put("schema.0.name", "a");
        properties1.put("schema.0.data-type", "BIGINT");

        final CatalogTable catalogTable1 = CatalogTable.fromProperties(properties1);

        final ResolvedSchema resolvedSchema1 =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("a", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        null);

        DynamicTableSourceSpec spec1 =
                new DynamicTableSourceSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        new ResolvedCatalogTable(catalogTable1, resolvedSchema1),
                        Collections.emptyList());

        Map<String, String> properties2 = new HashMap<>();
        properties2.put("connector", "values");
        properties2.put("disable-lookup", "true");
        properties2.put("enable-watermark-push-down", "true");
        properties2.put("filterable-fields", "b");
        properties2.put("bounded", "false");
        properties2.put("schema.0.name", "a");
        properties2.put("schema.0.data-type", "BIGINT");
        properties2.put("schema.1.name", "b");
        properties2.put("schema.1.data-type", "INT");
        properties2.put("schema.2.name", "c");
        properties2.put("schema.2.data-type", "STRING");
        properties2.put("schema.3.name", "p");
        properties2.put("schema.3.data-type", "STRING");
        properties2.put("schema.4.name", "m1");
        properties2.put("schema.4.data-type", "INT");
        properties2.put("schema.5.name", "m2");
        properties2.put("schema.5.data-type", "STRING");
        properties2.put("schema.6.name", "ts");
        properties2.put("schema.6.data-type", "TIMESTAMP(3)");
        properties2.put("readable-metadata", "m1:INT, m2:STRING");

        final CatalogTable catalogTable2 = CatalogTable.fromProperties(properties2);

        final ResolvedSchema resolvedSchema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("c", DataTypes.STRING()),
                                Column.physical("p", DataTypes.STRING()),
                                Column.physical("m1", DataTypes.INT()),
                                Column.physical("m2", DataTypes.STRING()),
                                Column.physical("ts", DataTypes.TIMESTAMP(3))),
                        Collections.emptyList(),
                        null);

        FlinkTypeFactory factory = FlinkTypeFactory.INSTANCE();
        RexBuilder rexBuilder = new RexBuilder(factory);
        DynamicTableSourceSpec spec2 =
                new DynamicTableSourceSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        new ResolvedCatalogTable(catalogTable2, resolvedSchema2),
                        Arrays.asList(
                                new ProjectPushDownSpec(
                                        new int[][] {{0}, {1}, {4}, {6}},
                                        RowType.of(
                                                new LogicalType[] {
                                                    new BigIntType(),
                                                    new IntType(),
                                                    new IntType(),
                                                    new TimestampType(3)
                                                },
                                                new String[] {"a", "b", "m1", "ts"})),
                                new ReadingMetadataSpec(
                                        Arrays.asList("m1", "m2"),
                                        RowType.of(
                                                new LogicalType[] {
                                                    new BigIntType(),
                                                    new IntType(),
                                                    new IntType(),
                                                    new TimestampType(3)
                                                },
                                                new String[] {"a", "b", "m1", "ts"})),
                                new FilterPushDownSpec(
                                        Collections.singletonList(
                                                // b >= 10
                                                rexBuilder.makeCall(
                                                        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                                        rexBuilder.makeInputRef(
                                                                factory.createSqlType(
                                                                        SqlTypeName.INTEGER),
                                                                1),
                                                        rexBuilder.makeExactLiteral(
                                                                new BigDecimal(10))))),
                                new WatermarkPushDownSpec(
                                        rexBuilder.makeCall(
                                                SqlStdOperatorTable.MINUS,
                                                rexBuilder.makeInputRef(
                                                        factory.createSqlType(
                                                                SqlTypeName.TIMESTAMP, 3),
                                                        3),
                                                rexBuilder.makeIntervalLiteral(
                                                        BigDecimal.valueOf(1000),
                                                        new SqlIntervalQualifier(
                                                                TimeUnit.SECOND,
                                                                TimeUnit.SECOND,
                                                                SqlParserPos.ZERO))),
                                        5000,
                                        RowType.of(
                                                new BigIntType(),
                                                new IntType(),
                                                new IntType(),
                                                new TimestampType(
                                                        false, TimestampKind.ROWTIME, 3))),
                                new LimitPushDownSpec(100),
                                new PartitionPushDownSpec(
                                        Arrays.asList(
                                                new HashMap<String, String>() {
                                                    {
                                                        put("p", "A");
                                                    }
                                                },
                                                new HashMap<String, String>() {
                                                    {
                                                        put("p", "B");
                                                    }
                                                }))));
        return Arrays.asList(spec1, spec2);
    }
}
