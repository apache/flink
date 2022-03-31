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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.formats.testcsv.TestCsvFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.LimitPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceWatermarkSpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.TestDynamicTableFactory.PASSWORD;
import static org.apache.flink.table.factories.TestDynamicTableFactory.TARGET;
import static org.apache.flink.table.factories.TestFormatFactory.DELIMITER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link DynamicTableSourceSpec} serialization and deserialization. */
@Execution(CONCURRENT)
public class DynamicTableSourceSpecSerdeTest {

    public static Stream<DynamicTableSourceSpec> testDynamicTableSinkSpecSerde() {
        Map<String, String> options1 = new HashMap<>();
        options1.put("connector", FileSystemTableFactory.IDENTIFIER);
        options1.put("format", TestCsvFormatFactory.IDENTIFIER);
        options1.put("path", "/tmp");

        final ResolvedSchema resolvedSchema1 =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("a", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        null);

        final CatalogTable catalogTable1 =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema1).build(),
                        null,
                        Collections.emptyList(),
                        options1);

        DynamicTableSourceSpec spec1 =
                new DynamicTableSourceSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        TableConfigOptions.TABLE_CATALOG_NAME.defaultValue(),
                                        TableConfigOptions.TABLE_DATABASE_NAME.defaultValue(),
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable1, resolvedSchema1)),
                        null);

        Map<String, String> options2 = new HashMap<>();
        options2.put("connector", TestValuesTableFactory.IDENTIFIER);
        options2.put("disable-lookup", "true");
        options2.put("enable-watermark-push-down", "true");
        options2.put("filterable-fields", "b");
        options2.put("bounded", "false");
        options2.put("readable-metadata", "m1:INT, m2:STRING");

        final ResolvedSchema resolvedSchema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("c", DataTypes.STRING()),
                                Column.physical("p", DataTypes.STRING()),
                                Column.metadata("m1", DataTypes.INT(), null, false),
                                Column.metadata("m2", DataTypes.STRING(), null, false),
                                Column.physical("ts", DataTypes.TIMESTAMP(3))),
                        Collections.emptyList(),
                        null);

        final CatalogTable catalogTable2 =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema2).build(),
                        null,
                        Collections.emptyList(),
                        options2);

        FlinkTypeFactory factory = FlinkTypeFactory.INSTANCE();
        RexBuilder rexBuilder = new RexBuilder(factory);
        DynamicTableSourceSpec spec2 =
                new DynamicTableSourceSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        TableConfigOptions.TABLE_CATALOG_NAME.defaultValue(),
                                        TableConfigOptions.TABLE_DATABASE_NAME.defaultValue(),
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable2, resolvedSchema2)),
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
                                                                2,
                                                                TimeUnit.SECOND,
                                                                6,
                                                                SqlParserPos.ZERO))),
                                        5000,
                                        RowType.of(
                                                new BigIntType(),
                                                new IntType(),
                                                new IntType(),
                                                new TimestampType(
                                                        false, TimestampKind.ROWTIME, 3))),
                                new SourceWatermarkSpec(
                                        true,
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
        return Stream.of(spec1, spec2);
    }

    @ParameterizedTest
    @MethodSource("testDynamicTableSinkSpecSerde")
    void testDynamicTableSourceSpecSerde(DynamicTableSourceSpec spec) throws IOException {
        PlannerMocks plannerMocks = PlannerMocks.create();

        CatalogManager catalogManager = plannerMocks.getCatalogManager();
        catalogManager.createTable(
                spec.getContextResolvedTable().getResolvedTable(),
                spec.getContextResolvedTable().getIdentifier(),
                false);

        SerdeContext serdeCtx =
                configuredSerdeContext(catalogManager, plannerMocks.getTableConfig());

        // Re-init the spec to be permanent with correct catalog
        spec =
                new DynamicTableSourceSpec(
                        ContextResolvedTable.permanent(
                                spec.getContextResolvedTable().getIdentifier(),
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                spec.getContextResolvedTable().getResolvedTable()),
                        spec.getSourceAbilities());

        String actualJson = toJson(serdeCtx, spec);
        DynamicTableSourceSpec actual =
                toObject(serdeCtx, actualJson, DynamicTableSourceSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(spec.getContextResolvedTable());
        assertThat(actual.getSourceAbilities()).isEqualTo(spec.getSourceAbilities());

        assertThat(actual.getScanTableSource(plannerMocks.getPlannerContext().getFlinkContext()))
                .isNotNull();
    }

    @Test
    void testDynamicTableSourceSpecSerdeWithEnrichmentOptions() throws Exception {
        // Test model
        ObjectIdentifier identifier =
                ObjectIdentifier.of(
                        TableConfigOptions.TABLE_CATALOG_NAME.defaultValue(),
                        TableConfigOptions.TABLE_DATABASE_NAME.defaultValue(),
                        "my_table");

        String formatPrefix = FactoryUtil.getFormatPrefix(FORMAT, TestFormatFactory.IDENTIFIER);

        Map<String, String> planOptions = new HashMap<>();
        planOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        planOptions.put(TARGET.key(), "abc");
        planOptions.put(PASSWORD.key(), "abc");
        planOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        planOptions.put(formatPrefix + DELIMITER.key(), "|");

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        catalogOptions.put(TARGET.key(), "abc");
        catalogOptions.put(PASSWORD.key(), "xyz");
        catalogOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        catalogOptions.put(formatPrefix + DELIMITER.key(), ",");

        ResolvedCatalogTable planResolvedCatalogTable = tableWithOnlyPhysicalColumns(planOptions);
        ResolvedCatalogTable catalogResolvedCatalogTable =
                tableWithOnlyPhysicalColumns(catalogOptions);

        // Create planner mocks
        PlannerMocks plannerMocks =
                PlannerMocks.create(
                        new Configuration()
                                .set(PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.ALL)
                                .set(PLAN_COMPILE_CATALOG_OBJECTS, CatalogPlanCompilation.ALL));

        CatalogManager catalogManager = plannerMocks.getCatalogManager();
        catalogManager.createTable(catalogResolvedCatalogTable, identifier, false);

        // Mock the context
        SerdeContext serdeCtx =
                configuredSerdeContext(catalogManager, plannerMocks.getTableConfig());

        DynamicTableSourceSpec planSpec =
                new DynamicTableSourceSpec(
                        ContextResolvedTable.permanent(
                                identifier,
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                planResolvedCatalogTable),
                        Collections.emptyList());

        String actualJson = toJson(serdeCtx, planSpec);
        DynamicTableSourceSpec actual =
                toObject(serdeCtx, actualJson, DynamicTableSourceSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(planSpec.getContextResolvedTable());
        assertThat(actual.getSourceAbilities()).isNull();

        TestDynamicTableFactory.DynamicTableSourceMock dynamicTableSource =
                (TestDynamicTableFactory.DynamicTableSourceMock)
                        actual.getScanTableSource(
                                plannerMocks.getPlannerContext().getFlinkContext());

        assertThat(dynamicTableSource.password).isEqualTo("xyz");
        assertThat(
                        ((TestFormatFactory.DecodingFormatMock) dynamicTableSource.valueFormat)
                                .delimiter)
                .isEqualTo(",");
    }

    static ResolvedCatalogTable tableWithOnlyPhysicalColumns(Map<String, String> options) {
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.STRING()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("c", DataTypes.BOOLEAN())),
                        Collections.emptyList(),
                        null);

        return new ResolvedCatalogTable(
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        null,
                        Collections.emptyList(),
                        options),
                resolvedSchema);
    }
}
