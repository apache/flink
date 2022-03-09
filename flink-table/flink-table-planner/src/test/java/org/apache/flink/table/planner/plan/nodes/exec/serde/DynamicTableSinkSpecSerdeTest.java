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
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.CatalogManagerMocks;

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

import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.TestDynamicTableFactory.BUFFER_SIZE;
import static org.apache.flink.table.factories.TestDynamicTableFactory.TARGET;
import static org.apache.flink.table.factories.TestFormatFactory.DELIMITER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DynamicTableSourceSpecSerdeTest.tableWithOnlyPhysicalColumns;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link DynamicTableSinkSpec} serialization and deserialization. */
@Execution(CONCURRENT)
class DynamicTableSinkSpecSerdeTest {

    static Stream<DynamicTableSinkSpec> testDynamicTableSinkSpecSerde() {
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

        DynamicTableSinkSpec spec1 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable1, resolvedSchema1)),
                        null);

        Map<String, String> options2 = new HashMap<>();
        options2.put("connector", FileSystemTableFactory.IDENTIFIER);
        options2.put("format", TestCsvFormatFactory.IDENTIFIER);
        options2.put("path", "/tmp");

        final ResolvedSchema resolvedSchema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("p", DataTypes.STRING())),
                        Collections.emptyList(),
                        null);
        final CatalogTable catalogTable2 =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema2).build(),
                        null,
                        Collections.emptyList(),
                        options2);

        DynamicTableSinkSpec spec2 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable2, resolvedSchema2)),
                        Arrays.asList(
                                new OverwriteSpec(true),
                                new PartitioningSpec(
                                        new HashMap<String, String>() {
                                            {
                                                put("p", "A");
                                            }
                                        })));

        Map<String, String> options3 = new HashMap<>();
        options3.put("connector", TestValuesTableFactory.IDENTIFIER);
        options3.put("writable-metadata", "m:STRING");

        final ResolvedSchema resolvedSchema3 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.metadata("m", DataTypes.STRING(), null, false)),
                        Collections.emptyList(),
                        null);
        final CatalogTable catalogTable3 =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema3).build(),
                        null,
                        Collections.emptyList(),
                        options3);

        DynamicTableSinkSpec spec3 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable3, resolvedSchema3)),
                        Collections.singletonList(
                                new WritingMetadataSpec(
                                        Collections.singletonList("m"),
                                        RowType.of(new BigIntType(), new IntType()))));

        return Stream.of(spec1, spec2, spec3);
    }

    @ParameterizedTest
    @MethodSource("testDynamicTableSinkSpecSerde")
    void testDynamicTableSinkSpecSerde(DynamicTableSinkSpec spec) throws IOException {
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
                new DynamicTableSinkSpec(
                        ContextResolvedTable.permanent(
                                spec.getContextResolvedTable().getIdentifier(),
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                spec.getContextResolvedTable().getResolvedTable()),
                        spec.getSinkAbilities());

        String actualJson = toJson(serdeCtx, spec);
        DynamicTableSinkSpec actual = toObject(serdeCtx, actualJson, DynamicTableSinkSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(spec.getContextResolvedTable());
        assertThat(actual.getSinkAbilities()).isEqualTo(spec.getSinkAbilities());

        assertThat(actual.getTableSink(plannerMocks.getPlannerContext().getFlinkContext()))
                .isNotNull();
    }

    @Test
    void testDynamicTableSinkSpecSerdeWithEnrichmentOptions() throws Exception {
        // Test model
        ObjectIdentifier identifier =
                ObjectIdentifier.of(
                        CatalogManagerMocks.DEFAULT_CATALOG,
                        CatalogManagerMocks.DEFAULT_DATABASE,
                        "my_table");

        String formatPrefix = FactoryUtil.getFormatPrefix(FORMAT, TestFormatFactory.IDENTIFIER);

        Map<String, String> planOptions = new HashMap<>();
        planOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        planOptions.put(TARGET.key(), "abc");
        planOptions.put(BUFFER_SIZE.key(), "1000");
        planOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        planOptions.put(formatPrefix + DELIMITER.key(), "|");

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        catalogOptions.put(TARGET.key(), "xyz");
        catalogOptions.put(BUFFER_SIZE.key(), "2000");
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

        DynamicTableSinkSpec planSpec =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.permanent(
                                identifier,
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                planResolvedCatalogTable),
                        Collections.emptyList());

        String actualJson = toJson(serdeCtx, planSpec);
        DynamicTableSinkSpec actual = toObject(serdeCtx, actualJson, DynamicTableSinkSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(planSpec.getContextResolvedTable());
        assertThat(actual.getSinkAbilities()).isNull();

        TestDynamicTableFactory.DynamicTableSinkMock dynamicTableSink =
                (TestDynamicTableFactory.DynamicTableSinkMock)
                        actual.getTableSink(plannerMocks.getPlannerContext().getFlinkContext());

        assertThat(dynamicTableSink.target).isEqualTo("abc");
        assertThat(dynamicTableSink.bufferSize).isEqualTo(2000);
        assertThat(((TestFormatFactory.EncodingFormatMock) dynamicTableSink.valueFormat).delimiter)
                .isEqualTo(",");
    }
}
