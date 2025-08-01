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

package org.apache.flink.table.planner.plan;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.catalog.CatalogSchemaModel;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.catalog.FlinkSchema;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.TestTableSource;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for FlinkCalciteCatalogReader. */
class FlinkCalciteCatalogReaderTest {
    private final FlinkTypeFactory typeFactory =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    private final String tableMockName = "ts";
    private final String modelMockName = "ms";

    private SchemaPlus rootSchemaPlus;
    private FlinkCalciteCatalogReader catalogReader;

    @BeforeEach
    void init() {
        rootSchemaPlus = CalciteSchema.createRootSchema(true, false).plus();
        Properties prop = new Properties();
        prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfigImpl calciteConnConfig = new CalciteConnectionConfigImpl(prop);
        catalogReader =
                new FlinkCalciteCatalogReader(
                        CalciteSchema.from(rootSchemaPlus),
                        Collections.emptyList(),
                        typeFactory,
                        calciteConnConfig);
    }

    @Test
    void testGetFlinkPreparingTableBase() {
        // Mock CatalogSchemaTable.
        final ObjectIdentifier objectIdentifier = ObjectIdentifier.of("a", "b", "c");
        final ResolvedSchema schema =
                new ResolvedSchema(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        Collections.emptyList());
        final CatalogTable catalogTable =
                ConnectorCatalogTable.source(
                        new TestTableSource(
                                true,
                                TableSchema.fromResolvedSchema(schema, DefaultSqlFactory.INSTANCE)),
                        true);
        final ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, schema);
        CatalogSchemaTable mockTable =
                new CatalogSchemaTable(
                        ContextResolvedTable.permanent(
                                objectIdentifier,
                                CatalogManagerMocks.createEmptyCatalog(),
                                resolvedCatalogTable),
                        FlinkStatistic.UNKNOWN(),
                        true);

        rootSchemaPlus.add(tableMockName, mockTable);
        Prepare.PreparingTable preparingTable =
                catalogReader.getTable(Collections.singletonList(tableMockName));
        assertThat(preparingTable).isInstanceOf(FlinkPreparingTableBase.class);
    }

    @Test
    void testGetNonFlinkPreparingTableBase() {
        Table nonFlinkTableMock = mock(Table.class);
        when(nonFlinkTableMock.getRowType(typeFactory)).thenReturn(mock(RelDataType.class));
        rootSchemaPlus.add(tableMockName, nonFlinkTableMock);
        Prepare.PreparingTable resultTable =
                catalogReader.getTable(Collections.singletonList(tableMockName));
        assertThat(resultTable).isNotInstanceOf(FlinkPreparingTableBase.class);
    }

    @ParameterizedTest(name = "Case sensitive {0}")
    @ValueSource(booleans = {true, false})
    void testGetCatalogSchemaModel(boolean caseSensitive) {
        // Mock CatalogSchemaModel
        final ResolvedCatalogModel resolvedCatalogModel = getModel();
        CatalogSchemaModel mockModel =
                new CatalogSchemaModel(
                        ContextResolvedModel.permanent(
                                ObjectIdentifier.of(modelMockName, "", ""),
                                CatalogManagerMocks.createEmptyCatalog(),
                                resolvedCatalogModel));

        MockFlinkSchema mockFlinkSchema = new MockFlinkSchema();
        mockFlinkSchema.addModel(modelMockName, mockModel);

        Properties prop = new Properties();
        prop.setProperty(
                CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                Boolean.toString(caseSensitive));
        catalogReader =
                new FlinkCalciteCatalogReader(
                        CalciteSchemaBuilder.asRootSchema(mockFlinkSchema),
                        Collections.emptyList(),
                        typeFactory,
                        new CalciteConnectionConfigImpl(prop));

        CatalogSchemaModel model = catalogReader.getModel(Collections.singletonList(modelMockName));
        assertThat(model).isInstanceOf(CatalogSchemaModel.class);

        model = catalogReader.getModel(Collections.singletonList(modelMockName.toUpperCase()));
        if (caseSensitive) {
            assertThat(model).isNull();
        } else {
            assertThat(model).isInstanceOf(CatalogSchemaModel.class);
        }

        model = catalogReader.getModel(Collections.singletonList("non_exist_model"));
        assertThat(model).isNull();
    }

    @Test
    void testGetModelFromNonFlinkSchema() {
        assertThatThrownBy(() -> catalogReader.getModel(Collections.singletonList(modelMockName)))
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining(
                        "class org.apache.calcite.jdbc.CalciteConnectionImpl$RootSchema cannot be cast to class org.apache.flink.table.planner.catalog.FlinkSchema");
    }

    private static ResolvedCatalogModel getModel() {
        final CatalogModel catalogModel =
                CatalogModel.of(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("f1", DataTypes.INT())
                                .build(),
                        org.apache.flink.table.api.Schema.newBuilder()
                                .column("label", DataTypes.STRING())
                                .build(),
                        null,
                        "some comment");

        final Column f1 = Column.physical("f1", DataTypes.INT());
        final Column label = Column.physical("label", DataTypes.STRING());
        final ResolvedSchema inputSchema = ResolvedSchema.of(f1);
        final ResolvedSchema outputSchema = ResolvedSchema.of(label);

        return ResolvedCatalogModel.of(catalogModel, inputSchema, outputSchema);
    }

    private static class MockFlinkSchema extends FlinkSchema {

        private final Map<String, CatalogSchemaModel> modelMap = new HashMap<>();

        @Override
        public Table getTable(String name) {
            return null;
        }

        @Override
        public Set<String> getTableNames() {
            return Set.of();
        }

        @Override
        public @Nullable Schema getSubSchema(String name) {
            return null;
        }

        public void addModel(String name, CatalogSchemaModel model) {
            modelMap.put(name, model);
        }

        @Override
        public CatalogSchemaModel getModel(String name) {
            return modelMap.get(name);
        }

        @Override
        public Set<String> getModelNames() {
            return modelMap.keySet();
        }

        @Override
        public FlinkSchema copy() {
            return null;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Collections.emptySet();
        }

        @Override
        public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
            return null;
        }

        @Override
        public boolean isMutable() {
            return false;
        }
    }
}
