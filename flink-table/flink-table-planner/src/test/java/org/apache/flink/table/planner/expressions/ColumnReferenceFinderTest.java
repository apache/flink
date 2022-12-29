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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ColumnReferenceFinder}. */
public class ColumnReferenceFinderTest {

    private final boolean isStreamingMode = true;

    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog("builtin", catalog)
                    .config(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ExecutionOptions.RUNTIME_MODE.key(),
                                            RuntimeExecutionMode.STREAMING.name())))
                    .build();
    private final PlannerMocks plannerMocks =
            PlannerMocks.newBuilder()
                    .withTableConfig(TableConfig.getDefault())
                    .withCatalogManager(catalogManager)
                    .withRootSchema(
                            asRootSchema(
                                    new CatalogManagerCalciteSchema(
                                            catalogManager, isStreamingMode)))
                    .build();

    private final PlannerContext plannerContext = plannerMocks.getPlannerContext();
    private final FunctionCatalog functionCatalog = plannerMocks.getFunctionCatalog();

    private final Supplier<FlinkPlannerImpl> plannerSupplier = plannerContext::createFlinkPlanner;

    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerContext.getRexFactory());

    private ResolvedSchema resolvedSchema;
    private List<Column> resolvedColumns;

    @BeforeEach
    public void beforeEach() {
        catalogManager.initSchemaResolver(
                true,
                ExpressionResolverMocks.basicResolver(catalogManager, functionCatalog, parser));
        catalogManager.registerCatalog("cat1", catalog);
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "default", "MyTable");
        catalogManager.createTable(
                CatalogTable.of(
                        Schema.newBuilder()
                                .columnByExpression("a", "b || '_001'")
                                .column("b", DataTypes.STRING())
                                .columnByExpression("c", "d * e + 2")
                                .column("d", DataTypes.DOUBLE())
                                .columnByMetadata("e", DataTypes.INT(), null, true)
                                .column(
                                        "tuple",
                                        DataTypes.ROW(DataTypes.TIMESTAMP(3), DataTypes.INT()))
                                .columnByExpression("ts", "tuple.f0")
                                .watermark("ts", "ts - interval '5' day")
                                .build(),
                        "comment",
                        Collections.emptyList(),
                        Collections.singletonMap("foo", "bar")),
                tableIdentifier,
                false);
        resolvedSchema = catalogManager.getTable(tableIdentifier).get().getResolvedSchema();
        resolvedColumns = resolvedSchema.getColumns();
    }

    @Test
    public void testFindReferencedColumn() {
        assertThat(
                        ColumnReferenceFinder.findReferencedColumn(
                                getComputedColumn(0), resolvedColumns, false))
                .containsExactlyInAnyOrder("b");

        assertThat(
                        ColumnReferenceFinder.findReferencedColumn(
                                getComputedColumn(2), resolvedColumns, false))
                .containsExactlyInAnyOrder("d", "e");

        assertThat(
                        ColumnReferenceFinder.findReferencedColumn(
                                getComputedColumn(6), resolvedColumns, false))
                .containsExactlyInAnyOrder("tuple");

        assertThat(
                        ColumnReferenceFinder.findReferencedColumn(
                                getWatermark(), resolvedColumns, true))
                .containsExactlyInAnyOrder("ts");
    }

    private ResolvedExpression getComputedColumn(int idx) {
        Column column = resolvedColumns.get(idx);
        assertThat(column).isInstanceOf(Column.ComputedColumn.class);
        return ((Column.ComputedColumn) column).getExpression();
    }

    private ResolvedExpression getWatermark() {
        return resolvedSchema.getWatermarkSpecs().get(0).getWatermarkExpression();
    }
}
