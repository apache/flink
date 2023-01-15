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

package org.apache.flink.table.planner.operations;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletePushDownUtils}. */
public class DeletePushDownUtilsTest {
    private final TableConfig tableConfig = TableConfig.getDefault();
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog("builtin", catalog)
                    .config(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ExecutionOptions.RUNTIME_MODE.key(),
                                            RuntimeExecutionMode.BATCH.name())))
                    .build();

    private final PlannerMocks plannerMocks =
            PlannerMocks.newBuilder()
                    .withBatchMode(true)
                    .withTableConfig(tableConfig)
                    .withCatalogManager(catalogManager)
                    .withRootSchema(
                            asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)))
                    .build();
    private final PlannerContext plannerContext = plannerMocks.getPlannerContext();
    private final CalciteParser parser = plannerContext.createCalciteParser();
    private final FlinkPlannerImpl flinkPlanner = plannerContext.createFlinkPlanner();

    @Test
    public void testGetDynamicTableSink() {
        // create a table with connector = test-update-delete
        Map<String, String> options = new HashMap<>();
        options.put("connector", "test-update-delete");
        CatalogTable catalogTable = createTestCatalogTable(options);
        ObjectIdentifier tableId = ObjectIdentifier.of("builtin", "default", "t");
        catalogManager.createTable(catalogTable, tableId, false);
        ContextResolvedTable resolvedTable =
                ContextResolvedTable.permanent(
                        tableId, catalog, catalogManager.resolveCatalogTable(catalogTable));
        LogicalTableModify tableModify = getTableModifyFromSql("DELETE FROM t");
        Optional<DynamicTableSink> optionalDynamicTableSink =
                DeletePushDownUtils.getDynamicTableSink(resolvedTable, tableModify, catalogManager);
        // verify we can get the dynamic table sink
        assertThat(optionalDynamicTableSink).isPresent();
        assertThat(optionalDynamicTableSink.get())
                .isInstanceOf(TestUpdateDeleteTableFactory.SupportsDeletePushDownSink.class);

        // create table with connector = COLLECTION, it's legacy table sink
        options.put("connector", "COLLECTION");
        catalogTable = createTestCatalogTable(options);
        tableId = ObjectIdentifier.of("builtin", "default", "t1");
        catalogManager.createTable(catalogTable, tableId, false);
        resolvedTable =
                ContextResolvedTable.permanent(
                        tableId, catalog, catalogManager.resolveCatalogTable(catalogTable));
        tableModify = getTableModifyFromSql("DELETE FROM t1");
        optionalDynamicTableSink =
                DeletePushDownUtils.getDynamicTableSink(resolvedTable, tableModify, catalogManager);
        // verify it should be empty since it's not an instance of DynamicTableSink but is legacy
        // TableSink
        assertThat(optionalDynamicTableSink).isEmpty();
    }

    @Test
    public void testGetResolveFilterExpressions() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder()
                                .column("f0", DataTypes.INT().notNull())
                                .column("f1", DataTypes.STRING().nullable())
                                .column("f2", DataTypes.BIGINT().nullable())
                                .build(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap());
        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "t"), false);

        // verify there's no where clause
        LogicalTableModify tableModify = getTableModifyFromSql("DELETE FROM t");
        Optional<List<ResolvedExpression>> optionalResolvedExpressions =
                DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
        verifyExpression(optionalResolvedExpressions, "[]");

        tableModify = getTableModifyFromSql("DELETE FROM t where f0 = 1 and f1 = '123'");
        optionalResolvedExpressions = DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
        verifyExpression(optionalResolvedExpressions, "[equals(f0, 1), equals(f1, '123')]");

        tableModify = getTableModifyFromSql("DELETE FROM t where f0 = 1 + 6 and f0 < 6");
        optionalResolvedExpressions = DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
        assertThat(optionalResolvedExpressions).isPresent();
        verifyExpression(optionalResolvedExpressions, "[false]");

        tableModify = getTableModifyFromSql("DELETE FROM t where f0 = f2 + 1");
        optionalResolvedExpressions = DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
        verifyExpression(
                optionalResolvedExpressions, "[equals(cast(f0, BIGINT NOT NULL), plus(f2, 1))]");

        // resolve filters is not available as it contains sub-query
        tableModify = getTableModifyFromSql("DELETE FROM t where f0 > (select count(1) from t)");
        optionalResolvedExpressions = DeletePushDownUtils.getResolvedFilterExpressions(tableModify);
        assertThat(optionalResolvedExpressions).isEmpty();
    }

    private CatalogTable createTestCatalogTable(Map<String, String> options) {
        return CatalogTable.of(
                Schema.newBuilder()
                        .column("f0", DataTypes.INT().notNull())
                        .column("f1", DataTypes.STRING().nullable())
                        .column("f2", DataTypes.BIGINT().nullable())
                        .build(),
                null,
                Collections.emptyList(),
                options);
    }

    private LogicalTableModify getTableModifyFromSql(String sql) {
        SqlNode sqlNode = parser.parse(sql);
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        RelRoot deleteRelational = flinkPlanner.rel(validated);
        return (LogicalTableModify) deleteRelational.rel;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void verifyExpression(
            Optional<List<ResolvedExpression>> optionalResolvedExpressions, String expected) {
        assertThat(optionalResolvedExpressions).isPresent();
        assertThat(optionalResolvedExpressions.get().toString()).isEqualTo(expected);
    }
}
