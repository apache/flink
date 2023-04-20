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
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/** Test base for testing convert sql statement to operation. */
public class SqlNodeToOperationConversionTestBase {
    private final boolean isStreamingMode = false;
    private final TableConfig tableConfig = TableConfig.getDefault();
    protected final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    protected final CatalogManager catalogManager =
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
                            asRootSchema(
                                    new CatalogManagerCalciteSchema(
                                            catalogManager, isStreamingMode)))
                    .build();
    private final PlannerContext plannerContext = plannerMocks.getPlannerContext();
    protected final FunctionCatalog functionCatalog = plannerMocks.getFunctionCatalog();

    private final Supplier<FlinkPlannerImpl> plannerSupplier = plannerContext::createFlinkPlanner;

    protected final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerContext.getRexFactory());

    @BeforeEach
    public void before() throws TableAlreadyExistException, DatabaseNotExistException {
        catalogManager.initSchemaResolver(
                isStreamingMode,
                ExpressionResolverMocks.basicResolver(catalogManager, functionCatalog, parser));

        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
        final Schema tableSchema =
                Schema.newBuilder()
                        .fromResolvedSchema(
                                ResolvedSchema.of(
                                        Column.physical("a", DataTypes.BIGINT()),
                                        Column.physical("b", DataTypes.VARCHAR(Integer.MAX_VALUE)),
                                        Column.physical("c", DataTypes.INT()),
                                        Column.physical("d", DataTypes.VARCHAR(Integer.MAX_VALUE))))
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put("connector", "COLLECTION");
        final CatalogTable catalogTable =
                CatalogTable.of(tableSchema, "", Collections.emptyList(), options);
        catalog.createTable(path1, catalogTable, true);
        catalog.createTable(path2, catalogTable, true);
    }

    @AfterEach
    public void after() throws TableNotExistException {
        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
        catalog.dropTable(path1, true);
        catalog.dropTable(path2, true);
    }

    protected Operation parse(String sql, FlinkPlannerImpl planner, CalciteParser parser) {
        SqlNode node = parser.parse(sql);
        return SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
    }

    protected Operation parse(String sql) {
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);
        SqlNode node = parser.parse(sql);
        return SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
    }

    protected FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createFlinkPlanner();
    }

    protected CalciteParser getParserBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createCalciteParser();
    }
}
