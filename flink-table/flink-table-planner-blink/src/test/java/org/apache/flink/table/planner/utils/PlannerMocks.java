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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.CatalogTableSchemaResolver;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.utils.CatalogManagerMocks;

import java.util.ArrayList;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/** A utility class for creating an instance of {@link FlinkPlannerImpl} for testing. */
public class PlannerMocks {
    public static FlinkPlannerImpl createDefaultPlanner() {
        final boolean isStreamingMode = false;
        TableConfig tableConfig = new TableConfig();
        CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        ModuleManager moduleManager = new ModuleManager();
        FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, catalogManager, moduleManager);
        PlannerContext plannerContext =
                new PlannerContext(
                        tableConfig,
                        functionCatalog,
                        catalogManager,
                        asRootSchema(
                                new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                        new ArrayList<>());
        FlinkPlannerImpl planner =
                plannerContext.createFlinkPlanner(
                        catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
        Parser parser =
                new ParserImpl(
                        catalogManager,
                        () -> planner,
                        planner::parser,
                        t ->
                                plannerContext.createSqlExprToRexConverter(
                                        plannerContext.getTypeFactory().buildRelNodeRowType(t)));
        catalogManager.setCatalogTableSchemaResolver(
                new CatalogTableSchemaResolver(parser, isStreamingMode));
        return planner;
    }

    private PlannerMocks() {}
}
