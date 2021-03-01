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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.calcite.tools.FrameworkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** A Parser that uses Hive's planner to parse a statement. */
public class HiveParser extends ParserImpl {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);

    private final PlannerContext plannerContext;
    private final FlinkCalciteCatalogReader catalogReader;
    private final FrameworkConfig frameworkConfig;

    HiveParser(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator,
            PlannerContext plannerContext) {
        super(
                catalogManager,
                validatorSupplier,
                calciteParserSupplier,
                sqlExprToRexConverterCreator);
        this.plannerContext = plannerContext;
        this.catalogReader =
                plannerContext.createCatalogReader(
                        false,
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase());
        this.frameworkConfig = plannerContext.createFrameworkConfig();
    }

    @Override
    public List<Operation> parse(String statement) {
        return super.parse(statement);
    }
}
