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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.bridge.CalciteContext;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.FunctionCatalog;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/** A context for default dialect with calcite related interfaces. */
@Internal
public class DefaultCalciteContext implements CalciteContext {

    private final CatalogManager catalogManager;
    private final PlannerContext plannerContext;

    public DefaultCalciteContext(CatalogManager catalogManager, PlannerContext plannerContext) {
        this.catalogManager = catalogManager;
        this.plannerContext = plannerContext;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Override
    public CatalogRegistry getCatalogRegistry() {
        return catalogManager;
    }

    @Override
    public CalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity) {
        return plannerContext.createCatalogReader(lenientCaseSensitivity);
    }

    @Override
    public RelOptCluster getCluster() {
        return plannerContext.getCluster();
    }

    @Override
    public FrameworkConfig createFrameworkConfig() {
        return plannerContext.createFrameworkConfig();
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return plannerContext.getTypeFactory();
    }

    @Override
    public RelBuilder createRelBuilder() {
        return plannerContext.createRelBuilder();
    }

    @Override
    public TableConfig getTableConfig() {
        return plannerContext.getFlinkContext().getTableConfig();
    }

    @Override
    public ClassLoader getClassLoader() {
        return plannerContext.getFlinkContext().getClassLoader();
    }

    @Override
    public FunctionCatalog getFunctionCatalog() {
        return plannerContext.getFlinkContext().getFunctionCatalog();
    }

    @Override
    public RelOptTable.ToRelContext createToRelContext() {
        return plannerContext.createFlinkPlanner().createToRelContext();
    }
}
