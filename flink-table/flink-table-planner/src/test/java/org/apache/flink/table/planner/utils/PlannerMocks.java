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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * A utility class for instantiating and holding mocks for {@link FlinkPlannerImpl}, {@link
 * ParserImpl} and {@link CatalogManager} for testing.
 */
public class PlannerMocks {

    private final FlinkPlannerImpl planner;
    private final ParserImpl parser;
    private final CatalogManager catalogManager;
    private final FunctionCatalog functionCatalog;
    private final TableConfig tableConfig;
    private final PlannerContext plannerContext;

    @SuppressWarnings("rawtypes")
    private PlannerMocks(
            boolean isBatchMode,
            TableConfig tableConfig,
            ResourceManager resourceManager,
            CatalogManager catalogManager,
            List<RelTraitDef> traitDefs,
            CalciteSchema rootSchema) {
        this.catalogManager = catalogManager;
        this.tableConfig = tableConfig;

        final ModuleManager moduleManager = new ModuleManager();

        this.functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);

        this.plannerContext =
                new PlannerContext(
                        isBatchMode,
                        tableConfig,
                        moduleManager,
                        functionCatalog,
                        catalogManager,
                        rootSchema != null
                                ? rootSchema
                                : asRootSchema(
                                        new CatalogManagerCalciteSchema(
                                                catalogManager, !isBatchMode)),
                        traitDefs,
                        PlannerMocks.class.getClassLoader());

        this.planner = plannerContext.createFlinkPlanner();
        this.parser =
                new ParserImpl(
                        catalogManager,
                        () -> planner,
                        planner::parser,
                        plannerContext.getRexFactory());

        catalogManager.initSchemaResolver(
                true,
                ExpressionResolver.resolverFor(
                        tableConfig,
                        PlannerMocks.class.getClassLoader(),
                        name -> {
                            throw new UnsupportedOperationException();
                        },
                        functionCatalog.asLookup(parser::parseIdentifier),
                        catalogManager.getDataTypeFactory(),
                        parser::parseSqlExpression));
    }

    public FlinkPlannerImpl getPlanner() {
        return planner;
    }

    public ParserImpl getParser() {
        return parser;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public FunctionCatalog getFunctionCatalog() {
        return functionCatalog;
    }

    public TableConfig getTableConfig() {
        return tableConfig;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public PlannerMocks registerTemporaryTable(String tableName, Schema tableSchema) {
        final CatalogTable table =
                CatalogTable.of(tableSchema, null, Collections.emptyList(), Collections.emptyMap());

        this.getCatalogManager()
                .createTemporaryTable(
                        table,
                        ObjectIdentifier.of(
                                this.getCatalogManager().getCurrentCatalog(),
                                this.getCatalogManager().getCurrentDatabase(),
                                tableName),
                        false);

        return this;
    }

    /** Builder for {@link PlannerMocks} to facilitate various test use cases. */
    @SuppressWarnings("rawtypes")
    public static class Builder {

        private boolean batchMode = false;
        private TableConfig tableConfig = TableConfig.getDefault();
        private CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        private ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0],
                        Thread.currentThread().getContextClassLoader(),
                        tableConfig.getConfiguration());
        private List<RelTraitDef> traitDefs = Collections.emptyList();
        private CalciteSchema rootSchema;

        private Builder() {}

        public Builder withBatchMode(boolean batchMode) {
            this.batchMode = batchMode;
            return this;
        }

        public Builder withTableConfig(TableConfig tableConfig) {
            this.tableConfig = tableConfig;
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            tableConfig.addConfiguration(configuration);
            return this;
        }

        public Builder withResourceManager(ResourceManager resourceManager) {
            this.resourceManager = resourceManager;
            return this;
        }

        public Builder withCatalogManager(CatalogManager catalogManager) {
            this.catalogManager = catalogManager;
            return this;
        }

        public Builder withTraitDefs(List<RelTraitDef> traitDefs) {
            this.traitDefs = traitDefs;
            return this;
        }

        public Builder withRootSchema(CalciteSchema rootSchema) {
            this.rootSchema = rootSchema;
            return this;
        }

        public PlannerMocks build() {
            return new PlannerMocks(
                    batchMode, tableConfig, resourceManager, catalogManager, traitDefs, rootSchema);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static PlannerMocks create() {
        return new Builder().build();
    }

    public static PlannerMocks create(boolean batchMode) {
        return new Builder().withBatchMode(batchMode).build();
    }

    public static PlannerMocks create(Configuration configuration) {
        return new Builder().withConfiguration(configuration).build();
    }
}
