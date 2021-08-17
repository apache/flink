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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.factories.Factory;

/**
 * Factory that creates {@link Planner}.
 *
 * <p>This factory is used with Java's Service Provider Interfaces (SPI) for discovering. A factory
 * is called with a set of normalized properties that describe the desired configuration. Those
 * properties may include execution configurations such as watermark interval, max parallelism etc.,
 * table specific initialization configuration such as if the queries should be executed in batch
 * mode.
 */
@Internal
public interface PlannerFactory extends Factory {

    /** {@link #factoryIdentifier()} for the default {@link Planner}. */
    String DEFAULT_IDENTIFIER = "default";

    /** Creates a corresponding {@link Planner}. */
    Planner create(Context context);

    /** Context used when creating a planner. */
    interface Context {
        /** The executor required by the planner. */
        Executor getExecutor();

        /** The configuration of the planner to use. */
        TableConfig getTableConfig();

        /** The catalog manager to look up tables and views. */
        CatalogManager getCatalogManager();

        /** The function catalog to look up user defined functions. */
        FunctionCatalog getFunctionCatalog();
    }

    /** Default implementation of {@link Context}. */
    class DefaultPlannerContext implements Context {
        private final Executor executor;
        private final TableConfig tableConfig;
        private final CatalogManager catalogManager;
        private final FunctionCatalog functionCatalog;

        public DefaultPlannerContext(
                Executor executor,
                TableConfig tableConfig,
                CatalogManager catalogManager,
                FunctionCatalog functionCatalog) {
            this.executor = executor;
            this.tableConfig = tableConfig;
            this.catalogManager = catalogManager;
            this.functionCatalog = functionCatalog;
        }

        @Override
        public Executor getExecutor() {
            return executor;
        }

        @Override
        public TableConfig getTableConfig() {
            return tableConfig;
        }

        @Override
        public CatalogManager getCatalogManager() {
            return catalogManager;
        }

        @Override
        public FunctionCatalog getFunctionCatalog() {
            return functionCatalog;
        }
    }
}
