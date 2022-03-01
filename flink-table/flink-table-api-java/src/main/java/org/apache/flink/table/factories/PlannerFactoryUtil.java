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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.delegation.PlannerFactory.Context;
import org.apache.flink.table.delegation.PlannerFactory.DefaultPlannerContext;
import org.apache.flink.table.module.ModuleManager;

/** Utility for discovering and instantiating {@link PlannerFactory}. */
@Internal
public class PlannerFactoryUtil {

    /** Discovers a planner factory and creates a planner instance. */
    public static Planner createPlanner(
            Executor executor,
            TableConfig tableConfig,
            ModuleManager moduleManager,
            CatalogManager catalogManager,
            FunctionCatalog functionCatalog) {
        final PlannerFactory plannerFactory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        PlannerFactory.class,
                        PlannerFactory.DEFAULT_IDENTIFIER);

        final Context context =
                new DefaultPlannerContext(
                        executor, tableConfig, moduleManager, catalogManager, functionCatalog);
        return plannerFactory.create(context);
    }

    private PlannerFactoryUtil() {}
}
