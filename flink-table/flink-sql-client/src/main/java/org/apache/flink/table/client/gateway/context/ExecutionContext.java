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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.MutableURLClassLoader;

import java.lang.reflect.Method;

import static org.apache.flink.table.client.gateway.context.SessionContext.SessionState;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as it
 * might be reused across different query submissions.
 */
public class ExecutionContext {

    // TODO: merge the ExecutionContext into the SessionContext.
    // Members that should be reused in the same session.
    private final Configuration flinkConfig;
    private final SessionState sessionState;
    private final MutableURLClassLoader classLoader;

    private final StreamTableEnvironment tableEnv;

    public ExecutionContext(
            Configuration flinkConfig,
            MutableURLClassLoader classLoader,
            SessionState sessionState) {
        this.flinkConfig = flinkConfig;
        this.sessionState = sessionState;
        this.classLoader = classLoader;
        this.tableEnv = createTableEnvironment();
    }

    /**
     * Create a new {@link ExecutionContext}.
     *
     * <p>It just copies from the {@link ExecutionContext} and rebuild a new {@link
     * TableEnvironment}.
     */
    public ExecutionContext(ExecutionContext context) {
        this.flinkConfig = context.flinkConfig;
        this.sessionState = context.sessionState;
        this.classLoader = context.classLoader;

        this.tableEnv = createTableEnvironment();
    }

    public StreamTableEnvironment getTableEnvironment() {
        return tableEnv;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helper to create Table Environment
    // ------------------------------------------------------------------------------------------------------------------

    private StreamTableEnvironment createTableEnvironment() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(flinkConfig).build();

        // We need not different StreamExecutionEnvironments to build and submit flink job,
        // instead we just use StreamExecutionEnvironment#executeAsync(StreamGraph) method
        // to execute existing StreamGraph.
        // This requires StreamExecutionEnvironment to have a full flink configuration.
        StreamExecutionEnvironment streamExecEnv =
                new StreamExecutionEnvironment(new Configuration(flinkConfig), classLoader);

        final Executor executor = lookupExecutor(streamExecEnv, classLoader);

        return createStreamTableEnvironment(
                streamExecEnv,
                settings,
                executor,
                sessionState.catalogManager,
                sessionState.moduleManager,
                sessionState.resourceManager,
                sessionState.functionCatalog,
                classLoader);
    }

    private static StreamTableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog,
            ClassLoader userClassLoader) {

        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                tableConfig,
                env,
                planner,
                executor,
                settings.isStreamingMode());
    }

    private static Executor lookupExecutor(
            StreamExecutionEnvironment executionEnvironment, ClassLoader userClassLoader) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            userClassLoader,
                            ExecutorFactory.class,
                            ExecutorFactory.DEFAULT_IDENTIFIER);
            final Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }
}
