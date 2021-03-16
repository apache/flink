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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.SinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceSinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceTableEntry;
import org.apache.flink.table.client.config.entries.TemporalTableEntry;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.descriptors.CoreModuleDescriptorValidator;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.TemporaryClassLoaderContext;

import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Utils to initialize {@link TableEnvironment} from {@link Environment}.
 *
 * @deprecated This will be dropped in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class LegacyTableEnvironmentInitializer {

    public static void initializeSessionState(
            TableEnvironment tableEnv, Environment environment, URLClassLoader classLoader) {

        // --------------------------------------------------------------------------------------------------------------
        // Step.1 Create modules and load them into the TableEnvironment.
        // --------------------------------------------------------------------------------------------------------------
        // No need to register the modules info if already inherit from the same session.
        Map<String, Module> modules = new LinkedHashMap<>();
        environment
                .getModules()
                .forEach(
                        (name, entry) ->
                                modules.put(name, createModule(entry.asMap(), classLoader)));
        if (!modules.isEmpty()) {
            // unload core module first to respect whatever users configure
            tableEnv.unloadModule(CoreModuleDescriptorValidator.MODULE_TYPE_CORE);
            modules.forEach(tableEnv::loadModule);
        }

        // --------------------------------------------------------------------------------------------------------------
        // Step.2 create user-defined functions and temporal tables then register them.
        // --------------------------------------------------------------------------------------------------------------
        // No need to register the functions if already inherit from the same session.
        registerFunctions(tableEnv, environment, classLoader);

        // --------------------------------------------------------------------------------------------------------------
        // Step.3 Create catalogs and register them.
        // --------------------------------------------------------------------------------------------------------------
        // No need to register the catalogs if already inherit from the same session.
        initializeCatalogs(tableEnv, environment, classLoader);
    }

    private static void initializeCatalogs(
            TableEnvironment tableEnv, Environment environment, URLClassLoader classLoader) {
        // --------------------------------------------------------------------------------------------------------------
        // Step.1 Create catalogs and register them.
        // --------------------------------------------------------------------------------------------------------------

        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            environment
                    .getCatalogs()
                    .forEach(
                            (name, entry) -> {
                                Catalog catalog =
                                        createCatalog(tableEnv, name, entry.asMap(), classLoader);
                                tableEnv.registerCatalog(name, catalog);
                            });
        }

        // --------------------------------------------------------------------------------------------------------------
        // Step.2 create table sources & sinks, and register them.
        // --------------------------------------------------------------------------------------------------------------

        Map<String, TableSource<?>> tableSources = new HashMap<>();
        Map<String, TableSink<?>> tableSinks = new HashMap<>();
        environment
                .getTables()
                .forEach(
                        (name, entry) -> {
                            if (entry instanceof SourceTableEntry
                                    || entry instanceof SourceSinkTableEntry) {
                                tableSources.put(
                                        name,
                                        createTableSource(
                                                tableEnv,
                                                environment.getExecution().isStreamingPlanner(),
                                                classLoader,
                                                name,
                                                entry.asMap()));
                            }
                            if (entry instanceof SinkTableEntry
                                    || entry instanceof SourceSinkTableEntry) {
                                tableSinks.put(
                                        name,
                                        createTableSink(
                                                tableEnv,
                                                environment.getExecution().isStreamingPlanner(),
                                                environment.getExecution().inBatchMode(),
                                                classLoader,
                                                name,
                                                entry.asMap()));
                            }
                        });
        // register table sources
        tableSources.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSourceInternal);
        // register table sinks
        tableSinks.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSinkInternal);

        // --------------------------------------------------------------------------------------------------------------
        // Step.4 Register temporal tables.
        // --------------------------------------------------------------------------------------------------------------
        environment
                .getTables()
                .forEach(
                        (name, entry) -> {
                            if (entry instanceof TemporalTableEntry) {
                                final TemporalTableEntry temporalTableEntry =
                                        (TemporalTableEntry) entry;
                                registerTemporalTable(tableEnv, temporalTableEntry);
                            }
                        });

        // --------------------------------------------------------------------------------------------------------------
        // Step.4 Register views in specified order.
        // --------------------------------------------------------------------------------------------------------------
        environment
                .getTables()
                .forEach(
                        (name, entry) -> {
                            // if registering a view fails at this point,
                            // it means that it accesses tables that are not available anymore
                            if (entry instanceof ViewEntry) {
                                final ViewEntry viewEntry = (ViewEntry) entry;
                                registerTemporaryView(tableEnv, viewEntry);
                            }
                        });

        // --------------------------------------------------------------------------------------------------------------
        // Step.5 Set current catalog and database.
        // --------------------------------------------------------------------------------------------------------------
        // Switch to the current catalog.
        Optional<String> catalog = environment.getExecution().getCurrentCatalog();
        catalog.ifPresent(tableEnv::useCatalog);

        // Switch to the current database.
        Optional<String> database = environment.getExecution().getCurrentDatabase();
        database.ifPresent(tableEnv::useDatabase);
    }

    private static Module createModule(
            Map<String, String> moduleProperties, ClassLoader classLoader) {
        final ModuleFactory factory =
                TableFactoryService.find(ModuleFactory.class, moduleProperties, classLoader);
        return factory.createModule(moduleProperties);
    }

    private static Catalog createCatalog(
            TableEnvironment tableEnv,
            String catalogName,
            Map<String, String> catalogProperties,
            ClassLoader classLoader) {
        return FactoryUtil.createCatalog(
                catalogName,
                catalogProperties,
                tableEnv.getConfig().getConfiguration(),
                classLoader);
    }

    private static TableSource<?> createTableSource(
            TableEnvironment tableEnv,
            boolean isStreamingPlanner,
            URLClassLoader classLoader,
            String name,
            Map<String, String> sourceProperties) {
        if (isStreamingPlanner) {
            // blink planner and old planner in streaming mode
            final TableSourceFactory<?> factory =
                    (TableSourceFactory<?>)
                            TableFactoryService.find(
                                    TableSourceFactory.class, sourceProperties, classLoader);
            return factory.createTableSource(
                    new TableSourceFactoryContextImpl(
                            ObjectIdentifier.of(
                                    tableEnv.getCurrentCatalog(),
                                    tableEnv.getCurrentDatabase(),
                                    name),
                            CatalogTableImpl.fromProperties(sourceProperties),
                            tableEnv.getConfig().getConfiguration(),
                            true));
        } else {
            // old planner in batch mode
            final BatchTableSourceFactory<?> factory =
                    (BatchTableSourceFactory<?>)
                            TableFactoryService.find(
                                    BatchTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createBatchTableSource(sourceProperties);
        }
    }

    private static TableSink<?> createTableSink(
            TableEnvironment tableEnv,
            boolean isStreamingPlanner,
            boolean isBounded,
            URLClassLoader classLoader,
            String name,
            Map<String, String> sinkProperties) {
        if (isStreamingPlanner) {
            // blink planner and old planner in streaming mode
            final TableSinkFactory<?> factory =
                    (TableSinkFactory<?>)
                            TableFactoryService.find(
                                    TableSinkFactory.class, sinkProperties, classLoader);

            return factory.createTableSink(
                    new TableSinkFactoryContextImpl(
                            ObjectIdentifier.of(
                                    tableEnv.getCurrentCatalog(),
                                    tableEnv.getCurrentDatabase(),
                                    name),
                            CatalogTableImpl.fromProperties(sinkProperties),
                            tableEnv.getConfig().getConfiguration(),
                            isBounded,
                            true));
        } else {
            // old planner in batch mode
            final BatchTableSinkFactory<?> factory =
                    (BatchTableSinkFactory<?>)
                            TableFactoryService.find(
                                    BatchTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createBatchTableSink(sinkProperties);
        }
    }

    private static void registerFunctions(
            TableEnvironment tableEnv, Environment environment, URLClassLoader classLoader) {
        Map<String, FunctionDefinition> functions = new LinkedHashMap<>();
        environment
                .getFunctions()
                .forEach(
                        (name, entry) -> {
                            final UserDefinedFunction function =
                                    FunctionService.createFunction(
                                            entry.getDescriptor(),
                                            classLoader,
                                            false,
                                            tableEnv.getConfig().getConfiguration());
                            functions.put(name, function);
                        });

        if (tableEnv instanceof StreamTableEnvironment) {
            StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
            functions.forEach(
                    (k, v) -> {
                        // Blink planner uses FLIP-65 functions for scalar and table functions
                        // aggregate functions still use the old type inference
                        if (environment.getExecution().isBlinkPlanner()) {
                            if (v instanceof ScalarFunction || v instanceof TableFunction) {
                                streamTableEnvironment.createTemporarySystemFunction(
                                        k, (UserDefinedFunction) v);
                            } else if (v instanceof AggregateFunction) {
                                streamTableEnvironment.registerFunction(
                                        k, (AggregateFunction<?, ?>) v);
                            } else {
                                throw new SqlExecutionException(
                                        "Unsupported function type: " + v.getClass().getName());
                            }
                        }
                        // legacy
                        else {
                            if (v instanceof ScalarFunction) {
                                streamTableEnvironment.registerFunction(k, (ScalarFunction) v);
                            } else if (v instanceof AggregateFunction) {
                                streamTableEnvironment.registerFunction(
                                        k, (AggregateFunction<?, ?>) v);
                            } else if (v instanceof TableFunction) {
                                streamTableEnvironment.registerFunction(k, (TableFunction<?>) v);
                            } else {
                                throw new SqlExecutionException(
                                        "Unsupported function type: " + v.getClass().getName());
                            }
                        }
                    });
        } else {
            BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
            functions.forEach(
                    (k, v) -> {
                        if (v instanceof ScalarFunction) {
                            batchTableEnvironment.registerFunction(k, (ScalarFunction) v);
                        } else if (v instanceof AggregateFunction) {
                            batchTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
                        } else if (v instanceof TableFunction) {
                            batchTableEnvironment.registerFunction(k, (TableFunction<?>) v);
                        } else {
                            throw new SqlExecutionException(
                                    "Unsupported function type: " + v.getClass().getName());
                        }
                    });
        }
    }

    private static void registerTemporaryView(TableEnvironment tableEnv, ViewEntry viewEntry) {
        try {
            tableEnv.createTemporaryView(
                    viewEntry.getName(), tableEnv.sqlQuery(viewEntry.getQuery()));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Invalid view '"
                            + viewEntry.getName()
                            + "' with query:\n"
                            + viewEntry.getQuery()
                            + "\nCause: "
                            + e.getMessage());
        }
    }

    private static void registerTemporalTable(
            TableEnvironment tableEnv, TemporalTableEntry temporalTableEntry) {
        try {
            final Table table = tableEnv.from(temporalTableEntry.getHistoryTable());
            List<String> primaryKeyFields = temporalTableEntry.getPrimaryKeyFields();
            if (primaryKeyFields.size() > 1) {
                throw new ValidationException(
                        "Temporal tables over a composite primary key are not supported yet.");
            }
            final TableFunction<?> function =
                    table.createTemporalTableFunction(
                            $(temporalTableEntry.getTimeAttribute()), $(primaryKeyFields.get(0)));
            if (tableEnv instanceof StreamTableEnvironment) {
                StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
                streamTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
            } else {
                BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
                batchTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
            }
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Invalid temporal table '"
                            + temporalTableEntry.getName()
                            + "' over table '"
                            + temporalTableEntry.getHistoryTable()
                            + ".\nCause: "
                            + e.getMessage());
        }
    }
}
