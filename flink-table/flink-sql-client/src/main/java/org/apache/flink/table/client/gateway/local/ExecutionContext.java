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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FallbackRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamPipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.DeploymentEntry;
import org.apache.flink.table.client.config.entries.ExecutionEntry;
import org.apache.flink.table.client.config.entries.SinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceSinkTableEntry;
import org.apache.flink.table.client.config.entries.SourceTableEntry;
import org.apache.flink.table.client.config.entries.TemporalTableEntry;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.CoreModuleDescriptorValidator;
import org.apache.flink.table.descriptors.FunctionDescriptorValidator;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
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
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as it
 * might be reused across different query submissions.
 *
 * @param <ClusterID> cluster id
 */
public class ExecutionContext<ClusterID> {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

    private final Environment environment;
    private final SessionContext originalSessionContext;
    private final URLClassLoader classLoader;

    private final Configuration flinkConfig;
    private final ClusterClientFactory<ClusterID> clusterClientFactory;
    private final ClusterID clusterId;
    private final ClusterSpecification clusterSpec;

    private TableEnvironment tableEnv;
    private ExecutionEnvironment execEnv;
    private StreamExecutionEnvironment streamExecEnv;
    private Executor executor;

    // Members that should be reused in the same session.
    private SessionState sessionState;

    private ExecutionContext(
            Environment environment,
            SessionContext originalSessionContext,
            @Nullable SessionState sessionState,
            List<URL> dependencies,
            Configuration flinkConfig,
            ClusterClientServiceLoader clusterClientServiceLoader,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines)
            throws FlinkException {
        this.environment = environment;
        this.originalSessionContext = originalSessionContext;

        this.flinkConfig = flinkConfig;

        if (containsPythonFunction(environment)) {
            dependencies = addPythonDependency(dependencies);
        }

        // create class loader
        classLoader =
                ClientUtils.buildUserCodeClassLoader(
                        dependencies,
                        Collections.emptyList(),
                        this.getClass().getClassLoader(),
                        flinkConfig);

        // Initialize the TableEnvironment.
        initializeTableEnvironment(sessionState);

        LOG.debug("Deployment descriptor: {}", environment.getDeployment());
        final CommandLine commandLine =
                createCommandLine(environment.getDeployment(), commandLineOptions);

        flinkConfig.addAll(
                createExecutionConfig(
                        commandLine, commandLineOptions, availableCommandLines, dependencies));

        final ClusterClientServiceLoader serviceLoader = checkNotNull(clusterClientServiceLoader);
        clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
        checkState(clusterClientFactory != null);

        clusterId = clusterClientFactory.getClusterId(flinkConfig);
        clusterSpec = clusterClientFactory.getClusterSpecification(flinkConfig);
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    /**
     * Get the {@link SessionContext} when initialize the ExecutionContext. It's usually used when
     * resetting the session properties.
     *
     * @return the original session context.
     */
    public SessionContext getOriginalSessionContext() {
        return this.originalSessionContext;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public ClusterSpecification getClusterSpec() {
        return clusterSpec;
    }

    public ClusterID getClusterId() {
        return clusterId;
    }

    public ClusterDescriptor<ClusterID> createClusterDescriptor() {
        return clusterClientFactory.createClusterDescriptor(flinkConfig);
    }

    public Map<String, Catalog> getCatalogs() {
        Map<String, Catalog> catalogs = new HashMap<>();
        for (String name : tableEnv.listCatalogs()) {
            tableEnv.getCatalog(name).ifPresent(c -> catalogs.put(name, c));
        }
        return catalogs;
    }

    public SessionState getSessionState() {
        return this.sessionState;
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    public <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            return supplier.get();
        }
    }

    /**
     * Executes the given Runnable using the execution context's classloader as thread classloader.
     */
    void wrapClassLoader(Runnable runnable) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            runnable.run();
        }
    }

    public TableEnvironment getTableEnvironment() {
        return tableEnv;
    }

    public ExecutionConfig getExecutionConfig() {
        if (streamExecEnv != null) {
            return streamExecEnv.getConfig();
        } else {
            return execEnv.getConfig();
        }
    }

    public Pipeline createPipeline(String name) {
        return wrapClassLoader(
                () -> {
                    if (streamExecEnv != null) {
                        StreamTableEnvironmentImpl streamTableEnv =
                                (StreamTableEnvironmentImpl) tableEnv;
                        return streamTableEnv.getPipeline(name);
                    } else {
                        BatchTableEnvironmentImpl batchTableEnv =
                                (BatchTableEnvironmentImpl) tableEnv;
                        return batchTableEnv.getPipeline(name);
                    }
                });
    }

    /** Returns a builder for this {@link ExecutionContext}. */
    public static Builder builder(
            Environment defaultEnv,
            SessionContext sessionContext,
            List<URL> dependencies,
            Configuration configuration,
            ClusterClientServiceLoader serviceLoader,
            Options commandLineOptions,
            List<CustomCommandLine> commandLines) {
        return new Builder(
                defaultEnv,
                sessionContext,
                dependencies,
                configuration,
                serviceLoader,
                commandLineOptions,
                commandLines);
    }

    /** Close resources associated with this ExecutionContext, e.g. catalogs. */
    public void close() {
        wrapClassLoader(() -> getCatalogs().values().forEach(Catalog::close));
        try {
            classLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing class loader.", e);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Non-public methods
    // ------------------------------------------------------------------------------------------------------------------

    private static Configuration createExecutionConfig(
            CommandLine commandLine,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines,
            List<URL> dependencies)
            throws FlinkException {
        LOG.debug("Available commandline options: {}", commandLineOptions);
        List<String> options =
                Stream.of(commandLine.getOptions())
                        .map(o -> o.getOpt() + "=" + o.getValue())
                        .collect(Collectors.toList());
        LOG.debug(
                "Instantiated commandline args: {}, options: {}",
                commandLine.getArgList(),
                options);

        final CustomCommandLine activeCommandLine =
                findActiveCommandLine(availableCommandLines, commandLine);
        LOG.debug(
                "Available commandlines: {}, active commandline: {}",
                availableCommandLines,
                activeCommandLine);

        Configuration executionConfig = activeCommandLine.toConfiguration(commandLine);

        try {
            final ProgramOptions programOptions = ProgramOptions.create(commandLine);
            final ExecutionConfigAccessor executionConfigAccessor =
                    ExecutionConfigAccessor.fromProgramOptions(programOptions, dependencies);
            executionConfigAccessor.applyToConfiguration(executionConfig);
        } catch (CliArgsException e) {
            throw new SqlExecutionException("Invalid deployment run options.", e);
        }

        LOG.info("Executor config: {}", executionConfig);
        return executionConfig;
    }

    private static CommandLine createCommandLine(
            DeploymentEntry deployment, Options commandLineOptions) {
        try {
            return deployment.getCommandLine(commandLineOptions);
        } catch (Exception e) {
            throw new SqlExecutionException("Invalid deployment options.", e);
        }
    }

    private static CustomCommandLine findActiveCommandLine(
            List<CustomCommandLine> availableCommandLines, CommandLine commandLine) {
        for (CustomCommandLine cli : availableCommandLines) {
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new SqlExecutionException("Could not find a matching deployment.");
    }

    private Module createModule(Map<String, String> moduleProperties, ClassLoader classLoader) {
        final ModuleFactory factory =
                TableFactoryService.find(ModuleFactory.class, moduleProperties, classLoader);
        return factory.createModule(moduleProperties);
    }

    private Catalog createCatalog(
            String name, Map<String, String> catalogProperties, ClassLoader classLoader) {
        final CatalogFactory factory =
                TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
        return factory.createCatalog(name, catalogProperties);
    }

    private TableSource<?> createTableSource(String name, Map<String, String> sourceProperties) {
        if (environment.getExecution().isStreamingPlanner()) {
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
        } else if (environment.getExecution().isBatchPlanner()) {
            final BatchTableSourceFactory<?> factory =
                    (BatchTableSourceFactory<?>)
                            TableFactoryService.find(
                                    BatchTableSourceFactory.class, sourceProperties, classLoader);
            return factory.createBatchTableSource(sourceProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sources.");
    }

    private TableSink<?> createTableSink(String name, Map<String, String> sinkProperties) {
        if (environment.getExecution().isStreamingPlanner()) {
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
                            !environment.getExecution().inStreamingMode(),
                            true));
        } else if (environment.getExecution().isBatchPlanner()) {
            final BatchTableSinkFactory<?> factory =
                    (BatchTableSinkFactory<?>)
                            TableFactoryService.find(
                                    BatchTableSinkFactory.class, sinkProperties, classLoader);
            return factory.createBatchTableSink(sinkProperties);
        }
        throw new SqlExecutionException("Unsupported execution type for sinks.");
    }

    private static TableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig config,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            ClassLoader userClassLoader) {

        final Map<String, String> plannerProperties = settings.toPlannerProperties();
        final Planner planner =
                ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                        .create(
                                plannerProperties,
                                executor,
                                config,
                                functionCatalog,
                                catalogManager);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                config,
                env,
                planner,
                executor,
                settings.isStreamingMode(),
                userClassLoader);
    }

    private static Executor lookupExecutor(
            Map<String, String> executorProperties,
            StreamExecutionEnvironment executionEnvironment) {
        try {
            ExecutorFactory executorFactory =
                    ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor)
                    createMethod.invoke(executorFactory, executorProperties, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private void initializeTableEnvironment(@Nullable SessionState sessionState) {
        final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
        final boolean noInheritedState = sessionState == null;
        // Step 0.0 Initialize the table configuration.
        final TableConfig config = createTableConfig();

        if (noInheritedState) {
            // --------------------------------------------------------------------------------------------------------------
            // Step.1 Create environments
            // --------------------------------------------------------------------------------------------------------------

            // Step 1.0 Initialize the ModuleManager if required.
            final ModuleManager moduleManager = new ModuleManager();

            // Step 1.1 Initialize the CatalogManager if required.
            final CatalogManager catalogManager =
                    CatalogManager.newBuilder()
                            .classLoader(classLoader)
                            .config(config.getConfiguration())
                            .defaultCatalog(
                                    settings.getBuiltInCatalogName(),
                                    new GenericInMemoryCatalog(
                                            settings.getBuiltInCatalogName(),
                                            settings.getBuiltInDatabaseName()))
                            .build();

            // Step 1.2 Initialize the FunctionCatalog if required.
            final FunctionCatalog functionCatalog =
                    new FunctionCatalog(config, catalogManager, moduleManager);

            // Step 1.3 Set up session state.
            this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);

            // Must initialize the table environment before actually the
            createTableEnvironment(
                    settings, config, catalogManager, moduleManager, functionCatalog);

            // --------------------------------------------------------------------------------------------------------------
            // Step.2 Create modules and load them into the TableEnvironment.
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
            // Step.3 create user-defined functions and temporal tables then register them.
            // --------------------------------------------------------------------------------------------------------------
            // No need to register the functions if already inherit from the same session.
            registerFunctions();

            // --------------------------------------------------------------------------------------------------------------
            // Step.4 Create catalogs and register them.
            // --------------------------------------------------------------------------------------------------------------
            // No need to register the catalogs if already inherit from the same session.
            initializeCatalogs();
        } else {
            // Set up session state.
            this.sessionState = sessionState;
            createTableEnvironment(
                    settings,
                    config,
                    sessionState.catalogManager,
                    sessionState.moduleManager,
                    sessionState.functionCatalog);
        }
    }

    private TableConfig createTableConfig() {
        final TableConfig config = new TableConfig();
        config.addConfiguration(flinkConfig);
        Configuration conf = config.getConfiguration();
        environment.getConfiguration().asMap().forEach(conf::setString);
        ExecutionEntry execution = environment.getExecution();
        config.setIdleStateRetentionTime(
                Time.milliseconds(execution.getMinStateRetention()),
                Time.milliseconds(execution.getMaxStateRetention()));

        if (execution.getParallelism().isPresent()) {
            conf.set(CoreOptions.DEFAULT_PARALLELISM, execution.getParallelism().get());
        }
        conf.set(PipelineOptions.MAX_PARALLELISM, execution.getMaxParallelism());
        conf.set(StreamPipelineOptions.TIME_CHARACTERISTIC, execution.getTimeCharacteristic());
        if (execution.getTimeCharacteristic() == TimeCharacteristic.EventTime) {
            conf.set(
                    PipelineOptions.AUTO_WATERMARK_INTERVAL,
                    Duration.ofMillis(execution.getPeriodicWatermarksInterval()));
        }

        setRestartStrategy(conf);
        return config;
    }

    private void setRestartStrategy(Configuration conf) {
        RestartStrategyConfiguration restartStrategy =
                environment.getExecution().getRestartStrategy();
        if (restartStrategy instanceof NoRestartStrategyConfiguration) {
            conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        } else if (restartStrategy instanceof FixedDelayRestartStrategyConfiguration) {
            conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            FixedDelayRestartStrategyConfiguration fixedDelay =
                    ((FixedDelayRestartStrategyConfiguration) restartStrategy);
            conf.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                    fixedDelay.getRestartAttempts());
            conf.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    Duration.ofMillis(
                            fixedDelay.getDelayBetweenAttemptsInterval().toMilliseconds()));
        } else if (restartStrategy instanceof FailureRateRestartStrategyConfiguration) {
            conf.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
            FailureRateRestartStrategyConfiguration failureRate =
                    (FailureRateRestartStrategyConfiguration) restartStrategy;
            conf.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
                    failureRate.getMaxFailureRate());
            conf.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                    Duration.ofMillis(failureRate.getFailureInterval().toMilliseconds()));
            conf.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY,
                    Duration.ofMillis(
                            failureRate.getDelayBetweenAttemptsInterval().toMilliseconds()));
        } else if (restartStrategy instanceof FallbackRestartStrategyConfiguration) {
            // default is FallbackRestartStrategyConfiguration
            // see ExecutionConfig.restartStrategyConfiguration
            conf.removeConfig(RestartStrategyOptions.RESTART_STRATEGY);
        }
    }

    private void createTableEnvironment(
            EnvironmentSettings settings,
            TableConfig config,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog) {
        if (environment.getExecution().isStreamingPlanner()) {
            streamExecEnv = createStreamExecutionEnvironment();
            execEnv = null;

            final Map<String, String> executorProperties = settings.toExecutorProperties();
            executor = lookupExecutor(executorProperties, streamExecEnv);
            tableEnv =
                    createStreamTableEnvironment(
                            streamExecEnv,
                            settings,
                            config,
                            executor,
                            catalogManager,
                            moduleManager,
                            functionCatalog,
                            classLoader);
        } else if (environment.getExecution().isBatchPlanner()) {
            streamExecEnv = null;
            execEnv = ExecutionEnvironment.getExecutionEnvironment();
            executor = null;
            tableEnv =
                    new BatchTableEnvironmentImpl(execEnv, config, catalogManager, moduleManager);
        } else {
            throw new SqlExecutionException("Unsupported execution type specified.");
        }
    }

    private void initializeCatalogs() {
        // --------------------------------------------------------------------------------------------------------------
        // Step.1 Create catalogs and register them.
        // --------------------------------------------------------------------------------------------------------------
        wrapClassLoader(
                () -> {
                    environment
                            .getCatalogs()
                            .forEach(
                                    (name, entry) -> {
                                        Catalog catalog =
                                                createCatalog(name, entry.asMap(), classLoader);
                                        tableEnv.registerCatalog(name, catalog);
                                    });
                });

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
                                tableSources.put(name, createTableSource(name, entry.asMap()));
                            }
                            if (entry instanceof SinkTableEntry
                                    || entry instanceof SourceSinkTableEntry) {
                                tableSinks.put(name, createTableSink(name, entry.asMap()));
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
                                registerTemporalTable(temporalTableEntry);
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
                                registerTemporaryView(viewEntry);
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

    private StreamExecutionEnvironment createStreamExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for TimeCharacteristic validation in StreamTableEnvironmentImpl
        env.setStreamTimeCharacteristic(environment.getExecution().getTimeCharacteristic());
        if (environment.getExecution().getTimeCharacteristic() == TimeCharacteristic.EventTime) {
            env.getConfig()
                    .setAutoWatermarkInterval(
                            environment.getExecution().getPeriodicWatermarksInterval());
        }
        return env;
    }

    private void registerFunctions() {
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
                                            getTableEnvironment().getConfig().getConfiguration());
                            functions.put(name, function);
                        });
        registerFunctions(functions);
    }

    private void registerFunctions(Map<String, FunctionDefinition> functions) {
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

    private void registerTemporaryView(ViewEntry viewEntry) {
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

    private void registerTemporalTable(TemporalTableEntry temporalTableEntry) {
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

    private boolean containsPythonFunction(Environment environment) {
        return environment.getFunctions().values().stream()
                .anyMatch(
                        f ->
                                FunctionDescriptorValidator.FROM_VALUE_PYTHON.equals(
                                        f.getDescriptor()
                                                .toProperties()
                                                .get(FunctionDescriptorValidator.FROM)));
    }

    private List<URL> addPythonDependency(List<URL> dependencies) {
        List<URL> newDependencies = new ArrayList<>(dependencies);
        try {
            URL location =
                    Class.forName(
                                    "org.apache.flink.python.PythonFunctionRunner",
                                    false,
                                    Thread.currentThread().getContextClassLoader())
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation();
            if (Paths.get(location.toURI()).toFile().isFile()) {
                newDependencies.add(location);
            }
        } catch (URISyntaxException | ClassNotFoundException e) {
            throw new SqlExecutionException(
                    "Python UDF detected but flink-python jar not found. "
                            + "If you starts SQL-Client via `sql-client.sh`, please add the flink-python jar "
                            + "via `-j` command option manually.",
                    e);
        }
        return newDependencies;
    }

    // ~ Inner Class -------------------------------------------------------------------------------

    /** Builder for {@link ExecutionContext}. */
    public static class Builder {
        // Required members.
        private final SessionContext sessionContext;
        private final List<URL> dependencies;
        private final Configuration configuration;
        private final ClusterClientServiceLoader serviceLoader;
        private final Options commandLineOptions;
        private final List<CustomCommandLine> commandLines;

        private Environment defaultEnv;
        private Environment currentEnv;

        // Optional members.
        @Nullable private SessionState sessionState;

        private Builder(
                Environment defaultEnv,
                @Nullable SessionContext sessionContext,
                List<URL> dependencies,
                Configuration configuration,
                ClusterClientServiceLoader serviceLoader,
                Options commandLineOptions,
                List<CustomCommandLine> commandLines) {
            this.defaultEnv = defaultEnv;
            this.sessionContext = sessionContext;
            this.dependencies = dependencies;
            this.configuration = configuration;
            this.serviceLoader = serviceLoader;
            this.commandLineOptions = commandLineOptions;
            this.commandLines = commandLines;
        }

        public Builder env(Environment environment) {
            this.currentEnv = environment;
            return this;
        }

        public Builder sessionState(SessionState sessionState) {
            this.sessionState = sessionState;
            return this;
        }

        public ExecutionContext<?> build() {
            try {
                return new ExecutionContext<>(
                        this.currentEnv == null
                                ? Environment.merge(defaultEnv, sessionContext.getSessionEnv())
                                : this.currentEnv,
                        this.sessionContext,
                        this.sessionState,
                        this.dependencies,
                        this.configuration,
                        this.serviceLoader,
                        this.commandLineOptions,
                        this.commandLines);
            } catch (Throwable t) {
                // catch everything such that a configuration does not crash the executor
                throw new SqlExecutionException("Could not create execution context.", t);
            }
        }
    }

    /** Represents the state that should be reused in one session. * */
    public static class SessionState {
        public final CatalogManager catalogManager;
        public final ModuleManager moduleManager;
        public final FunctionCatalog functionCatalog;

        private SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.functionCatalog = functionCatalog;
        }

        public static SessionState of(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            return new SessionState(catalogManager, moduleManager, functionCatalog);
        }
    }
}
