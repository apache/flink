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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
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
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.delegation.ExecutorBase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.
 *
 * @param <ClusterID> cluster id
 */
public class ExecutionContext<ClusterID> {

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

	private final Environment environment;
	private final SessionContext originalSessionContext;
	private final ClassLoader classLoader;

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
			List<CustomCommandLine> availableCommandLines) throws FlinkException {
		this.environment = environment;
		this.originalSessionContext = originalSessionContext;

		this.flinkConfig = flinkConfig;

		// create class loader
		classLoader = FlinkUserCodeClassLoaders.parentFirst(
				dependencies.toArray(new URL[dependencies.size()]),
				this.getClass().getClassLoader());

		// Initialize the TableEnvironment.
		initializeTableEnvironment(sessionState);

		LOG.debug("Deployment descriptor: {}", environment.getDeployment());
		final CommandLine commandLine = createCommandLine(
				environment.getDeployment(),
				commandLineOptions);

		flinkConfig.addAll(createExecutionConfig(
				commandLine,
				commandLineOptions,
				availableCommandLines,
				dependencies));

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
	 * Get the {@link SessionContext} when initialize the ExecutionContext. It's usually used when resetting the session
	 * properties.
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
		try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(classLoader)){
			return supplier.get();
		}
	}

	/**
	 * Executes the given Runnable using the execution context's classloader as thread classloader.
	 */
	void wrapClassLoader(Runnable runnable) {
		try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(classLoader)){
			runnable.run();
		}
	}

	public QueryConfig getQueryConfig() {
		if (streamExecEnv != null) {
			final StreamQueryConfig config = new StreamQueryConfig();
			final long minRetention = environment.getExecution().getMinStateRetention();
			final long maxRetention = environment.getExecution().getMaxStateRetention();
			config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
			return config;
		} else {
			return new BatchQueryConfig();
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

	public Pipeline createPipeline(String name, Configuration flinkConfig) {
		if (streamExecEnv != null) {
			// special case for Blink planner to apply batch optimizations
			// note: it also modifies the ExecutionConfig!
			if (executor instanceof ExecutorBase) {
				return ((ExecutorBase) executor).getStreamGraph(name);
			}
			return streamExecEnv.getStreamGraph(name);
		} else {
			final int parallelism = execEnv.getParallelism();
			return execEnv.createProgramPlan(name);
		}
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
		return new Builder(defaultEnv, sessionContext, dependencies, configuration,
				serviceLoader, commandLineOptions, commandLines);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Non-public methods
	//------------------------------------------------------------------------------------------------------------------

	private static Configuration createExecutionConfig(
			CommandLine commandLine,
			Options commandLineOptions,
			List<CustomCommandLine> availableCommandLines,
			List<URL> dependencies) throws FlinkException {
		LOG.debug("Available commandline options: {}", commandLineOptions);
		List<String> options = Stream
				.of(commandLine.getOptions())
				.map(o -> o.getOpt() + "=" + o.getValue())
				.collect(Collectors.toList());
		LOG.debug(
				"Instantiated commandline args: {}, options: {}",
				commandLine.getArgList(),
				options);

		final CustomCommandLine activeCommandLine = findActiveCommandLine(
				availableCommandLines,
				commandLine);
		LOG.debug(
				"Available commandlines: {}, active commandline: {}",
				availableCommandLines,
				activeCommandLine);

		Configuration executionConfig = activeCommandLine.applyCommandLineOptionsToConfiguration(
				commandLine);

		try {
			final ProgramOptions programOptions = new ProgramOptions(commandLine);
			final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromProgramOptions(programOptions, dependencies);
			executionConfigAccessor.applyToConfiguration(executionConfig);
		} catch (CliArgsException e) {
			throw new SqlExecutionException("Invalid deployment run options.", e);
		}

		LOG.info("Executor config: {}", executionConfig);
		return executionConfig;
	}

	private static CommandLine createCommandLine(DeploymentEntry deployment, Options commandLineOptions) {
		try {
			return deployment.getCommandLine(commandLineOptions);
		} catch (Exception e) {
			throw new SqlExecutionException("Invalid deployment options.", e);
		}
	}

	private static CustomCommandLine findActiveCommandLine(List<CustomCommandLine> availableCommandLines, CommandLine commandLine) {
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

	private Catalog createCatalog(String name, Map<String, String> catalogProperties, ClassLoader classLoader) {
		final CatalogFactory factory =
			TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
		return factory.createCatalog(name, catalogProperties);
	}

	private static TableSource<?> createTableSource(ExecutionEntry execution, Map<String, String> sourceProperties, ClassLoader classLoader) {
		if (execution.isStreamingPlanner()) {
			final TableSourceFactory<?> factory = (TableSourceFactory<?>)
				TableFactoryService.find(TableSourceFactory.class, sourceProperties, classLoader);
			return factory.createTableSource(sourceProperties);
		} else if (execution.isBatchPlanner()) {
			final BatchTableSourceFactory<?> factory = (BatchTableSourceFactory<?>)
				TableFactoryService.find(BatchTableSourceFactory.class, sourceProperties, classLoader);
			return factory.createBatchTableSource(sourceProperties);
		}
		throw new SqlExecutionException("Unsupported execution type for sources.");
	}

	private static TableSink<?> createTableSink(ExecutionEntry execution, Map<String, String> sinkProperties, ClassLoader classLoader) {
		if (execution.isStreamingPlanner()) {
			final TableSinkFactory<?> factory = (TableSinkFactory<?>)
				TableFactoryService.find(TableSinkFactory.class, sinkProperties, classLoader);
			return factory.createTableSink(sinkProperties);
		} else if (execution.isBatchPlanner()) {
			final BatchTableSinkFactory<?> factory = (BatchTableSinkFactory<?>)
				TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
			return factory.createBatchTableSink(sinkProperties);
		}
		throw new SqlExecutionException("Unsupported execution type for sinks.");
	}

	private static TableEnvironment createStreamTableEnvironment(
			StreamExecutionEnvironment env,
			EnvironmentSettings settings,
			Executor executor,
			CatalogManager catalogManager,
			ModuleManager moduleManager,
			FunctionCatalog functionCatalog) {

		final TableConfig config = TableConfig.getDefault();

		final Map<String, String> plannerProperties = settings.toPlannerProperties();
		final Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, config, functionCatalog, catalogManager);

		return new StreamTableEnvironmentImpl(
			catalogManager,
			moduleManager,
			functionCatalog,
			config,
			env,
			planner,
			executor,
			settings.isStreamingMode());
	}

	private static Executor lookupExecutor(
			Map<String, String> executorProperties,
			StreamExecutionEnvironment executionEnvironment) {
		try {
			ExecutorFactory executorFactory = ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
			Method createMethod = executorFactory.getClass()
				.getMethod("create", Map.class, StreamExecutionEnvironment.class);

			return (Executor) createMethod.invoke(
				executorFactory,
				executorProperties,
				executionEnvironment);
		} catch (Exception e) {
			throw new TableException(
				"Could not instantiate the executor. Make sure a planner module is on the classpath",
				e);
		}
	}

	private void initializeTableEnvironment(@Nullable SessionState sessionState) {
		final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
		final boolean noInheritedState = sessionState == null;
		if (noInheritedState) {
			//--------------------------------------------------------------------------------------------------------------
			// Step.1 Create environments
			//--------------------------------------------------------------------------------------------------------------
			// Step 1.1 Initialize the CatalogManager if required.
			final CatalogManager catalogManager = new CatalogManager(
					settings.getBuiltInCatalogName(),
					new GenericInMemoryCatalog(
							settings.getBuiltInCatalogName(),
							settings.getBuiltInDatabaseName()));
			// Step 1.2 Initialize the ModuleManager if required.
			final ModuleManager moduleManager = new ModuleManager();
			// Step 1.3 Initialize the FunctionCatalog if required.
			final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager, moduleManager);
			// Step 1.4 Set up session state.
			this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);

			// Must initialize the table environment before actually the
			createTableEnvironment(settings, catalogManager, moduleManager, functionCatalog);

			//--------------------------------------------------------------------------------------------------------------
			// Step.2 Create modules and load them into the TableEnvironment.
			//--------------------------------------------------------------------------------------------------------------
			// No need to register the modules info if already inherit from the same session.
			Map<String, Module> modules = new LinkedHashMap<>();
			environment.getModules().forEach((name, entry) ->
					modules.put(name, createModule(entry.asMap(), classLoader))
			);
			if (!modules.isEmpty()) {
				// unload core module first to respect whatever users configure
				tableEnv.unloadModule(CoreModuleDescriptorValidator.MODULE_TYPE_CORE);
				modules.forEach(tableEnv::loadModule);
			}

			//--------------------------------------------------------------------------------------------------------------
			// Step.3 create user-defined functions and temporal tables then register them.
			//--------------------------------------------------------------------------------------------------------------
			// No need to register the functions if already inherit from the same session.
			registerFunctions();

			//--------------------------------------------------------------------------------------------------------------
			// Step.4 Create catalogs and register them.
			//--------------------------------------------------------------------------------------------------------------
			// No need to register the catalogs if already inherit from the same session.
			initializeCatalogs();
		} else {
			// Set up session state.
			this.sessionState = sessionState;
			createTableEnvironment(
					settings,
					sessionState.catalogManager,
					sessionState.moduleManager,
					sessionState.functionCatalog);
		}
	}

	private void createTableEnvironment(
			EnvironmentSettings settings,
			CatalogManager catalogManager,
			ModuleManager moduleManager,
			FunctionCatalog functionCatalog) {
		if (environment.getExecution().isStreamingPlanner()) {
			streamExecEnv = createStreamExecutionEnvironment();
			execEnv = null;

			final Map<String, String> executorProperties = settings.toExecutorProperties();
			executor = lookupExecutor(executorProperties, streamExecEnv);
			tableEnv = createStreamTableEnvironment(
					streamExecEnv,
					settings,
					executor,
					catalogManager,
					moduleManager,
					functionCatalog);
		} else if (environment.getExecution().isBatchPlanner()) {
			streamExecEnv = null;
			execEnv = createExecutionEnvironment();
			executor = null;
			tableEnv = new BatchTableEnvironmentImpl(
					execEnv,
					TableConfig.getDefault(),
					catalogManager,
					moduleManager);
		} else {
			throw new SqlExecutionException("Unsupported execution type specified.");
		}
		// set table configuration
		environment.getConfiguration().asMap().forEach((k, v) ->
				tableEnv.getConfig().getConfiguration().setString(k, v));
	}

	private void initializeCatalogs() {
		//--------------------------------------------------------------------------------------------------------------
		// Step.1 Create catalogs and register them.
		//--------------------------------------------------------------------------------------------------------------
		wrapClassLoader(() -> {
			environment.getCatalogs().forEach((name, entry) -> {
				Catalog catalog = createCatalog(name, entry.asMap(), classLoader);
				tableEnv.registerCatalog(name, catalog);
			});
		});

		//--------------------------------------------------------------------------------------------------------------
		// Step.2 create table sources & sinks, and register them.
		//--------------------------------------------------------------------------------------------------------------
		Map<String, TableSource<?>> tableSources = new HashMap<>();
		Map<String, TableSink<?>> tableSinks = new HashMap<>();
		environment.getTables().forEach((name, entry) -> {
			if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
				tableSources.put(name, createTableSource(environment.getExecution(), entry.asMap(), classLoader));
			}
			if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
				tableSinks.put(name, createTableSink(environment.getExecution(), entry.asMap(), classLoader));
			}
		});
		// register table sources
		tableSources.forEach(tableEnv::registerTableSource);
		// register table sinks
		tableSinks.forEach(tableEnv::registerTableSink);

		//--------------------------------------------------------------------------------------------------------------
		// Step.4 Register temporal tables.
		//--------------------------------------------------------------------------------------------------------------
		environment.getTables().forEach((name, entry) -> {
			if (entry instanceof TemporalTableEntry) {
				final TemporalTableEntry temporalTableEntry = (TemporalTableEntry) entry;
				registerTemporalTable(temporalTableEntry);
			}
		});

		//--------------------------------------------------------------------------------------------------------------
		// Step.4 Register views in specified order.
		//--------------------------------------------------------------------------------------------------------------
		environment.getTables().forEach((name, entry) -> {
			// if registering a view fails at this point,
			// it means that it accesses tables that are not available anymore
			if (entry instanceof ViewEntry) {
				final ViewEntry viewEntry = (ViewEntry) entry;
				registerView(viewEntry);
			}
		});

		//--------------------------------------------------------------------------------------------------------------
		// Step.5 Set current catalog and database.
		//--------------------------------------------------------------------------------------------------------------
		// Switch to the current catalog.
		Optional<String> catalog = environment.getExecution().getCurrentCatalog();
		catalog.ifPresent(tableEnv::useCatalog);

		// Switch to the current database.
		Optional<String> database = environment.getExecution().getCurrentDatabase();
		database.ifPresent(tableEnv::useDatabase);
	}

	private ExecutionEnvironment createExecutionEnvironment() {
		final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
		execEnv.setRestartStrategy(environment.getExecution().getRestartStrategy());
		execEnv.setParallelism(environment.getExecution().getParallelism());
		return execEnv;
	}

	private StreamExecutionEnvironment createStreamExecutionEnvironment() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(environment.getExecution().getRestartStrategy());
		env.setParallelism(environment.getExecution().getParallelism());
		env.setMaxParallelism(environment.getExecution().getMaxParallelism());
		env.setStreamTimeCharacteristic(environment.getExecution().getTimeCharacteristic());
		if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
			env.getConfig().setAutoWatermarkInterval(environment.getExecution().getPeriodicWatermarksInterval());
		}
		return env;
	}

	private void registerFunctions() {
		Map<String, FunctionDefinition> functions = new LinkedHashMap<>();
		environment.getFunctions().forEach((name, entry) -> {
			final UserDefinedFunction function = FunctionService.createFunction(entry.getDescriptor(), classLoader, false);
			functions.put(name, function);
		});
		registerFunctions(functions);
	}

	private void registerFunctions(Map<String, FunctionDefinition> functions) {
		if (tableEnv instanceof StreamTableEnvironment) {
			StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
			functions.forEach((k, v) -> {
				if (v instanceof ScalarFunction) {
					streamTableEnvironment.registerFunction(k, (ScalarFunction) v);
				} else if (v instanceof AggregateFunction) {
					streamTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
				} else if (v instanceof TableFunction) {
					streamTableEnvironment.registerFunction(k, (TableFunction<?>) v);
				} else {
					throw new SqlExecutionException("Unsupported function type: " + v.getClass().getName());
				}
			});
		} else {
			BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
			functions.forEach((k, v) -> {
				if (v instanceof ScalarFunction) {
					batchTableEnvironment.registerFunction(k, (ScalarFunction) v);
				} else if (v instanceof AggregateFunction) {
					batchTableEnvironment.registerFunction(k, (AggregateFunction<?, ?>) v);
				} else if (v instanceof TableFunction) {
					batchTableEnvironment.registerFunction(k, (TableFunction<?>) v);
				} else {
					throw new SqlExecutionException("Unsupported function type: " + v.getClass().getName());
				}
			});
		}
	}

	private void registerView(ViewEntry viewEntry) {
		try {
			tableEnv.registerTable(viewEntry.getName(), tableEnv.sqlQuery(viewEntry.getQuery()));
		} catch (Exception e) {
			throw new SqlExecutionException(
				"Invalid view '" + viewEntry.getName() + "' with query:\n" + viewEntry.getQuery()
					+ "\nCause: " + e.getMessage());
		}
	}

	private void registerTemporalTable(TemporalTableEntry temporalTableEntry) {
		try {
			final Table table = tableEnv.scan(temporalTableEntry.getHistoryTable());
			final TableFunction<?> function = table.createTemporalTableFunction(
				temporalTableEntry.getTimeAttribute(),
				String.join(",", temporalTableEntry.getPrimaryKeyFields()));
			if (tableEnv instanceof StreamTableEnvironment) {
				StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tableEnv;
				streamTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
			} else {
				BatchTableEnvironment batchTableEnvironment = (BatchTableEnvironment) tableEnv;
				batchTableEnvironment.registerFunction(temporalTableEntry.getName(), function);
			}
		} catch (Exception e) {
			throw new SqlExecutionException(
				"Invalid temporal table '" + temporalTableEntry.getName() + "' over table '" +
					temporalTableEntry.getHistoryTable() + ".\nCause: " + e.getMessage());
		}
	}

	private Pipeline createPipeline(String name) {
		if (streamExecEnv != null) {
			// special case for Blink planner to apply batch optimizations
			// note: it also modifies the ExecutionConfig!
			if (executor instanceof ExecutorBase) {
				return ((ExecutorBase) executor).getStreamGraph(name);
			}
			return streamExecEnv.getStreamGraph(name);
		} else {
			return execEnv.createProgramPlan(name);
		}
	}

	//~ Inner Class -------------------------------------------------------------------------------

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
		@Nullable
		private SessionState sessionState;

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

	/** Represents the state that should be reused in one session. **/
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
