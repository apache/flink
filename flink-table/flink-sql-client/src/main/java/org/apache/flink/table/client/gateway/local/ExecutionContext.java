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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.
 *
 * @param <T> cluster id
 */
public class ExecutionContext<T> {

	private final SessionContext sessionContext;
	private final Environment mergedEnv;
	private final List<URL> dependencies;
	private final ClassLoader classLoader;
	private final Map<String, TableSource<?>> tableSources;
	private final Map<String, TableSink<?>> tableSinks;
	private final Map<String, UserDefinedFunction> functions;
	private final Configuration flinkConfig;
	private final CommandLine commandLine;
	private final CustomCommandLine<T> activeCommandLine;
	private final RunOptions runOptions;
	private final T clusterId;
	private final ClusterSpecification clusterSpec;

	public ExecutionContext(Environment defaultEnvironment, SessionContext sessionContext, List<URL> dependencies,
			Configuration flinkConfig, Options commandLineOptions, List<CustomCommandLine<?>> availableCommandLines) {
		this.sessionContext = sessionContext.copy(); // create internal copy because session context is mutable
		this.mergedEnv = Environment.merge(defaultEnvironment, sessionContext.getEnvironment());
		this.dependencies = dependencies;
		this.flinkConfig = flinkConfig;

		// create class loader
		classLoader = FlinkUserCodeClassLoaders.parentFirst(
			dependencies.toArray(new URL[dependencies.size()]),
			this.getClass().getClassLoader());

		// create table sources & sinks.
		tableSources = new HashMap<>();
		tableSinks = new HashMap<>();
		mergedEnv.getTables().forEach((name, entry) -> {
			if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
				tableSources.put(name, createTableSource(mergedEnv.getExecution(), entry.asMap(), classLoader));
			}
			if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
				tableSinks.put(name, createTableSink(mergedEnv.getExecution(), entry.asMap(), classLoader));
			}
		});

		// create user-defined functions
		functions = new HashMap<>();
		mergedEnv.getFunctions().forEach((name, entry) -> {
			final UserDefinedFunction function = FunctionService.createFunction(entry.getDescriptor(), classLoader, false);
			functions.put(name, function);
		});

		// convert deployment options into command line options that describe a cluster
		commandLine = createCommandLine(mergedEnv.getDeployment(), commandLineOptions);
		activeCommandLine = findActiveCommandLine(availableCommandLines, commandLine);
		runOptions = createRunOptions(commandLine);
		clusterId = activeCommandLine.getClusterId(commandLine);
		clusterSpec = createClusterSpecification(activeCommandLine, commandLine);
	}

	public SessionContext getSessionContext() {
		return sessionContext;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}

	public Environment getMergedEnvironment() {
		return mergedEnv;
	}

	public ClusterSpecification getClusterSpec() {
		return clusterSpec;
	}

	public T getClusterId() {
		return clusterId;
	}

	public ClusterDescriptor<T> createClusterDescriptor() throws Exception {
		return activeCommandLine.createClusterDescriptor(commandLine);
	}

	public EnvironmentInstance createEnvironmentInstance() {
		try {
			return wrapClassLoader(EnvironmentInstance::new);
		} catch (Throwable t) {
			// catch everything such that a wrong environment does not affect invocations
			throw new SqlExecutionException("Could not create environment instance.", t);
		}
	}

	public Map<String, TableSource<?>> getTableSources() {
		return tableSources;
	}

	public Map<String, TableSink<?>> getTableSinks() {
		return tableSinks;
	}

	/**
	 * Executes the given supplier using the execution context's classloader as thread classloader.
	 */
	public <R> R wrapClassLoader(Supplier<R> supplier) {
		try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(classLoader)){
			return supplier.get();
		}
	}

	// --------------------------------------------------------------------------------------------

	private static CommandLine createCommandLine(DeploymentEntry deployment, Options commandLineOptions) {
		try {
			return deployment.getCommandLine(commandLineOptions);
		} catch (Exception e) {
			throw new SqlExecutionException("Invalid deployment options.", e);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> CustomCommandLine<T> findActiveCommandLine(List<CustomCommandLine<?>> availableCommandLines, CommandLine commandLine) {
		for (CustomCommandLine<?> cli : availableCommandLines) {
			if (cli.isActive(commandLine)) {
				return (CustomCommandLine<T>) cli;
			}
		}
		throw new SqlExecutionException("Could not find a matching deployment.");
	}

	private static RunOptions createRunOptions(CommandLine commandLine) {
		try {
			return new RunOptions(commandLine);
		} catch (CliArgsException e) {
			throw new SqlExecutionException("Invalid deployment run options.", e);
		}
	}

	private static ClusterSpecification createClusterSpecification(CustomCommandLine<?> activeCommandLine, CommandLine commandLine) {
		try {
			return activeCommandLine.getClusterSpecification(commandLine);
		} catch (FlinkException e) {
			throw new SqlExecutionException("Could not create cluster specification for the given deployment.", e);
		}
	}

	private static TableSource<?> createTableSource(ExecutionEntry execution, Map<String, String> sourceProperties, ClassLoader classLoader) {
		if (execution.isStreamingExecution()) {
			final StreamTableSourceFactory<?> factory = (StreamTableSourceFactory<?>)
				TableFactoryService.find(StreamTableSourceFactory.class, sourceProperties, classLoader);
			return factory.createStreamTableSource(sourceProperties);
		} else if (execution.isBatchExecution()) {
			final BatchTableSourceFactory<?> factory = (BatchTableSourceFactory<?>)
				TableFactoryService.find(BatchTableSourceFactory.class, sourceProperties, classLoader);
			return factory.createBatchTableSource(sourceProperties);
		}
		throw new SqlExecutionException("Unsupported execution type for sources.");
	}

	private static TableSink<?> createTableSink(ExecutionEntry execution, Map<String, String> sinkProperties, ClassLoader classLoader) {
		if (execution.isStreamingExecution()) {
			final StreamTableSinkFactory<?> factory = (StreamTableSinkFactory<?>)
				TableFactoryService.find(StreamTableSinkFactory.class, sinkProperties, classLoader);
			return factory.createStreamTableSink(sinkProperties);
		} else if (execution.isBatchExecution()) {
			final BatchTableSinkFactory<?> factory = (BatchTableSinkFactory<?>)
				TableFactoryService.find(BatchTableSinkFactory.class, sinkProperties, classLoader);
			return factory.createBatchTableSink(sinkProperties);
		}
		throw new SqlExecutionException("Unsupported execution type for sinks.");
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * {@link ExecutionEnvironment} and {@link StreamExecutionEnvironment} cannot be reused
	 * across multiple queries because they are stateful. This class abstracts execution
	 * environments and table environments.
	 */
	public class EnvironmentInstance {

		private final QueryConfig queryConfig;
		private final ExecutionEnvironment execEnv;
		private final StreamExecutionEnvironment streamExecEnv;
		private final TableEnvironment tableEnv;

		private EnvironmentInstance() {
			// create environments
			if (mergedEnv.getExecution().isStreamingExecution()) {
				streamExecEnv = createStreamExecutionEnvironment();
				execEnv = null;
				tableEnv = StreamTableEnvironment.create(streamExecEnv);
			} else if (mergedEnv.getExecution().isBatchExecution()) {
				streamExecEnv = null;
				execEnv = createExecutionEnvironment();
				tableEnv = BatchTableEnvironment.create(execEnv);
			} else {
				throw new SqlExecutionException("Unsupported execution type specified.");
			}

			// create query config
			queryConfig = createQueryConfig();

			// register table sources
			tableSources.forEach(tableEnv::registerTableSource);

			// register table sinks
			tableSinks.forEach(tableEnv::registerTableSink);

			// register user-defined functions
			registerFunctions();

			// register views and temporal tables in specified order
			mergedEnv.getTables().forEach((name, entry) -> {
				// if registering a view fails at this point,
				// it means that it accesses tables that are not available anymore
				if (entry instanceof ViewEntry) {
					final ViewEntry viewEntry = (ViewEntry) entry;
					registerView(viewEntry);
				} else if (entry instanceof TemporalTableEntry) {
					final TemporalTableEntry temporalTableEntry = (TemporalTableEntry) entry;
					registerTemporalTable(temporalTableEntry);
				}
			});
		}

		public QueryConfig getQueryConfig() {
			return queryConfig;
		}

		public ExecutionEnvironment getExecutionEnvironment() {
			return execEnv;
		}

		public StreamExecutionEnvironment getStreamExecutionEnvironment() {
			return streamExecEnv;
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

		public JobGraph createJobGraph(String name) {
			final FlinkPlan plan = createPlan(name, flinkConfig);
			return ClusterClient.getJobGraph(
				flinkConfig,
				plan,
				dependencies,
				runOptions.getClasspaths(),
				runOptions.getSavepointRestoreSettings());
		}

		private FlinkPlan createPlan(String name, Configuration flinkConfig) {
			if (streamExecEnv != null) {
				final StreamGraph graph = streamExecEnv.getStreamGraph();
				graph.setJobName(name);
				return graph;
			} else {
				final int parallelism = execEnv.getParallelism();
				final Plan unoptimizedPlan = execEnv.createProgramPlan();
				unoptimizedPlan.setJobName(name);
				final Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);
				return ClusterClient.getOptimizedPlan(compiler, unoptimizedPlan, parallelism);
			}
		}

		private ExecutionEnvironment createExecutionEnvironment() {
			final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
			execEnv.setRestartStrategy(mergedEnv.getExecution().getRestartStrategy());
			execEnv.setParallelism(mergedEnv.getExecution().getParallelism());
			return execEnv;
		}

		private StreamExecutionEnvironment createStreamExecutionEnvironment() {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setRestartStrategy(mergedEnv.getExecution().getRestartStrategy());
			env.setParallelism(mergedEnv.getExecution().getParallelism());
			env.setMaxParallelism(mergedEnv.getExecution().getMaxParallelism());
			env.setStreamTimeCharacteristic(mergedEnv.getExecution().getTimeCharacteristic());
			if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
				env.getConfig().setAutoWatermarkInterval(mergedEnv.getExecution().getPeriodicWatermarksInterval());
			}
			return env;
		}

		private QueryConfig createQueryConfig() {
			if (streamExecEnv != null) {
				final StreamQueryConfig config = new StreamQueryConfig();
				final long minRetention = mergedEnv.getExecution().getMinStateRetention();
				final long maxRetention = mergedEnv.getExecution().getMaxStateRetention();
				config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
				return config;
			} else {
				return new BatchQueryConfig();
			}
		}

		private void registerFunctions() {
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
	}
}
