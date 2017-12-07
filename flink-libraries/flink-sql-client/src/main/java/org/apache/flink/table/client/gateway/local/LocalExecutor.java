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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Deployment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.Execution;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutor implements Executor {

	private static final String DEFAULT_ENV_FILE = "sql-client-defaults.yaml";

	private final Environment environment;
	private final List<URL> dependencies;
	private final Configuration flinkConfig;
	private final ResultStore resultStore;

	/**
	 * Creates a local executor for submitting table programs and retrieving results.
	 */
	public LocalExecutor(URL defaultEnv, List<URL> jars, List<URL> libraries) {
		// discover configuration
		final String flinkConfigDir;
		try {
			// find the configuration directory
			flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

			// load the global configuration
			this.flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);
		} catch (Exception e) {
			throw new SqlClientException("Could not load Flink configuration.", e);
		}

		// try to find a default environment
		if (defaultEnv == null) {
			final String defaultFilePath = flinkConfigDir + "/" + DEFAULT_ENV_FILE;
			System.out.println("No default environment specified.");
			System.out.print("Searching for '" + defaultFilePath + "'...");
			final File file = new File(defaultFilePath);
			if (file.exists()) {
				System.out.println("found.");
				try {
					defaultEnv = Path.fromLocalFile(file).toUri().toURL();
				} catch (MalformedURLException e) {
					throw new RuntimeException(e);
				}
			} else {
				System.out.println("not found.");
			}
		}

		// inform user
		if (defaultEnv != null) {
			System.out.println("Reading default environment from: " + defaultEnv);
			try {
				environment = Environment.parse(defaultEnv);
			} catch (IOException e) {
				throw new SqlClientException("Could not read default environment file at: " + defaultEnv, e);
			}
		} else {
			environment = new Environment();
		}

		// discover dependencies
		dependencies = new ArrayList<>();
		try {
			// find jar files
			for (URL url : jars) {
				JobWithJars.checkJarFile(url);
				dependencies.add(url);
			}

			// find jar files in library directories
			for (URL libUrl : libraries) {
				final File dir = new File(libUrl.toURI());
				if (!dir.isDirectory()) {
					throw new SqlClientException("Directory expected: " + dir);
				} else if (!dir.canRead()) {
					throw new SqlClientException("Directory cannot be read: " + dir);
				}
				final File[] files = dir.listFiles();
				if (files == null) {
					throw new SqlClientException("Directory cannot be read: " + dir);
				}
				for (File f : files) {
					// only consider jars
					if (f.isFile() && f.getAbsolutePath().toLowerCase().endsWith(".jar")) {
						final URL url = f.toURI().toURL();
						JobWithJars.checkJarFile(url);
						dependencies.add(url);
					}
				}
			}
		} catch (Exception e) {
			throw new SqlClientException("Could not load all required JAR files.", e);
		}

		// prepare result store
		resultStore = new ResultStore(flinkConfig);
	}

	public LocalExecutor(Environment environment, List<URL> dependencies, Configuration flinkConfig) {
		this.environment = environment;
		this.dependencies = dependencies;
		this.flinkConfig = flinkConfig;

		// prepare result store
		resultStore = new ResultStore(flinkConfig);
	}

	@Override
	public void start() {
		// nothing to do yet
	}

	@Override
	public Map<String, String> getSessionProperties(SessionContext context) throws SqlExecutionException {
		final Environment env = createEnvironment(context);
		final Map<String, String> properties = new HashMap<>();
		properties.putAll(env.getExecution().toProperties());
		properties.putAll(env.getDeployment().toProperties());
		return properties;
	}

	@Override
	public List<String> listTables(SessionContext context) throws SqlExecutionException {
		final Environment env = createEnvironment(context);
		final TableEnvironment tableEnv = createTableEnvironment(env);
		return Arrays.asList(tableEnv.listTables());
	}

	@Override
	public TableSchema getTableSchema(SessionContext context, String name) throws SqlExecutionException {
		final Environment env = createEnvironment(context);
		final TableEnvironment tableEnv = createTableEnvironment(env);
		try {
			return tableEnv.scan(name).getSchema();
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("No table with this name could be found.", t);
		}
	}

	@Override
	public String explainStatement(SessionContext context, String statement) throws SqlExecutionException {
		final Environment env = createEnvironment(context);

		// translate
		try {
			final Tuple2<TableEnvironment, Table> table = createTable(env, statement);
			return table.f0.explain(table.f1);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	@Override
	public ResultDescriptor executeQuery(SessionContext context, String query) throws SqlExecutionException {
		final Environment env = createEnvironment(context);

		// create table here to fail quickly for wrong queries
		final Tuple2<TableEnvironment, Table> table = createTable(env, query);

		// deployment
		final ClusterClient<?> clusterClient = createDeployment(env.getDeployment());

		// initialize result
		final DynamicResult result = resultStore.createResult(
			env,
			table.f1.getSchema(),
			getExecutionConfig(table.f0));

		// create job graph with jars
		final JobGraph jobGraph;
		try {
			jobGraph = createJobGraph(
				context.getName() + ": " + query,
				env.getExecution(),
				table.f0,
				table.f1,
				result.getTableSink(),
				clusterClient);
		} catch (Throwable t) {
			// the result needs to be closed as long as
			// it not stored in the result store
			result.close();
			throw t;
		}

		// store the result with a unique id (the job id for now)
		final String resultId = jobGraph.getJobID().toString();
		resultStore.storeResult(resultId, result);

		// create class loader
		final ClassLoader classLoader = JobWithJars.buildUserCodeClassLoader(
			dependencies,
			Collections.emptyList(),
			this.getClass().getClassLoader());

		// create execution
		final Runnable program = () -> {
			// we need to submit the job attached for now
			// otherwise it is not possible to retrieve the reason why an execution failed
			try {
				clusterClient.run(jobGraph, classLoader);
			} catch (ProgramInvocationException e) {
				throw new SqlExecutionException("Could not execute table program.", e);
			} finally {
				try {
					clusterClient.shutdown();
				} catch (Exception e) {
					// ignore
				}
			}
		};

		// start result retrieval
		result.startRetrieval(program);

		return new ResultDescriptor(resultId, table.f1.getSchema(), result.isMaterialized());
	}

	@Override
	public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(SessionContext context, String resultId) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		if (result.isMaterialized()) {
			throw new SqlExecutionException("Invalid result retrieval mode.");
		}
		return ((ChangelogResult) result).retrieveChanges();
	}

	@Override
	public TypedResult<Integer> snapshotResult(SessionContext context, String resultId, int pageSize) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		if (!result.isMaterialized()) {
			throw new SqlExecutionException("Invalid result retrieval mode.");
		}
		return ((MaterializedResult) result).snapshot(pageSize);
	}

	@Override
	public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		if (!result.isMaterialized()) {
			throw new SqlExecutionException("Invalid result retrieval mode.");
		}
		return ((MaterializedResult) result).retrievePage(page);
	}

	@Override
	public void cancelQuery(SessionContext context, String resultId) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}

		// stop retrieval and remove the result
		result.close();
		resultStore.removeResult(resultId);

		// stop Flink job
		final Environment env = createEnvironment(context);
		final ClusterClient<?> clusterClient = createDeployment(env.getDeployment());
		try {
			clusterClient.cancel(new JobID(StringUtils.hexStringToByte(resultId)));
		} catch (Throwable t) {
			// the job might has finished earlier
		} finally {
			try {
				clusterClient.shutdown();
			} catch (Throwable t) {
				// ignore
			}
		}
	}

	@Override
	public void stop(SessionContext context) {
		resultStore.getResults().forEach((resultId) -> {
			try {
				cancelQuery(context, resultId);
			} catch (Throwable t) {
				// ignore any throwable to keep the clean up running
			}
		});
	}

	// --------------------------------------------------------------------------------------------

	private Tuple2<TableEnvironment, Table> createTable(Environment env, String query) {
		final TableEnvironment tableEnv = createTableEnvironment(env);

		// parse and validate query
		try {
			return Tuple2.of(tableEnv, tableEnv.sqlQuery(query));
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	private JobGraph createJobGraph(String name, Execution exec, TableEnvironment tableEnv,
		Table table, TableSink<?> sink, ClusterClient<?> clusterClient) {

		final QueryConfig queryConfig = createQueryConfig(exec);

		// translate
		try {
			table.writeToSink(sink, queryConfig);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}

		// extract plan
		final FlinkPlan plan;
		if (exec.isStreamingExecution()) {
			final StreamGraph graph = ((StreamTableEnvironment) tableEnv).execEnv().getStreamGraph();
			graph.setJobName(name);
			plan = graph;
		} else {
			final int parallelism = exec.getParallelism();
			final Plan unoptimizedPlan = ((BatchTableEnvironment) tableEnv).execEnv().createProgramPlan();
			unoptimizedPlan.setJobName(name);
			final Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(),
				clusterClient.getFlinkConfiguration());
			plan = ClusterClient.getOptimizedPlan(compiler, unoptimizedPlan, parallelism);
		}

		// create job graph
		return clusterClient.getJobGraph(
			plan,
			dependencies,
			Collections.emptyList(),
			SavepointRestoreSettings.none());
	}

	@SuppressWarnings("unchecked")
	private ExecutionConfig getExecutionConfig(TableEnvironment tableEnv) {
		if (tableEnv instanceof StreamTableEnvironment) {
			return ((StreamTableEnvironment) tableEnv).execEnv().getConfig();
		} else {
			return ((BatchTableEnvironment) tableEnv).execEnv().getConfig();
		}
	}

	private ClusterClient<?> createDeployment(Deployment deploy) {

		// change some configuration options for being more responsive
		flinkConfig.setString(AkkaOptions.LOOKUP_TIMEOUT, deploy.getResponseTimeout() + " ms");
		flinkConfig.setString(AkkaOptions.CLIENT_TIMEOUT, deploy.getResponseTimeout() + " ms");

		// get cluster client
		final ClusterClient<?> clusterClient;
		if (deploy.isStandaloneDeployment()) {
			clusterClient = createStandaloneClusterClient(flinkConfig);
			clusterClient.setPrintStatusDuringExecution(false);
		} else {
			throw new SqlExecutionException("Unsupported deployment.");
		}

		return clusterClient;
	}

	private ClusterClient<?> createStandaloneClusterClient(Configuration configuration) {
		final ClusterDescriptor<StandaloneClusterId> descriptor = new StandaloneClusterDescriptor(configuration);
		try {
			return descriptor.retrieve(StandaloneClusterId.getInstance());
		} catch (ClusterRetrieveException e) {
			throw new SqlExecutionException("Could not retrievePage standalone cluster.", e);
		}
	}

	private Environment createEnvironment(SessionContext context) {
		return Environment.merge(environment, context.getEnvironment());
	}

	private TableEnvironment createTableEnvironment(Environment env) {
		try {
			final TableEnvironment tableEnv;
			if (env.getExecution().isStreamingExecution()) {
				final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
				execEnv.setParallelism(env.getExecution().getParallelism());
				execEnv.setMaxParallelism(env.getExecution().getMaxParallelism());
				tableEnv = StreamTableEnvironment.getTableEnvironment(execEnv);
			} else {
				final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
				execEnv.setParallelism(env.getExecution().getParallelism());
				tableEnv = BatchTableEnvironment.getTableEnvironment(execEnv);
			}

			env.getSources().forEach((name, source) -> {
				TableSource<?> tableSource = TableSourceFactoryService.findTableSourceFactory(source);
				tableEnv.registerTableSource(name, tableSource);
			});

			return tableEnv;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not create table environment.", e);
		}
	}

	private QueryConfig createQueryConfig(Execution exec) {
		if (exec.isStreamingExecution()) {
			final StreamQueryConfig config = new StreamQueryConfig();
			final long minRetention = exec.getMinStateRetention();
			final long maxRetention = exec.getMaxStateRetention();
			config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
			return config;
		} else {
			return new BatchQueryConfig();
		}
	}
}
