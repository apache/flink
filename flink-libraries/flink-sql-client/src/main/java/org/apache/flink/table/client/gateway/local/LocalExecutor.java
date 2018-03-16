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

import org.apache.flink.api.common.JobID;
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
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Deployment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.sinks.TableSink;
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

	private final Environment defaultEnvironment;
	private final List<URL> dependencies;
	private final Configuration flinkConfig;
	private final ResultStore resultStore;

	/**
	 * Cached execution context for unmodified sessions. Do not access this variable directly
	 * but through {@link LocalExecutor#getOrCreateExecutionContext}.
	 */
	private ExecutionContext executionContext;

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
				defaultEnvironment = Environment.parse(defaultEnv);
			} catch (IOException e) {
				throw new SqlClientException("Could not read default environment file at: " + defaultEnv, e);
			}
		} else {
			defaultEnvironment = new Environment();
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

	/**
	 * Constructor for testing purposes.
	 */
	public LocalExecutor(Environment defaultEnvironment, List<URL> dependencies, Configuration flinkConfig) {
		this.defaultEnvironment = defaultEnvironment;
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
	public Map<String, String> getSessionProperties(SessionContext session) throws SqlExecutionException {
		final Environment env = getOrCreateExecutionContext(session).getMergedEnvironment();
		final Map<String, String> properties = new HashMap<>();
		properties.putAll(env.getExecution().toProperties());
		properties.putAll(env.getDeployment().toProperties());
		return properties;
	}

	@Override
	public List<String> listTables(SessionContext session) throws SqlExecutionException {
		final TableEnvironment tableEnv = getOrCreateExecutionContext(session).getTableEnvironment();
		return Arrays.asList(tableEnv.listTables());
	}

	@Override
	public TableSchema getTableSchema(SessionContext session, String name) throws SqlExecutionException {
		final TableEnvironment tableEnv = getOrCreateExecutionContext(session).getTableEnvironment();
		try {
			return tableEnv.scan(name).getSchema();
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("No table with this name could be found.", t);
		}
	}

	@Override
	public String explainStatement(SessionContext session, String statement) throws SqlExecutionException {
		final ExecutionContext context = getOrCreateExecutionContext(session);

		// translate
		try {
			final Table table = createTable(context, statement);
			return context.getTableEnvironment().explain(table);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	@Override
	public ResultDescriptor executeQuery(SessionContext session, String query) throws SqlExecutionException {
		final ExecutionContext context = getOrCreateExecutionContext(session);
		final Environment mergedEnv = context.getMergedEnvironment();

		// create table here to fail quickly for wrong queries
		final Table table = createTable(context, query);
		final TableSchema resultSchema = table.getSchema().withoutTimeAttributes();

		// deployment
		final ClusterClient<?> clusterClient = createDeployment(mergedEnv.getDeployment());

		// initialize result
		final DynamicResult result = resultStore.createResult(
			mergedEnv,
			resultSchema,
			context.getExecutionConfig());

		// create job graph with jars
		final JobGraph jobGraph;
		try {
			jobGraph = createJobGraph(context, context.getSessionContext().getName() + ": " + query, table,
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

		// create execution
		final Runnable program = () -> {
			// we need to submit the job attached for now
			// otherwise it is not possible to retrieve the reason why an execution failed
			try {
				clusterClient.run(jobGraph, context.getClassLoader());
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

		return new ResultDescriptor(resultId, resultSchema, result.isMaterialized());
	}

	@Override
	public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(SessionContext session,
			String resultId) throws SqlExecutionException {
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
	public TypedResult<Integer> snapshotResult(SessionContext session, String resultId, int pageSize) throws SqlExecutionException {
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
	public void cancelQuery(SessionContext session, String resultId) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}

		// stop retrieval and remove the result
		result.close();
		resultStore.removeResult(resultId);

		// stop Flink job
		final Environment mergedEnv = getOrCreateExecutionContext(session).getMergedEnvironment();
		final ClusterClient<?> clusterClient = createDeployment(mergedEnv.getDeployment());
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
	public void stop(SessionContext session) {
		resultStore.getResults().forEach((resultId) -> {
			try {
				cancelQuery(session, resultId);
			} catch (Throwable t) {
				// ignore any throwable to keep the clean up running
			}
		});
	}

	// --------------------------------------------------------------------------------------------

	private Table createTable(ExecutionContext context, String query) {
		// parse and validate query
		try {
			return context.getTableEnvironment().sqlQuery(query);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	private JobGraph createJobGraph(ExecutionContext context, String name, Table table,
			TableSink<?> sink, ClusterClient<?> clusterClient) {

		// translate
		try {
			table.writeToSink(sink, context.getQueryConfig());
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}

		// extract plan
		final FlinkPlan plan = context.createPlan(name, clusterClient.getFlinkConfiguration());

		// create job graph
		return clusterClient.getJobGraph(
			plan,
			dependencies,
			Collections.emptyList(),
			SavepointRestoreSettings.none());
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

	/**
	 * Creates or reuses the execution context.
	 */
	private synchronized ExecutionContext getOrCreateExecutionContext(SessionContext session) throws SqlExecutionException {
		if (executionContext == null || !executionContext.getSessionContext().equals(session)) {
			try {
				executionContext = new ExecutionContext(defaultEnvironment, session, dependencies);
			} catch (Throwable t) {
				// catch everything such that a configuration does not crash the executor
				throw new SqlExecutionException("Could not create execution context.", t);
			}
		}
		return executionContext;
	}
}
