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
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);

	private static final String DEFAULT_ENV_FILE = "sql-client-defaults.yaml";

	// deployment

	private final Environment defaultEnvironment;
	private final List<URL> dependencies;
	private final Configuration flinkConfig;
	private final List<CustomCommandLine<?>> commandLines;
	private final Options commandLineOptions;

	// result maintenance

	private final ResultStore resultStore;

	/**
	 * Cached execution context for unmodified sessions. Do not access this variable directly
	 * but through {@link LocalExecutor#getOrCreateExecutionContext}.
	 */
	private ExecutionContext<?> executionContext;

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

			// initialize default file system
			try {
				FileSystem.initialize(this.flinkConfig);
			} catch (IOException e) {
				throw new SqlClientException(
					"Error while setting the default filesystem scheme from configuration.", e);
			}

			// load command lines for deployment
			this.commandLines = CliFrontend.loadCustomCommandLines(flinkConfig, flinkConfigDir);
			this.commandLineOptions = collectCommandLineOptions(commandLines);
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
					throw new SqlClientException(e);
				}
				LOG.info("Using default environment file: {}", defaultEnv);
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
		dependencies = discoverDependencies(jars, libraries);

		// prepare result store
		resultStore = new ResultStore(flinkConfig);
	}

	/**
	 * Constructor for testing purposes.
	 */
	public LocalExecutor(Environment defaultEnvironment, List<URL> dependencies, Configuration flinkConfig, CustomCommandLine<?> commandLine) {
		this.defaultEnvironment = defaultEnvironment;
		this.dependencies = dependencies;
		this.flinkConfig = flinkConfig;
		this.commandLines = Collections.singletonList(commandLine);
		this.commandLineOptions = collectCommandLineOptions(commandLines);

		// prepare result store
		resultStore = new ResultStore(flinkConfig);
	}

	@Override
	public void start() {
		// nothing to do yet
	}

	@Override
	public Map<String, String> getSessionProperties(SessionContext session) throws SqlExecutionException {
		final Environment env = getOrCreateExecutionContext(session)
			.getMergedEnvironment();
		final Map<String, String> properties = new HashMap<>();
		properties.putAll(env.getExecution().toProperties());
		properties.putAll(env.getDeployment().toProperties());
		return properties;
	}

	@Override
	public List<String> listTables(SessionContext session) throws SqlExecutionException {
		final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
			.createEnvironmentInstance()
			.getTableEnvironment();
		return Arrays.asList(tableEnv.listTables());
	}

	@Override
	public TableSchema getTableSchema(SessionContext session, String name) throws SqlExecutionException {
		final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
			.createEnvironmentInstance()
			.getTableEnvironment();
		try {
			return tableEnv.scan(name).getSchema();
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("No table with this name could be found.", t);
		}
	}

	@Override
	public String explainStatement(SessionContext session, String statement) throws SqlExecutionException {
		final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
			.createEnvironmentInstance()
			.getTableEnvironment();

		// translate
		try {
			final Table table = createTable(tableEnv, statement);
			return tableEnv.explain(table);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	@Override
	public ResultDescriptor executeQuery(SessionContext session, String query) throws SqlExecutionException {
		final ExecutionContext<?> context = getOrCreateExecutionContext(session);
		return executeQueryInternal(context, query);
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
		return ((ChangelogResult<?>) result).retrieveChanges();
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
		return ((MaterializedResult<?>) result).snapshot(pageSize);
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
		return ((MaterializedResult<?>) result).retrievePage(page);
	}

	@Override
	public void cancelQuery(SessionContext session, String resultId) throws SqlExecutionException {
		final ExecutionContext<?> context = getOrCreateExecutionContext(session);
		cancelQueryInternal(context, resultId);
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

	private <T> void cancelQueryInternal(ExecutionContext<T> context, String resultId) {
		final DynamicResult<T> result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}

		// stop retrieval and remove the result
		LOG.info("Cancelling job {} and result retrieval.", resultId);
		result.close();
		resultStore.removeResult(resultId);

		// stop Flink job
		try (final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor()) {
			ClusterClient<T> clusterClient = null;
			try {
				// retrieve existing cluster
				clusterClient = clusterDescriptor.retrieve(context.getClusterId());
				try {
					clusterClient.cancel(new JobID(StringUtils.hexStringToByte(resultId)));
				} catch (Throwable t) {
					// the job might has finished earlier
				}
			} catch (Exception e) {
				throw new SqlExecutionException("Could not retrieve or create a cluster.", e);
			} finally {
				try {
					if (clusterClient != null) {
						clusterClient.shutdown();
					}
				} catch (Exception e) {
					// ignore
				}
			}
		} catch (SqlExecutionException e) {
			throw e;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not locate a cluster.", e);
		}
	}

	private <T> ResultDescriptor executeQueryInternal(ExecutionContext<T> context, String query) {
		final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();

		// create table
		final Table table = createTable(envInst.getTableEnvironment(), query);

		// initialize result
		final DynamicResult<T> result = resultStore.createResult(
			context.getMergedEnvironment(),
			table.getSchema().withoutTimeAttributes(),
			envInst.getExecutionConfig());

		// create job graph with dependencies
		final String jobName = context.getSessionContext().getName() + ": " + query;
		final JobGraph jobGraph;
		try {
			table.writeToSink(result.getTableSink(), envInst.getQueryConfig());
			jobGraph = envInst.createJobGraph(jobName);
		} catch (Throwable t) {
			// the result needs to be closed as long as
			// it not stored in the result store
			result.close();
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}

		// store the result with a unique id (the job id for now)
		final String resultId = jobGraph.getJobID().toString();
		resultStore.storeResult(resultId, result);

		// create execution
		final Runnable program = () -> {
			LOG.info("Submitting job {} for query {}`", jobGraph.getJobID(), jobName);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Submitting job {} with the following environment: \n{}",
					jobGraph.getJobID(), context.getMergedEnvironment());
			}
			deployJob(context, jobGraph, result);
		};

		// start result retrieval
		result.startRetrieval(program);

		return new ResultDescriptor(
			resultId,
			table.getSchema().withoutTimeAttributes(),
			result.isMaterialized());
	}

	private Table createTable(TableEnvironment tableEnv, String query) {
		// parse and validate query
		try {
			return tableEnv.sqlQuery(query);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	/**
	 * Creates or reuses the execution context.
	 */
	private synchronized ExecutionContext<?> getOrCreateExecutionContext(SessionContext session) throws SqlExecutionException {
		if (executionContext == null || !executionContext.getSessionContext().equals(session)) {
			try {
				executionContext = new ExecutionContext<>(defaultEnvironment, session, dependencies,
					flinkConfig, commandLineOptions, commandLines);
			} catch (Throwable t) {
				// catch everything such that a configuration does not crash the executor
				throw new SqlExecutionException("Could not create execution context.", t);
			}
		}
		return executionContext;
	}

	/**
	 * Deploys a job. Depending on the deployment create a new job cluster. It saves cluster id in
	 * the result and blocks until job completion.
	 */
	private <T> void deployJob(ExecutionContext<T> context, JobGraph jobGraph, DynamicResult<T> result) {
		// create or retrieve cluster and deploy job
		try (final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor()) {
			ClusterClient<T> clusterClient = null;
			try {
				// new cluster
				if (context.getClusterId() == null) {
					// deploy job cluster with job attached
					clusterClient = clusterDescriptor.deployJobCluster(context.getClusterSpec(), jobGraph, false);
					// save the new cluster id
					result.setClusterId(clusterClient.getClusterId());
					// we need to hard cast for now
					((RestClusterClient<T>) clusterClient)
						.requestJobResult(jobGraph.getJobID())
						.get()
						.toJobExecutionResult(context.getClassLoader()); // throws exception if job fails
				}
				// reuse existing cluster
				else {
					// retrieve existing cluster
					clusterClient = clusterDescriptor.retrieve(context.getClusterId());
					// save the cluster id
					result.setClusterId(clusterClient.getClusterId());
					// submit the job
					clusterClient.setDetached(false);
					clusterClient.submitJob(jobGraph, context.getClassLoader()); // throws exception if job fails
				}
			} catch (Exception e) {
				throw new SqlExecutionException("Could not retrieve or create a cluster.", e);
			} finally {
				try {
					if (clusterClient != null) {
						clusterClient.shutdown();
					}
				} catch (Exception e) {
					// ignore
				}
			}
		} catch (SqlExecutionException e) {
			throw e;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not locate a cluster.", e);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static List<URL> discoverDependencies(List<URL> jars, List<URL> libraries) {
		final List<URL> dependencies = new ArrayList<>();
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

		if (LOG.isDebugEnabled()) {
			LOG.debug("Using the following dependencies: {}", dependencies);
		}

		return dependencies;
	}

	private static Options collectCommandLineOptions(List<CustomCommandLine<?>> commandLines) {
		final Options customOptions = new Options();
		for (CustomCommandLine<?> customCommandLine : commandLines) {
			customCommandLine.addRunOptions(customOptions);
		}
		return CliFrontendParser.mergeOptions(
			CliFrontendParser.getRunCommandOptions(),
			customOptions);
	}
}
