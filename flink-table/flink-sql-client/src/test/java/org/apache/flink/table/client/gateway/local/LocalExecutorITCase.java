/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.util.DummyCustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ExecutionEntry;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.SimpleCatalogFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Contains basic tests for the {@link LocalExecutor}.
 */
@RunWith(Parameterized.class)
public class LocalExecutorITCase extends TestLogger {

	@Parameters(name = "Planner: {0}")
	public static List<String> planner() {
		return Arrays.asList(
			ExecutionEntry.EXECUTION_PLANNER_VALUE_OLD,
			ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);
	}

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-catalogs.yaml";

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 2;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfig())
			.setNumberTaskManagers(NUM_TMS)
			.setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
			.build());

	private static ClusterClient<?> clusterClient;

	@BeforeClass
	public static void setup() {
		clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient();
	}

	private static Configuration getConfig() {
		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}

	@Parameter
	public String planner;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testValidateSession() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.validateSession(session);

		session.addView(ViewEntry.create("AdditionalView1", "SELECT 1"));
		session.addView(ViewEntry.create("AdditionalView2", "SELECT * FROM AdditionalView1"));
		executor.validateSession(session);

		List<String> actualTables = executor.listTables(session);
		List<String> expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2",
			"AdditionalView1",
			"AdditionalView2");
		assertEquals(expectedTables, actualTables);

		session.removeView("AdditionalView1");
		try {
			executor.validateSession(session);
			fail();
		} catch (SqlExecutionException e) {
			// AdditionalView2 needs AdditionalView1
		}

		session.removeView("AdditionalView2");
		executor.validateSession(session);

		actualTables = executor.listTables(session);
		expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2");
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testListCatalogs() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualCatalogs = executor.listCatalogs(session);

		final List<String> expectedCatalogs = Arrays.asList(
			"default_catalog",
			"catalog1",
			"simple-catalog");
		assertEquals(expectedCatalogs, actualCatalogs);
	}

	@Test
	public void testListDatabases() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualDatabases = executor.listDatabases(session);

		final List<String> expectedDatabases = Collections.singletonList("default_database");
		assertEquals(expectedDatabases, actualDatabases);
	}

	@Test
	public void testListTables() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualTables = executor.listTables(session);

		final List<String> expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2");
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testListUserDefinedFunctions() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualTables = executor.listUserDefinedFunctions(session);

		final List<String> expectedTables = Arrays.asList("aggregateUDF", "tableUDF", "scalarUDF");
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testGetSessionProperties() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		session.setSessionProperty("execution.result-mode", "changelog");

		executor.getSessionProperties(session);

		// modify defaults
		session.setSessionProperty("execution.result-mode", "table");

		final Map<String, String> actualProperties = executor.getSessionProperties(session);

		final Map<String, String> expectedProperties = new HashMap<>();
		expectedProperties.put("execution.planner", planner);
		expectedProperties.put("execution.type", "batch");
		expectedProperties.put("execution.time-characteristic", "event-time");
		expectedProperties.put("execution.periodic-watermarks-interval", "99");
		expectedProperties.put("execution.parallelism", "1");
		expectedProperties.put("execution.max-parallelism", "16");
		expectedProperties.put("execution.max-idle-state-retention", "0");
		expectedProperties.put("execution.min-idle-state-retention", "0");
		expectedProperties.put("execution.result-mode", "table");
		expectedProperties.put("execution.max-table-result-rows", "100");
		expectedProperties.put("execution.restart-strategy.type", "failure-rate");
		expectedProperties.put("execution.restart-strategy.max-failures-per-interval", "10");
		expectedProperties.put("execution.restart-strategy.failure-rate-interval", "99000");
		expectedProperties.put("execution.restart-strategy.delay", "1000");
		expectedProperties.put("table.optimizer.join-reorder-enabled", "false");
		expectedProperties.put("deployment.response-timeout", "5000");

		assertEquals(expectedProperties, actualProperties);
	}

	@Test
	public void testTableSchema() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final TableSchema actualTableSchema = executor.getTableSchema(session, "TableNumber2");

		final TableSchema expectedTableSchema = new TableSchema(
			new String[] {"IntegerField2", "StringField2"},
			new TypeInformation[] {Types.INT, Types.STRING});

		assertEquals(expectedTableSchema, actualTableSchema);
	}

	@Test
	public void testCompleteStatement() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> expectedTableHints = Arrays.asList(
			"default_catalog.default_database.TableNumber1",
			"default_catalog.default_database.TableNumber2",
			"default_catalog.default_database.TableSourceSink");
		assertEquals(expectedTableHints, executor.completeStatement(session, "SELECT * FROM Ta", 16));

		final List<String> expectedClause = Collections.singletonList("WHERE");
		assertEquals(expectedClause, executor.completeStatement(session, "SELECT * FROM TableNumber2 WH", 29));

		final List<String> expectedField = Arrays.asList("IntegerField1");
		assertEquals(expectedField, executor.completeStatement(session, "SELECT * FROM TableNumber1 WHERE Inte", 37));
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionChangelog() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(
				session,
				"SELECT scalarUDF(IntegerField1), StringField1 FROM TableNumber1");

			assertFalse(desc.isMaterialized());

			final List<String> actualResults =
					retrieveChangelogResult(executor, session, desc.getResultId());

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("(true,47,Hello World)");
			expectedResults.add("(true,27,Hello World)");
			expectedResults.add("(true,37,Hello World)");
			expectedResults.add("(true,37,Hello World)");
			expectedResults.add("(true,47,Hello World)");
			expectedResults.add("(true,57,Hello World!!!!)");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionTable() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final String query = "SELECT scalarUDF(IntegerField1), StringField1 FROM TableNumber1";

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("47,Hello World");
		expectedResults.add("27,Hello World");
		expectedResults.add("37,Hello World");
		expectedResults.add("37,Hello World");
		expectedResults.add("47,Hello World");
		expectedResults.add("57,Hello World!!!!");

		executeStreamQueryTable(replaceVars, query, expectedResults);
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionLimitedTable() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "1");

		final String query = "SELECT COUNT(*), StringField1 FROM TableNumber1 GROUP BY StringField1";

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("1,Hello World!!!!");

		executeStreamQueryTable(replaceVars, query, expectedResults);
	}

	@Test(timeout = 30_000L)
	public void testBatchQueryExecution() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT * FROM TestView1");

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = retrieveTableResult(executor, session, desc.getResultId());

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("47");
			expectedResults.add("27");
			expectedResults.add("37");
			expectedResults.add("37");
			expectedResults.add("47");
			expectedResults.add("57");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionSink() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// Case 1: Registered sink
			final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				session,
				"INSERT INTO TableSourceSink SELECT IntegerField1 = 42, StringField1 FROM TableNumber1");

			// wait for job completion and verify result
			boolean isRunning = true;
			while (isRunning) {
				Thread.sleep(50); // slow the processing down
				final JobStatus jobStatus = clusterClient.getJobStatus(JobID.fromHexString(targetDescriptor.getJobId())).get();
				switch (jobStatus) {
					case CREATED:
					case RUNNING:
						continue;
					case FINISHED:
						isRunning = false;
						verifySinkResult(csvOutputPath);
						break;
					default:
						fail("Unexpected job status.");
				}
			}

			// Case 2: Temporary sink
			session.setCurrentCatalog("simple-catalog");
			session.setCurrentDatabase("default_database");
			// all queries are pipelined to an in-memory sink, check it is properly registered
			final ResultDescriptor otherCatalogDesc = executor.executeQuery(session, "SELECT * FROM `test-table`");

			final List<String> otherCatalogResults = retrieveTableResult(
				executor,
				session,
				otherCatalogDesc.getResultId());

			TestBaseUtils.compareResultCollections(
				SimpleCatalogFactory.TABLE_CONTENTS.stream().map(Row::toString).collect(Collectors.toList()),
				otherCatalogResults,
				Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	@Test
	public void testUseCatalogAndUseDatabase() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(CATALOGS_ENVIRONMENT_FILE, clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			assertEquals(Arrays.asList("mydatabase"), executor.listDatabases(session));

			executor.useCatalog(session, "hivecatalog");

			assertEquals(
				Arrays.asList(DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE, HiveCatalog.DEFAULT_DB),
				executor.listDatabases(session));

			assertEquals(Collections.emptyList(), executor.listTables(session));

			executor.useDatabase(session, DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE);

			assertEquals(Arrays.asList(DependencyTest.TestHiveCatalogFactory.TEST_TABLE), executor.listTables(session));
		} finally {
			executor.stop(session);
		}
	}

	@Test
	public void testUseNonExistingDatabase() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		exception.expect(SqlExecutionException.class);
		executor.useDatabase(session, "nonexistingdb");
	}

	@Test
	public void testUseNonExistingCatalog() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		exception.expect(SqlExecutionException.class);
		executor.useCatalog(session, "nonexistingcatalog");
	}

	private void executeStreamQueryTable(
			Map<String, String> replaceVars,
			String query,
			List<String> expectedResults) throws Exception {

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(session, query);

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = retrieveTableResult(executor, session, desc.getResultId());

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	private void verifySinkResult(String path) throws IOException {
		final List<String> actualResults = new ArrayList<>();
		TestBaseUtils.readAllResultLines(actualResults, path);
		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("true,Hello World");
		expectedResults.add("false,Hello World");
		expectedResults.add("false,Hello World");
		expectedResults.add("false,Hello World");
		expectedResults.add("true,Hello World");
		expectedResults.add("false,Hello World!!!!");
		TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
	}

	private <T> LocalExecutor createDefaultExecutor(ClusterClient<T> clusterClient) throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		return new LocalExecutor(
			EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
			Collections.emptyList(),
			clusterClient.getFlinkConfiguration(),
			new DummyCustomCommandLine<T>(clusterClient));
	}

	private <T> LocalExecutor createModifiedExecutor(ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
		return new LocalExecutor(
			EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
			Collections.emptyList(),
			clusterClient.getFlinkConfiguration(),
			new DummyCustomCommandLine<T>(clusterClient));
	}

	private <T> LocalExecutor createModifiedExecutor(
			String yamlFile, ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
		return new LocalExecutor(
			EnvironmentFileUtil.parseModified(yamlFile, replaceVars),
			Collections.emptyList(),
			clusterClient.getFlinkConfiguration(),
			new DummyCustomCommandLine<T>(clusterClient));
	}

	private List<String> retrieveTableResult(
			Executor executor,
			SessionContext session,
			String resultID) throws InterruptedException {

		final List<String> actualResults = new ArrayList<>();
		while (true) {
			Thread.sleep(50); // slow the processing down
			final TypedResult<Integer> result = executor.snapshotResult(session, resultID, 2);
			if (result.getType() == TypedResult.ResultType.PAYLOAD) {
				actualResults.clear();
				IntStream.rangeClosed(1, result.getPayload()).forEach((page) -> {
					for (Row row : executor.retrieveResultPage(resultID, page)) {
						actualResults.add(row.toString());
					}
				});
			} else if (result.getType() == TypedResult.ResultType.EOS) {
				break;
			}
		}

		return actualResults;
	}

	private List<String> retrieveChangelogResult(
			Executor executor,
			SessionContext session,
			String resultID) throws InterruptedException {

		final List<String> actualResults = new ArrayList<>();
		while (true) {
			Thread.sleep(50); // slow the processing down
			final TypedResult<List<Tuple2<Boolean, Row>>> result =
					executor.retrieveResultChanges(session, resultID);
			if (result.getType() == TypedResult.ResultType.PAYLOAD) {
				for (Tuple2<Boolean, Row> change : result.getPayload()) {
					actualResults.add(change.toString());
				}
			} else if (result.getType() == TypedResult.ResultType.EOS) {
				break;
			}
		}
		return actualResults;
	}
}
