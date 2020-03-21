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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ExecutionEntry;
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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import java.util.Optional;
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
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
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
	public void testViews() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		executor.addView(sessionId, "AdditionalView1", "SELECT 1");
		executor.addView(sessionId, "AdditionalView2", "SELECT * FROM AdditionalView1");

		List<String> actualTables = executor.listTables(sessionId);
		List<String> expectedTables = Arrays.asList(
				"AdditionalView1",
				"AdditionalView2",
				"TableNumber1",
				"TableNumber2",
				"TableSourceSink",
				"TestView1",
				"TestView2");
		assertEquals(expectedTables, actualTables);

		try {
			executor.removeView(sessionId, "AdditionalView1");
			fail();
		} catch (SqlExecutionException e) {
			// AdditionalView2 needs AdditionalView1
		}

		executor.removeView(sessionId, "AdditionalView2");

		executor.removeView(sessionId, "AdditionalView1");

		actualTables = executor.listTables(sessionId);
		expectedTables = Arrays.asList(
				"TableNumber1",
				"TableNumber2",
				"TableSourceSink",
				"TestView1",
				"TestView2");
		assertEquals(expectedTables, actualTables);

		executor.closeSession(sessionId);
	}

	@Test
	public void testListCatalogs() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> actualCatalogs = executor.listCatalogs(sessionId);

		final List<String> expectedCatalogs = Arrays.asList(
			"catalog1",
			"default_catalog",
			"simple-catalog");
		assertEquals(expectedCatalogs, actualCatalogs);

		executor.closeSession(sessionId);
	}

	@Test
	public void testListDatabases() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> actualDatabases = executor.listDatabases(sessionId);

		final List<String> expectedDatabases = Collections.singletonList("default_database");
		assertEquals(expectedDatabases, actualDatabases);

		executor.closeSession(sessionId);
	}

	@Test
	public void testCreateDatabase() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		executor.executeUpdate(sessionId, "create database db1");

		final List<String> actualDatabases = executor.listDatabases(sessionId);
		final List<String> expectedDatabases = Arrays.asList("default_database", "db1");
		assertEquals(expectedDatabases, actualDatabases);

		executor.closeSession(sessionId);
	}

	@Test
	public void testDropDatabase() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		executor.executeUpdate(sessionId, "create database db1");

		List<String> actualDatabases = executor.listDatabases(sessionId);
		List<String> expectedDatabases = Arrays.asList("default_database", "db1");
		assertEquals(expectedDatabases, actualDatabases);

		executor.executeUpdate(sessionId, "drop database if exists db1");

		actualDatabases = executor.listDatabases(sessionId);
		expectedDatabases = Arrays.asList("default_database");
		assertEquals(expectedDatabases, actualDatabases);

		executor.closeSession(sessionId);
	}

	@Test
	public void testAlterDatabase() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		executor.executeUpdate(sessionId, "create database db1 comment 'db1_comment' with ('k1' = 'v1')");

		executor.executeUpdate(sessionId, "alter database db1 set ('k1' = 'a', 'k2' = 'b')");

		final List<String> actualDatabases = executor.listDatabases(sessionId);
		final List<String> expectedDatabases = Arrays.asList("default_database", "db1");
		assertEquals(expectedDatabases, actualDatabases);
		//todo: we should compare the new db1 properties after we support describe database in LocalExecutor.

		executor.closeSession(sessionId);
	}

	@Test
	public void testAlterTable() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final LocalExecutor localExecutor = (LocalExecutor) executor;
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);
		executor.useCatalog(sessionId, "simple-catalog");
		executor.useDatabase(sessionId, "default_database");
		List<String> actualTables = executor.listTables(sessionId);
		List<String> expectedTables = Arrays.asList("test-table");
		assertEquals(expectedTables, actualTables);
		executor.executeUpdate(sessionId, "alter table `test-table` rename to t1");
		actualTables = executor.listTables(sessionId);
		expectedTables = Arrays.asList("t1");
		assertEquals(expectedTables, actualTables);
		//todo: we should add alter table set test when we support create table in executor.
		executor.closeSession(sessionId);
	}

	@Test
	public void testListTables() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> actualTables = executor.listTables(sessionId);

		final List<String> expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2");
		assertEquals(expectedTables, actualTables);
		executor.closeSession(sessionId);
	}

	@Test
	public void testListUserDefinedFunctions() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> actualTables = executor.listUserDefinedFunctions(sessionId);

		final List<String> expectedTables = Arrays.asList("aggregateudf", "tableudf", "scalarudf");
		assertEquals(expectedTables, actualTables);

		executor.closeSession(sessionId);
	}

	@Test
	public void testGetSessionProperties() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);

		final SessionContext session = new SessionContext("test-session", new Environment());
		session.getSessionEnv().setExecution(ImmutableMap.of("result-mode", "changelog"));
		// Open the session and get the sessionId.
		String sessionId = executor.openSession(session);
		try {
			assertEquals("test-session", sessionId);
			assertEquals(executor.getSessionProperties(sessionId).get("execution.result-mode"), "changelog");

			// modify defaults
			executor.setSessionProperty(sessionId, "execution.result-mode", "table");

			final Map<String, String> actualProperties = executor.getSessionProperties(sessionId);

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

			// Reset session properties
			executor.resetSessionProperties(sessionId);
			assertEquals(executor.getSessionProperties(sessionId).get("execution.result-mode"), "changelog");
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testTableSchema() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final TableSchema actualTableSchema = executor.getTableSchema(sessionId, "TableNumber2");

		final TableSchema expectedTableSchema = new TableSchema(
			new String[]{"IntegerField2", "StringField2", "TimestampField2"},
			new TypeInformation[]{Types.INT, Types.STRING, Types.SQL_TIMESTAMP});

		assertEquals(expectedTableSchema, actualTableSchema);
		executor.closeSession(sessionId);
	}

	@Test
	public void testCompleteStatement() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> expectedTableHints = Arrays.asList(
			"default_catalog.default_database.TableNumber1",
			"default_catalog.default_database.TableNumber2",
			"default_catalog.default_database.TableSourceSink");
		assertEquals(expectedTableHints, executor.completeStatement(sessionId, "SELECT * FROM Ta", 16));

		final List<String> expectedClause = Collections.singletonList("WHERE");
		assertEquals(expectedClause, executor.completeStatement(sessionId, "SELECT * FROM TableNumber2 WH", 29));

		final List<String> expectedField = Arrays.asList("IntegerField1");
		assertEquals(expectedField, executor.completeStatement(sessionId, "SELECT * FROM TableNumber1 WHERE Inte", 37));
		executor.closeSession(sessionId);
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
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(
				sessionId,
				"SELECT scalarUDF(IntegerField1), StringField1, 'ABC' FROM TableNumber1");

			assertFalse(desc.isMaterialized());

			final List<String> actualResults =
				retrieveChangelogResult(executor, sessionId, desc.getResultId());

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("(true,47,Hello World,ABC)");
			expectedResults.add("(true,27,Hello World,ABC)");
			expectedResults.add("(true,37,Hello World,ABC)");
			expectedResults.add("(true,37,Hello World,ABC)");
			expectedResults.add("(true,47,Hello World,ABC)");
			expectedResults.add("(true,57,Hello World!!!!,ABC)");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test(timeout = 90_000L)
	public void testStreamQueryExecutionChangelogMultipleTimes() throws Exception {
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
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("(true,47,Hello World)");
		expectedResults.add("(true,27,Hello World)");
		expectedResults.add("(true,37,Hello World)");
		expectedResults.add("(true,37,Hello World)");
		expectedResults.add("(true,47,Hello World)");
		expectedResults.add("(true,57,Hello World!!!!)");

		try {
			for (int i = 0; i < 3; i++) {
				// start job and retrieval
				final ResultDescriptor desc = executor.executeQuery(
						sessionId,
						"SELECT scalarUDF(IntegerField1), StringField1 FROM TableNumber1");

				assertFalse(desc.isMaterialized());

				final List<String> actualResults =
						retrieveChangelogResult(executor, sessionId, desc.getResultId());

				TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
			}
		} finally {
			executor.closeSession(sessionId);
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

		final String query = "SELECT scalarUDF(IntegerField1), StringField1, 'ABC' FROM TableNumber1";

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("47,Hello World,ABC");
		expectedResults.add("27,Hello World,ABC");
		expectedResults.add("37,Hello World,ABC");
		expectedResults.add("37,Hello World,ABC");
		expectedResults.add("47,Hello World,ABC");
		expectedResults.add("57,Hello World!!!!,ABC");

		executeStreamQueryTable(replaceVars, query, expectedResults);
	}

	@Test(timeout = 90_000L)
	public void testStreamQueryExecutionTableMultipleTimes() throws Exception {
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

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			for (int i = 0; i < 3; i++) {
				executeStreamQueryTable(replaceVars, query, expectedResults);
			}
		} finally {
			executor.closeSession(sessionId);
		}
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
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			final ResultDescriptor desc = executor.executeQuery(sessionId, "SELECT *, 'ABC' FROM TestView1");

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = retrieveTableResult(executor, sessionId, desc.getResultId());

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("47,ABC");
			expectedResults.add("27,ABC");
			expectedResults.add("37,ABC");
			expectedResults.add("37,ABC");
			expectedResults.add("47,ABC");
			expectedResults.add("57,ABC");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test(timeout = 90_000L)
	public void testBatchQueryExecutionMultipleTimes() throws Exception {
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
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("47");
		expectedResults.add("27");
		expectedResults.add("37");
		expectedResults.add("37");
		expectedResults.add("47");
		expectedResults.add("57");

		try {
			for (int i = 0; i < 3; i++) {
				final ResultDescriptor desc = executor.executeQuery(sessionId, "SELECT * FROM TestView1");

				assertTrue(desc.isMaterialized());

				final List<String> actualResults = retrieveTableResult(executor, sessionId, desc.getResultId());

				TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
			}
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test(timeout = 30_000L)
	public void ensureExceptionOnFaultySourceInStreamingChangelogMode() throws Exception {
		final String missingFileName = "missing-source";

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", "missing-source");
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "none");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		Optional<Throwable> throwableWithMessage = Optional.empty();
		try {
			final ResultDescriptor desc = executor.executeQuery(sessionId, "SELECT * FROM TestView1");
			retrieveChangelogResult(executor, sessionId, desc.getResultId());
		} catch (SqlExecutionException e) {
			throwableWithMessage = findMissingFileException(e, missingFileName);
		} finally {
			executor.closeSession(sessionId);
		}
		assertTrue(throwableWithMessage.isPresent());
	}

	@Test(timeout = 30_000L)
	public void ensureExceptionOnFaultySourceInStreamingTableMode() throws Exception {
		final String missingFileName = "missing-source";

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", missingFileName);
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "1");
		replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "none");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		Optional<Throwable> throwableWithMessage = Optional.empty();
		try {
			final ResultDescriptor desc = executor.executeQuery(sessionId, "SELECT * FROM TestView1");
			retrieveTableResult(executor, sessionId, desc.getResultId());
		} catch (SqlExecutionException e) {
			throwableWithMessage = findMissingFileException(e, missingFileName);
		} finally {
			executor.closeSession(sessionId);
		}
		assertTrue(throwableWithMessage.isPresent());
	}

	@Test(timeout = 30_000L)
	public void ensureExceptionOnFaultySourceInBatch() throws Exception {
		final String missingFileName = "missing-source";

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", missingFileName);
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "none");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		Optional<Throwable> throwableWithMessage = Optional.empty();
		try {
			final ResultDescriptor desc = executor.executeQuery(sessionId, "SELECT * FROM TestView1");
			retrieveTableResult(executor, sessionId, desc.getResultId());
		} catch (SqlExecutionException e) {
			throwableWithMessage = findMissingFileException(e, missingFileName);
		} finally {
			executor.closeSession(sessionId);
		}
		assertTrue(throwableWithMessage.isPresent());
	}

	private Optional<Throwable> findMissingFileException(SqlExecutionException e, String filename) {
		Optional<Throwable> throwableWithMessage;

		// for "batch" sources
		throwableWithMessage = ExceptionUtils.findThrowableWithMessage(
				e, "File " + filename + " does not exist or the user running Flink");

		if (!throwableWithMessage.isPresent()) {
			// for "streaming" sources (the Blink runner always uses a streaming source
			throwableWithMessage = ExceptionUtils.findThrowableWithMessage(
					e, "The provided file path " + filename + " does not exist");
		}
		return throwableWithMessage;
	}

	@Test(timeout = 90_000L)
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
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			// Case 1: Registered sink
			// Case 1.1: Registered sink with uppercase insert into keyword.
			final String statement1 = "INSERT INTO TableSourceSink SELECT IntegerField1 = 42," +
					" StringField1, TimestampField1 FROM TableNumber1";
			executeAndVerifySinkResult(executor, sessionId, statement1, csvOutputPath);
			// Case 1.2: Registered sink with lowercase insert into keyword.
			final String statement2 = "insert Into TableSourceSink \n "
					+ "SELECT IntegerField1 = 42, StringField1, TimestampField1 "
					+ "FROM TableNumber1";
			executeAndVerifySinkResult(executor, sessionId, statement2, csvOutputPath);
			// Case 1.3: Execute the same statement again, the results should expect to be the same.
			executeAndVerifySinkResult(executor, sessionId, statement2, csvOutputPath);

			// Case 2: Temporary sink
			executor.useCatalog(sessionId, "simple-catalog");
			executor.useDatabase(sessionId, "default_database");
			// all queries are pipelined to an in-memory sink, check it is properly registered
			final ResultDescriptor otherCatalogDesc = executor.executeQuery(sessionId, "SELECT * FROM `test-table`");

			final List<String> otherCatalogResults = retrieveTableResult(
				executor,
				sessionId,
				otherCatalogDesc.getResultId());

			TestBaseUtils.compareResultCollections(
				SimpleCatalogFactory.TABLE_CONTENTS.stream().map(Row::toString).collect(Collectors.toList()),
				otherCatalogResults,
				Comparator.naturalOrder());
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testUseCatalogAndUseDatabase() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url1 = getClass().getClassLoader().getResource("test-data.csv");
		final URL url2 = getClass().getClassLoader().getResource("test-data-1.csv");
		Objects.requireNonNull(url1);
		Objects.requireNonNull(url2);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url1.getPath());
		replaceVars.put("$VAR_SOURCE_PATH2", url2.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(CATALOGS_ENVIRONMENT_FILE, clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			assertEquals(Collections.singletonList("mydatabase"), executor.listDatabases(sessionId));

			executor.useCatalog(sessionId, "hivecatalog");

			assertEquals(
				Arrays.asList(DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE, HiveCatalog.DEFAULT_DB),
				executor.listDatabases(sessionId));

			assertEquals(Collections.singletonList(DependencyTest.TestHiveCatalogFactory.TABLE_WITH_PARAMETERIZED_TYPES),
				executor.listTables(sessionId));

			executor.useDatabase(sessionId, DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE);

			assertEquals(Collections.singletonList(DependencyTest.TestHiveCatalogFactory.TEST_TABLE), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testUseNonExistingDatabase() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		exception.expect(SqlExecutionException.class);
		executor.useDatabase(sessionId, "nonexistingdb");
	}

	@Test
	public void testUseNonExistingCatalog() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		exception.expect(SqlExecutionException.class);
		executor.useCatalog(sessionId, "nonexistingcatalog");
		executor.closeSession(sessionId);
	}

	@Test
	public void testParameterizedTypes() throws Exception {
		// only blink planner supports parameterized types
		Assume.assumeTrue(planner.equals("blink"));
		final URL url1 = getClass().getClassLoader().getResource("test-data.csv");
		final URL url2 = getClass().getClassLoader().getResource("test-data-1.csv");
		Objects.requireNonNull(url1);
		Objects.requireNonNull(url2);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", url1.getPath());
		replaceVars.put("$VAR_SOURCE_PATH2", url2.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESULT_MODE", "table");

		final Executor executor = createModifiedExecutor(CATALOGS_ENVIRONMENT_FILE, clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		executor.useCatalog(sessionId, "hivecatalog");
		String resultID = executor.executeQuery(sessionId,
			"select * from " + DependencyTest.TestHiveCatalogFactory.TABLE_WITH_PARAMETERIZED_TYPES).getResultId();
		retrieveTableResult(executor, sessionId, resultID);

		// make sure legacy types still work
		executor.useCatalog(sessionId, "default_catalog");
		resultID = executor.executeQuery(sessionId, "select * from TableNumber3").getResultId();
		retrieveTableResult(executor, sessionId, resultID);
		executor.closeSession(sessionId);
	}

	@Test
	public void testCreateTable() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		final String ddlTemplate = "create table %s(\n" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c varchar\n" +
				") with (\n" +
				"  'connector.type'='filesystem',\n" +
				"  'format.type'='csv',\n" +
				"  'connector.path'='xxx'\n" +
				")\n";
		try {
			// Test create table with simple name.
			executor.useCatalog(sessionId, "catalog1");
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));

			// Test create table with full qualified name.
			executor.useCatalog(sessionId, "catalog1");
			executor.createTable(sessionId, String.format(ddlTemplate, "`simple-catalog`.`default_database`.MyTable3"));
			executor.createTable(sessionId, String.format(ddlTemplate, "`simple-catalog`.`default_database`.MyTable4"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));
			executor.useCatalog(sessionId, "simple-catalog");
			assertEquals(Arrays.asList("MyTable3", "MyTable4", "test-table"), executor.listTables(sessionId));

			// Test create table with db and table name.
			executor.useCatalog(sessionId, "catalog1");
			executor.createTable(sessionId, String.format(ddlTemplate, "`default`.MyTable5"));
			executor.createTable(sessionId, String.format(ddlTemplate, "`default`.MyTable6"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2", "MyTable5", "MyTable6"), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test @Ignore // TODO: reopen when FLINK-15075 was fixed.
	public void testCreateTableWithComputedColumn() throws Exception {
		Assume.assumeTrue(planner.equals("blink"));
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", "file:///fakePath1");
		replaceVars.put("$VAR_SOURCE_PATH2", "file:///fakePath2");
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final String ddlTemplate = "create table %s(\n" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c as a + 1\n" +
				") with (\n" +
				"  'connector.type'='filesystem',\n" +
				"  'format.type'='csv',\n" +
				"  'connector.path'='xxx'\n" +
				")\n";
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			executor.useCatalog(sessionId, "catalog1");
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test @Ignore // TODO: reopen when FLINK-15075 was fixed.
	public void testCreateTableWithWatermark() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_SOURCE_PATH1", "file:///fakePath1");
		replaceVars.put("$VAR_SOURCE_PATH2", "file:///fakePath2");
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final String ddlTemplate = "create table %s(\n" +
				"  a int,\n" +
				"  b timestamp(3),\n" +
				"  watermark for b as b - INTERVAL '5' second\n" +
				") with (\n" +
				"  'connector.type'='filesystem',\n" +
				"  'format.type'='csv',\n" +
				"  'connector.path'='xxx'\n" +
				")\n";
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			executor.useCatalog(sessionId, "catalog1");
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testCreateTableWithPropertiesChanged() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			executor.useCatalog(sessionId, "catalog1");
			executor.setSessionProperty(sessionId, "execution.type", "batch");
			final String ddlTemplate = "create table %s(\n" +
					"  a int,\n" +
					"  b bigint,\n" +
					"  c varchar\n" +
					") with (\n" +
					"  'connector.type'='filesystem',\n" +
					"  'format.type'='csv',\n" +
					"  'connector.path'='xxx',\n" +
					"  'update-mode'='append'\n" +
					")\n";
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			// Change the session property to trigger `new ExecutionContext`.
			executor.setSessionProperty(sessionId, "execution.restart-strategy.failure-rate-interval", "12345");
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));

			// Reset the session properties.
			executor.resetSessionProperties(sessionId);
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable3"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2", "MyTable3"), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testDropTable() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			executor.useCatalog(sessionId, "catalog1");
			executor.setSessionProperty(sessionId, "execution.type", "batch");
			final String ddlTemplate = "create table %s(\n" +
					"  a int,\n" +
					"  b bigint,\n" +
					"  c varchar\n" +
					") with (\n" +
					"  'connector.type'='filesystem',\n" +
					"  'format.type'='csv',\n" +
					"  'connector.path'='xxx',\n" +
					"  'update-mode'='append'\n" +
					")\n";
			// Test drop table.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE MyTable1");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));

			// Test drop table if exists.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE IF EXISTS MyTable1");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));

			// Test drop table with full qualified name.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE catalog1.`default`.MyTable1");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));

			// Test drop table with db and table name.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE `default`.MyTable1");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));

			// Test drop table that does not exist.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE IF EXISTS catalog2.`default`.MyTable1");
			assertEquals(Collections.singletonList("MyTable1"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE `default`.MyTable1");

			// Test drop table with properties changed.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			// Change the session property to trigger `new ExecutionContext`.
			executor.setSessionProperty(sessionId, "execution.restart-strategy.failure-rate-interval", "12345");
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE MyTable1");
			executor.dropTable(sessionId, "DROP TABLE MyTable2");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));

			// Test drop table with properties reset.
			// Reset the session properties.
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable1"));
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable2"));
			executor.resetSessionProperties(sessionId);
			executor.createTable(sessionId, String.format(ddlTemplate, "MyTable3"));
			assertEquals(Arrays.asList("MyTable1", "MyTable2", "MyTable3"), executor.listTables(sessionId));
			executor.dropTable(sessionId, "DROP TABLE MyTable1");
			executor.dropTable(sessionId, "DROP TABLE MyTable2");
			executor.dropTable(sessionId, "DROP TABLE MyTable3");
			assertEquals(Collections.emptyList(), executor.listTables(sessionId));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	private void executeStreamQueryTable(
			Map<String, String> replaceVars,
			String query,
			List<String> expectedResults) throws Exception {

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		assertEquals("test-session", sessionId);

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(sessionId, query);

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = retrieveTableResult(executor, sessionId, desc.getResultId());

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.closeSession(sessionId);
		}
	}

	private void verifySinkResult(String path) throws IOException {
		final List<String> actualResults = new ArrayList<>();
		TestBaseUtils.readAllResultLines(actualResults, path);
		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("true,Hello World,2020-01-01 00:00:01.0");
		expectedResults.add("false,Hello World,2020-01-01 00:00:02.0");
		expectedResults.add("false,Hello World,2020-01-01 00:00:03.0");
		expectedResults.add("false,Hello World,2020-01-01 00:00:04.0");
		expectedResults.add("true,Hello World,2020-01-01 00:00:05.0");
		expectedResults.add("false,Hello World!!!!,2020-01-01 00:00:06.0");
		TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
	}

	private void executeAndVerifySinkResult(
			Executor executor,
			String sessionId,
			String statement,
			String resultPath) throws Exception {
		final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				sessionId,
				statement);

		// wait for job completion and verify result
		boolean isRunning = true;
		while (isRunning) {
			Thread.sleep(50); // slow the processing down
			final JobStatus jobStatus = clusterClient.getJobStatus(targetDescriptor.getJobId()).get();
			switch (jobStatus) {
			case CREATED:
			case RUNNING:
				continue;
			case FINISHED:
				isRunning = false;
				verifySinkResult(resultPath);
				break;
			default:
				fail("Unexpected job status.");
			}
		}
	}

	private <T> LocalExecutor createDefaultExecutor(ClusterClient<T> clusterClient) throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", planner);
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
		return new LocalExecutor(
				EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
				Collections.emptyList(),
				clusterClient.getFlinkConfiguration(),
				new DefaultCLI(clusterClient.getFlinkConfiguration()),
				new DefaultClusterClientServiceLoader());
	}

	private <T> LocalExecutor createModifiedExecutor(ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
		replaceVars.putIfAbsent("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
		return new LocalExecutor(
				EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
				Collections.emptyList(),
				clusterClient.getFlinkConfiguration(),
				new DefaultCLI(clusterClient.getFlinkConfiguration()),
				new DefaultClusterClientServiceLoader());
	}

	private <T> LocalExecutor createModifiedExecutor(
			String yamlFile, ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
		replaceVars.putIfAbsent("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
		return new LocalExecutor(
				EnvironmentFileUtil.parseModified(yamlFile, replaceVars),
				Collections.emptyList(),
				clusterClient.getFlinkConfiguration(),
				new DefaultCLI(clusterClient.getFlinkConfiguration()),
				new DefaultClusterClientServiceLoader());
	}

	private List<String> retrieveTableResult(
			Executor executor,
			String sessionId,
			String resultID) throws InterruptedException {

		final List<String> actualResults = new ArrayList<>();
		while (true) {
			Thread.sleep(50); // slow the processing down
			final TypedResult<Integer> result = executor.snapshotResult(sessionId, resultID, 2);
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
			String sessionId,
			String resultID) throws InterruptedException {

		final List<String> actualResults = new ArrayList<>();
		while (true) {
			Thread.sleep(50); // slow the processing down
			final TypedResult<List<Tuple2<Boolean, Row>>> result =
				executor.retrieveResultChanges(sessionId, resultID);
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
