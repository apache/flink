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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.util.DummyCustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.streaming.connectors.kafka.KafkaITService;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Contains basic tests for the {@link LocalExecutor}.
 */
public class LocalExecutorITCase extends TestLogger {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 2;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfig(),
			NUM_TMS,
			NUM_SLOTS_PER_TM),
		true);

	@ClassRule
	public static final KafkaITService KAFKA_IT_SERVICE = new KafkaITService();

	private static ClusterClient<?> clusterClient;

	@BeforeClass
	public static void setup() {
		clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient();
	}

	private static Configuration getConfig() {
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4 * 1024 * 1024);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}

	@Test
	public void testValidateSession() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.validateSession(session);

		session.addView(ViewEntry.create("AdditionalView1", "SELECT 1"));
		session.addView(ViewEntry.create("AdditionalView2", "SELECT * FROM AdditionalView1"));
		executor.validateSession(session);

		List<String> actualTables =
			executor.listTables(session).stream().sorted().collect(Collectors.toList());
		List<String> expectedTables = Arrays.asList(
			"AdditionalView1",
			"AdditionalView2",
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2").stream().sorted().collect(Collectors.toList());
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

		actualTables =
			executor.listTables(session).stream().sorted().collect(Collectors.toList());
		expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2").stream().sorted().collect(Collectors.toList());
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testListTables() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualTables =
			executor.listTables(session).stream().sorted().collect(Collectors.toList());

		final List<String> expectedTables = Arrays.asList(
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2").stream().sorted().collect(Collectors.toList());
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testListUserDefinedFunctions() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final Set<String> actualUDXs = new HashSet<>(executor.listUserDefinedFunctions(session));
		final List<String> expectedUDXs = Arrays.asList("aggregateUDF", "tableUDF", "scalarUDF");
		expectedUDXs.stream().forEach(udf -> actualUDXs.contains(udf));
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
			new InternalType[]{DataTypes.INT, DataTypes.STRING});

		assertEquals(expectedTableSchema, actualTableSchema);
	}

	// TODO: Code Complete is not picked.
	@Test
	@Ignore
	public void testCompleteStatement() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> expectedTableHints = Arrays.asList(
			"TABLE",
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink");
		assertEquals(expectedTableHints, executor.completeStatement(session, "SELECT * FROM Ta", 16));

		final List<String> expectedClause = Collections.singletonList("WHERE");
		assertEquals(expectedClause, executor.completeStatement(session, "SELECT * FROM TableNumber2 WH", 29));

		final List<String> expectedField = Arrays.asList("INTERVAL", "IntegerField1");
		assertEquals(expectedField, executor.completeStatement(session, "SELECT * FROM TableNumber1 WHERE Inte", 37));
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionChangelog() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
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
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// start job
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
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testCreateTable() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.createTable(session, "CREATE TABLE TableFromDDL(field1 INT, field2 VARCHAR) WITH (type = 'type', attributeKey1 = 'value1', attributeKey2 = 'value2')");

		final List<String> actualTables =
			executor.listTables(session).stream().sorted().collect(Collectors.toList());
		final List<String> expectedTables = Arrays.asList(
			"TableFromDDL",
			"TableNumber1",
			"TableNumber2",
			"TableSourceSink",
			"TestView1",
			"TestView2").stream().sorted().collect(Collectors.toList());
		assertEquals(expectedTables, actualTables);

		final TableSchema schema = executor.getTableSchema(session, "TableFromDDL");
		final String expectedSchema =
			"root\n" +
			" |-- name: field1\n" +
			" |-- type: IntType\n" +
			" |-- isNullable: true\n" +
			" |-- name: field2\n" +
			" |-- type: StringType\n" +
			" |-- isNullable: true\n";
		assertEquals(expectedSchema, schema.toString());
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionFromDDLTable() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		// Create a table with DDL, which has the same schema of TableNumber1
		executor.createTable(
			session,
			"CREATE TABLE TableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
					" WITH (" +
					"  type = 'CSV'" +
					", path = '" + url.getPath() + "'" +
					", commentsPrefix = '#')");

		final String query = "SELECT scalarUDF(IntegerField1), StringField1 FROM TableFromDDL";
		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("47,Hello World");
		expectedResults.add("27,Hello World");
		expectedResults.add("37,Hello World");
		expectedResults.add("37,Hello World");
		expectedResults.add("47,Hello World");
		expectedResults.add("57,Hello World!!!!");

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

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionSinkToDDLTable() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.createTable(
			session,
			"CREATE TABLE SourceTableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		executor.createTable(
			session,
			"CREATE TABLE SinkTableFromDDL(BooleanField BOOLEAN, StringField VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + csvOutputPath + "')");

		try {
			final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				session,
				"INSERT INTO SinkTableFromDDL SELECT IntegerField1 = 42, StringField1 FROM SourceTableFromDDL");

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
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionUpsertSink() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.createTable(
			session,
			"CREATE TABLE SourceTableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		executor.createTable(
			session,
			"CREATE TABLE SinkTableFromDDL(IntegerField INT, StringField VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", updateMode = 'UPSERT'" +
				", path = '" + csvOutputPath + "')");

		try {
			final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				session,
				"INSERT INTO SinkTableFromDDL SELECT SUM(IntegerField1), StringField1 FROM SourceTableFromDDL GROUP BY StringField1");

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
						verifyUpsertSinkResult(csvOutputPath);
						break;
					default:
						fail("Unexpected job status.");
				}
			}
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testStreamQueryExecutionRetractSink() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.createTable(
			session,
			"CREATE TABLE SourceTableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		executor.createTable(
			session,
			"CREATE TABLE SinkTableFromDDL(IntegerField INT, StringField VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", updateMode = 'RETRACT'" +
				", path = '" + csvOutputPath + "')");

		try {
			final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				session,
				"INSERT INTO SinkTableFromDDL SELECT SUM(IntegerField1), StringField1 FROM SourceTableFromDDL GROUP BY StringField1");

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
						verifyRetractSinkResult(csvOutputPath);
						break;
					default:
						fail("Unexpected job status.");
				}
			}
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testTableSourceSinkTableAsSource() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);

		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_SINK_PATH", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "1");

		final String query = "SELECT COUNT(*), StringField FROM TableSourceSink GROUP BY StringField";

		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("1,Hello World!!!!");

		executeStreamQueryTable(replaceVars, query, expectedResults);
	}

	@Test(timeout = 30_000L)
	public void testStreamWindowQueryExecutionFromDDL() throws Exception {
		final String csvOutputPath = new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv").toURI().toString();
		final URL url = getClass().getClassLoader().getResource("test-data2.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		executor.createTable(
			session,
			"CREATE TABLE SourceTableFromDDL(IntegerField1 INT, StringField1 VARCHAR, " +
			"TsField TIMESTAMP, WATERMARK FOR TsField AS withOffset(TsField, 1000))" +
			" WITH (" +
			"  type = 'CSV'" +
			", path = '" + url.getPath() + "'" +
			", commentsPrefix = '#')");

		executor.createTable(
			session,
			"CREATE TABLE SinkTableFromDDL(StringField VARCHAR, WindowStart BIGINT, IntegerField INT)" +
			" WITH (" +
			"  type = 'CSV'" +
			", updateMode = 'APPEND'" +
			", path = '" + csvOutputPath + "')");

		try {
			final ProgramTargetDescriptor targetDescriptor = executor.executeUpdate(
				session,
				"INSERT INTO SinkTableFromDDL " +
				"SELECT StringField1, CAST(TUMBLE_START(TsField, INTERVAL '1' MINUTE) as BIGINT), SUM(IntegerField1) " +
				"FROM SourceTableFromDDL " +
				"GROUP BY StringField1, TUMBLE(TsField, INTERVAL '1' MINUTE)");

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
						final List<String> actualResults = new ArrayList<>();
						TestBaseUtils.readAllResultLines(actualResults, csvOutputPath);
						final List<String> expectedResults = new ArrayList<>();
						expectedResults.add("Hello World,0,170");
						expectedResults.add("Hello World!!!!,0,52");
						TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
						break;
					default:
						fail("Unexpected job status.");
				}
			}
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testBatchQueryExecutionFromDDLTable() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		// Create a table with DDL, which has the same schema of TableNumber1
		executor.createTable(
			session,
			"CREATE TABLE TableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		try {
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT scalarUDF(IntegerField1) FROM TableFromDDL");

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
	public void testBatchQueryExecutionRobustness() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		// Create an unknown table, which would cause Exception in following queries.
		executor.createTable(
			session,
			"CREATE TABLE UnknownTable(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'UNKNOWN'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		executor.createTable(
			session,
			"CREATE TABLE TableFromDDL(IntegerField1 INT, StringField1 VARCHAR)" +
				" WITH (" +
				"  type = 'CSV'" +
				", path = '" + url.getPath() + "'" +
				", commentsPrefix = '#')");

		try {
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT * FROM UnknownTable");
		} catch (Exception e) {
			assertTrue(e instanceof SqlExecutionException);
			e.printStackTrace();
		}

		try {
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT scalarUDF(IntegerField1) FROM TableFromDDL");

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
	public void testBatchQueryExecutionFromDDLViewWithUDF() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_UPDATE_MODE", "");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		// Create a scalar function
		executor.createFunction(
			session,
			"CREATE FUNCTION scalarDDL "
				+ "AS 'org.apache.flink.table.client.gateway.utils.UserDefinedFunctions$ScalarUDF'");
		// Create a view
		executor.createView(
			session,
			"CREATE VIEW TestViewDDL AS SELECT scalarDDL(IntegerField1) FROM TableNumber1");

		try {
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT * FROM TestViewDDL");

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

	@Test(timeout = 60_000L)
	public void testReadFromKafka() throws Exception {
		final String sourceTopic = "testReadFromKafka";
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		String resultId = null;
		try {
			executor.createTable(session, String.format(
					"CREATE TABLE t(key VARBINARY, msg VARBINARY, `topic` VARCHAR, `partition` INT, `offset`" +
					" BIGINT) with (type = 'KAFKA010', topic = '%s', `bootstrap.servers` =" +
					" '%s', `group.id` = 'test-group', startupMode = 'EARLIEST')",
					sourceTopic, KafkaITService.brokerConnectionStrings()));
			produceMessages(sourceTopic, KafkaITService.brokerConnectionStrings());

			ResultDescriptor descriptor = executor.executeQuery(session,
					"SELECT CAST(key AS VARCHAR), CAST (msg AS VARCHAR) FROM t");

			TypedResult<List<Tuple2<Boolean, Row>>> result;
			resultId = descriptor.getResultId();
			do {
				Thread.sleep(1);
				result = executor.retrieveResultChanges(session, resultId);
			} while (result.getType() == TypedResult.ResultType.EMPTY);

			assertEquals("key_0", result.getPayload().get(0).f1.getField(0));
			assertEquals("value_0", result.getPayload().get(0).f1.getField(1));
		} finally {
			if (resultId != null && !resultId.isEmpty()) {
				executor.cancelQuery(session, resultId);
			}
		}
	}

	@Test(timeout = 60_000L)
	public void testProduceToKafka() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		final String sinkTopic = "testProduceToKafka";
		try {
			executor.createTable(session, String.format(
					"CREATE TABLE kafka_sink(messageKey VARBINARY, messageValue VARBINARY, PRIMARY KEY " +
					"(messageKey)) with (type = 'KAFKA010', topic = '%s', `bootstrap.servers` = '%s', retries = '3')",
					sinkTopic, KafkaITService.brokerConnectionStrings()));
			executor.executeUpdate(session,
					"INSERT INTO kafka_sink (messageKey, messageValue) VALUES (CAST('key_0' AS VARBINARY), " +
					"CAST('value_0' AS VARBINARY))");

			// Consume from the sink topic.
			Properties props = new Properties();
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaITService.brokerConnectionStrings());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testGroupId");
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
				consumer.subscribe(Collections.singleton(sinkTopic));
				ConsumerRecords<String, String> consumerRecords = ConsumerRecords.empty();
				while (consumerRecords.isEmpty()) {
					consumerRecords = consumer.poll(1000L);
				}
				if (consumerRecords.isEmpty()) {
					fail("Failed to insert into table.");
				}
				ConsumerRecord<String, String> record = consumerRecords.iterator().next();
				assertEquals("key_0", record.key());
				assertEquals("value_0", record.value());
			}
		} finally {
			executor.stop(session);
		}
	}

	private void produceMessages(String topic, String brokerConnectionStrings) {
		KafkaITService.createTopic(topic, 1, 1);
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectionStrings);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		try (Producer<String, String > producer = new KafkaProducer<>(properties)) {
			TopicPartition tp = new TopicPartition(topic, 0);
			for (int i = 0; i < 10; i++) {
				producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), "key_" + i, "value_" + i),
						(recordMetadata, e) -> {
							if (e != null) {
								fail("Failed to send message to Kafka due to: " + e);
							}
						});
			}
		}
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

	private void verifyUpsertSinkResult(String path) throws IOException {
		final List<String> actualResults = new ArrayList<>();
		TestBaseUtils.readAllResultLines(actualResults, path);
		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("Add,42,Hello World");
		expectedResults.add("Add,64,Hello World");
		expectedResults.add("Add,96,Hello World");
		expectedResults.add("Add,128,Hello World");
		expectedResults.add("Add,170,Hello World");
		expectedResults.add("Add,52,Hello World!!!!");
		TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
	}

	private void verifyRetractSinkResult(String path) throws IOException {
		final List<String> actualResults = new ArrayList<>();
		TestBaseUtils.readAllResultLines(actualResults, path);
		final List<String> expectedResults = new ArrayList<>();
		expectedResults.add("True,42,Hello World");
		expectedResults.add("False,42,Hello World");
		expectedResults.add("True,64,Hello World");
		expectedResults.add("False,64,Hello World");
		expectedResults.add("True,96,Hello World");
		expectedResults.add("False,96,Hello World");
		expectedResults.add("True,128,Hello World");
		expectedResults.add("False,128,Hello World");
		expectedResults.add("True,170,Hello World");
		expectedResults.add("True,52,Hello World!!!!");
		TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
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
