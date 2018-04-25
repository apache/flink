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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.util.DummyCustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Contains basic tests for the {@link LocalExecutor}.
 */
public class LocalExecutorITCase extends TestLogger {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 2;

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfig(),
			NUM_TMS,
			NUM_SLOTS_PER_TM),
		true);

	private static ClusterClient<?> clusterClient;

	@BeforeClass
	public static void setup() {
		clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient();
	}

	private static Configuration getConfig() {
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4L);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}

	@Test
	public void testListTables() throws Exception {
		final Executor executor = createDefaultExecutor(clusterClient);
		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualTables = executor.listTables(session);

		final List<String> expectedTables = Arrays.asList("TableNumber1", "TableNumber2");
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
		expectedProperties.put("execution.type", "streaming");
		expectedProperties.put("execution.time-characteristic", "event-time");
		expectedProperties.put("execution.parallelism", "1");
		expectedProperties.put("execution.max-parallelism", "16");
		expectedProperties.put("execution.max-idle-state-retention", "0");
		expectedProperties.put("execution.min-idle-state-retention", "0");
		expectedProperties.put("execution.result-mode", "table");
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

	@Test(timeout = 30_000L)
	public void testQueryExecutionChangelog() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_0", url.getPath());
		replaceVars.put("$VAR_1", "/");
		replaceVars.put("$VAR_2", "changelog");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT * FROM TableNumber1");

			assertFalse(desc.isMaterialized());

			final List<String> actualResults = new ArrayList<>();

			while (true) {
				Thread.sleep(50); // slow the processing down
				final TypedResult<List<Tuple2<Boolean, Row>>> result =
					executor.retrieveResultChanges(session, desc.getResultId());
				if (result.getType() == TypedResult.ResultType.PAYLOAD) {
					for (Tuple2<Boolean, Row> change : result.getPayload()) {
						actualResults.add(change.toString());
					}
				} else if (result.getType() == TypedResult.ResultType.EOS) {
					break;
				}
			}

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("(true,42,Hello World)");
			expectedResults.add("(true,22,Hello World)");
			expectedResults.add("(true,32,Hello World)");
			expectedResults.add("(true,32,Hello World)");
			expectedResults.add("(true,42,Hello World)");
			expectedResults.add("(true,52,Hello World!!!!)");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	@Test(timeout = 30_000L)
	public void testQueryExecutionTable() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-data.csv");
		Objects.requireNonNull(url);
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_0", url.getPath());
		replaceVars.put("$VAR_1", "/");
		replaceVars.put("$VAR_2", "table");

		final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(session, "SELECT IntegerField1 FROM TableNumber1");

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = new ArrayList<>();

			while (true) {
				Thread.sleep(50); // slow the processing down
				final TypedResult<Integer> result = executor.snapshotResult(session, desc.getResultId(), 2);
				if (result.getType() == TypedResult.ResultType.PAYLOAD) {
					actualResults.clear();
					IntStream.rangeClosed(1, result.getPayload()).forEach((page) -> {
						for (Row row : executor.retrieveResultPage(desc.getResultId(), page)) {
							actualResults.add(row.toString());
						}
					});
				} else if (result.getType() == TypedResult.ResultType.EOS) {
					break;
				}
			}

			final List<String> expectedResults = new ArrayList<>();
			expectedResults.add("42");
			expectedResults.add("22");
			expectedResults.add("32");
			expectedResults.add("32");
			expectedResults.add("42");
			expectedResults.add("52");

			TestBaseUtils.compareResultCollections(expectedResults, actualResults, Comparator.naturalOrder());
		} finally {
			executor.stop(session);
		}
	}

	private <T> LocalExecutor createDefaultExecutor(ClusterClient<T> clusterClient) throws Exception {
		return new LocalExecutor(
			EnvironmentFileUtil.parseUnmodified(DEFAULTS_ENVIRONMENT_FILE),
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
}
