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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.minicluster.StandaloneMiniCluster;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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

	private static StandaloneMiniCluster cluster;

	@BeforeClass
	public static void before() throws Exception {
		final Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

		cluster = new StandaloneMiniCluster(config);
	}

	@AfterClass
	public static void after() throws Exception {
		cluster.close();
		cluster = null;
	}

	@Test
	public void testListTables() throws Exception {
		final Executor executor = createDefaultExecutor();
		final SessionContext context = new SessionContext("test-session", new Environment());

		final List<String> actualTables = executor.listTables(context);

		final List<String> expectedTables = Arrays.asList("TableNumber1", "TableNumber2");
		assertEquals(expectedTables, actualTables);
	}

	@Test
	public void testGetSessionProperties() throws Exception {
		final Executor executor = createDefaultExecutor();
		final SessionContext context = new SessionContext("test-session", new Environment());

		// modify defaults
		context.setSessionProperty("execution.result-mode", "table");

		final Map<String, String> actualProperties = executor.getSessionProperties(context);

		final Map<String, String> expectedProperties = new HashMap<>();
		expectedProperties.put("execution.type", "streaming");
		expectedProperties.put("execution.parallelism", "1");
		expectedProperties.put("execution.max-parallelism", "16");
		expectedProperties.put("execution.max-idle-state-retention", "0");
		expectedProperties.put("execution.min-idle-state-retention", "0");
		expectedProperties.put("execution.result-mode", "table");
		expectedProperties.put("deployment.type", "standalone");
		expectedProperties.put("deployment.response-timeout", "5000");

		assertEquals(expectedProperties, actualProperties);
	}

	@Test
	public void testTableSchema() throws Exception {
		final Executor executor = createDefaultExecutor();
		final SessionContext context = new SessionContext("test-session", new Environment());

		final TableSchema actualTableSchema = executor.getTableSchema(context, "TableNumber2");

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

		final Executor executor = createModifiedExecutor(replaceVars);
		final SessionContext context = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(context, "SELECT * FROM TableNumber1");

			assertFalse(desc.isMaterialized());

			final List<String> actualResults = new ArrayList<>();

			while (true) {
				Thread.sleep(50); // slow the processing down
				final TypedResult<List<Tuple2<Boolean, Row>>> result =
					executor.retrieveResultChanges(context, desc.getResultId());
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
			executor.stop(context);
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

		final Executor executor = createModifiedExecutor(replaceVars);
		final SessionContext context = new SessionContext("test-session", new Environment());

		try {
			// start job and retrieval
			final ResultDescriptor desc = executor.executeQuery(context, "SELECT IntegerField1 FROM TableNumber1");

			assertTrue(desc.isMaterialized());

			final List<String> actualResults = new ArrayList<>();

			while (true) {
				Thread.sleep(50); // slow the processing down
				final TypedResult<Integer> result = executor.snapshotResult(context, desc.getResultId(), 2);
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
			executor.stop(context);
		}
	}

	private LocalExecutor createDefaultExecutor() throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-sql-client-defaults.yaml");
		Objects.requireNonNull(url);
		final Environment env = Environment.parse(url);

		return new LocalExecutor(env, Collections.emptyList(), cluster.getConfiguration());
	}

	private LocalExecutor createModifiedExecutor(Map<String, String> replaceVars) throws Exception {
		final URL url = getClass().getClassLoader().getResource("test-sql-client-defaults.yaml");
		Objects.requireNonNull(url);
		String schema = FileUtils.readFileUtf8(new File(url.getFile()));

		for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
			schema = schema.replace(replaceVar.getKey(), replaceVar.getValue());
		}

		final Environment env = Environment.parse(schema);

		return new LocalExecutor(env, Collections.emptyList(), cluster.getConfiguration());
	}
}
