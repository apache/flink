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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.DummyTableSourceFactory;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import org.apache.commons.cli.Options;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ExecutionContext}.
 */
public class ExecutionContextTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String STREAMING_ENVIRONMENT_FILE = "test-sql-client-streaming.yaml";

	@Test
	public void testExecutionConfig() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final ExecutionConfig config = context.createEnvironmentInstance().getExecutionConfig();

		assertEquals(99, config.getAutoWatermarkInterval());

		final RestartStrategies.RestartStrategyConfiguration restartConfig = config.getRestartStrategy();
		assertTrue(restartConfig instanceof RestartStrategies.FailureRateRestartStrategyConfiguration);
		final RestartStrategies.FailureRateRestartStrategyConfiguration failureRateStrategy =
			(RestartStrategies.FailureRateRestartStrategyConfiguration) restartConfig;
		assertEquals(10, failureRateStrategy.getMaxFailureRate());
		assertEquals(99_000, failureRateStrategy.getFailureInterval().toMilliseconds());
		assertEquals(1_000, failureRateStrategy.getDelayBetweenAttemptsInterval().toMilliseconds());
	}

	@Test
	public void testFunctions() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final TableEnvironment tableEnv = context.createEnvironmentInstance().getTableEnvironment();
		final String[] expected = new String[]{"scalarUDF", "tableUDF", "aggregateUDF"};
		final String[] actual = tableEnv.listUserDefinedFunctions();
		Arrays.sort(expected);
		Arrays.sort(actual);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testTables() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final Map<String, TableSource<?>> sources = context.getTableSources();
		final Map<String, TableSink<?>> sinks = context.getTableSinks();

		assertEquals(
			new HashSet<>(Arrays.asList("TableSourceSink", "TableNumber1", "TableNumber2")),
			sources.keySet());

		assertEquals(
			new HashSet<>(Collections.singletonList("TableSourceSink")),
			sinks.keySet());

		assertArrayEquals(
			new String[]{"IntegerField1", "StringField1"},
			sources.get("TableNumber1").getTableSchema().getFieldNames());

		assertArrayEquals(
			new TypeInformation[]{Types.INT(), Types.STRING()},
			sources.get("TableNumber1").getTableSchema().getFieldTypes());

		assertArrayEquals(
			new String[]{"IntegerField2", "StringField2"},
			sources.get("TableNumber2").getTableSchema().getFieldNames());

		assertArrayEquals(
			new TypeInformation[]{Types.INT(), Types.STRING()},
			sources.get("TableNumber2").getTableSchema().getFieldTypes());

		assertArrayEquals(
			new String[]{"BooleanField", "StringField"},
			sinks.get("TableSourceSink").getFieldNames());

		assertArrayEquals(
			new TypeInformation[]{Types.BOOLEAN(), Types.STRING()},
			sinks.get("TableSourceSink").getFieldTypes());

		final TableEnvironment tableEnv = context.createEnvironmentInstance().getTableEnvironment();

		assertArrayEquals(
			new String[]{"TableNumber1", "TableNumber2", "TableSourceSink", "TestView1", "TestView2"},
			tableEnv.listTables());
	}

	@Test
	public void testTemporalTables() throws Exception {
		final ExecutionContext<?> context = createStreamingExecutionContext();

		assertEquals(
			new HashSet<>(Arrays.asList("EnrichmentSource", "HistorySource")),
			context.getTableSources().keySet());

		final StreamTableEnvironment tableEnv = (StreamTableEnvironment) context.createEnvironmentInstance().getTableEnvironment();

		assertArrayEquals(
			new String[]{"EnrichmentSource", "HistorySource", "HistoryView", "TemporalTableUsage"},
			tableEnv.listTables());

		assertArrayEquals(
			new String[]{"SourceTemporalTable", "ViewTemporalTable"},
			tableEnv.listUserDefinedFunctions());

		assertArrayEquals(
			new String[]{"integerField", "stringField", "rowtimeField", "integerField0", "stringField0", "rowtimeField0"},
			tableEnv.scan("TemporalTableUsage").getSchema().getFieldNames());
	}

	private <T> ExecutionContext<T> createExecutionContext(String file, Map<String, String> replaceVars) throws Exception {
		final Environment env = EnvironmentFileUtil.parseModified(
			file,
			replaceVars);
		final SessionContext session = new SessionContext("test-session", new Environment());
		final Configuration flinkConfig = new Configuration();
		return new ExecutionContext<>(
			env,
			session,
			Collections.emptyList(),
			flinkConfig,
			new Options(),
			Collections.singletonList(new DefaultCLI(flinkConfig)));
	}

	private <T> ExecutionContext<T> createDefaultExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		return createExecutionContext(DEFAULTS_ENVIRONMENT_FILE, replaceVars);
	}

	private <T> ExecutionContext<T> createStreamingExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_CONNECTOR_TYPE", DummyTableSourceFactory.CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY", DummyTableSourceFactory.TEST_PROPERTY);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "");
		return createExecutionContext(STREAMING_ENVIRONMENT_FILE, replaceVars);
	}
}
