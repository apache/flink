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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
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

/**
 * Test for {@link ExecutionContext}.
 */
public class ExecutionContextTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";

	@Test
	public void testExecutionConfig() throws Exception {
		final ExecutionContext<?> context = createExecutionContext();
		final ExecutionConfig config = context.createEnvironmentInstance().getExecutionConfig();
		assertEquals(99, config.getAutoWatermarkInterval());
	}

	@Test
	public void testFunctions() throws Exception {
		final ExecutionContext<?> context = createExecutionContext();
		final TableEnvironment tableEnv = context.createEnvironmentInstance().getTableEnvironment();
		final String[] expected = new String[]{"scalarUDF", "tableUDF", "aggregateUDF"};
		final String[] actual = tableEnv.listUserDefinedFunctions();
		Arrays.sort(expected);
		Arrays.sort(actual);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testSourceSinks() throws Exception {
		final ExecutionContext<?> context = createExecutionContext();
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
			sources.get("TableNumber1").getTableSchema().getColumnNames());

		assertArrayEquals(
			new TypeInformation[]{Types.INT(), Types.STRING()},
			sources.get("TableNumber1").getTableSchema().getTypes());

		assertArrayEquals(
			new String[]{"IntegerField2", "StringField2"},
			sources.get("TableNumber2").getTableSchema().getColumnNames());

		assertArrayEquals(
			new TypeInformation[]{Types.INT(), Types.STRING()},
			sources.get("TableNumber2").getTableSchema().getTypes());

		assertArrayEquals(
			new String[]{"BooleanField", "StringField"},
			sinks.get("TableSourceSink").getFieldNames());

		assertArrayEquals(
			new TypeInformation[]{Types.BOOLEAN(), Types.STRING()},
			sinks.get("TableSourceSink").getFieldTypes());

		final TableEnvironment tableEnv = context.createEnvironmentInstance().getTableEnvironment();

		assertArrayEquals(
			new String[]{"TableNumber1", "TableNumber2", "TableSourceSink"},
			tableEnv.listTables());
	}

	private <T> ExecutionContext<T> createExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_2", "streaming");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		final Environment env = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
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
}
