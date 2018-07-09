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
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.apache.commons.cli.Options;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

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

	private <T> ExecutionContext<T> createExecutionContext() throws Exception {
		final Environment env = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
			Collections.singletonMap("$VAR_2", "streaming"));
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
