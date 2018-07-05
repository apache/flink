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

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Dependency tests for {@link LocalExecutor}. Mainly for testing classloading of dependencies.
 */
public class DependencyTest {

	public static final String CONNECTOR_TYPE_VALUE = "test-connector";
	public static final String TEST_PROPERTY = "test-property";
	public static final String CONNECTOR_TEST_PROPERTY = "connector.test-property";

	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";
	private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";

	@Test
	public void testTableFactoryDiscovery() throws Exception {
		// create environment
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_0", CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_1", TEST_PROPERTY);
		replaceVars.put("$VAR_2", "test-value");
		final Environment env = EnvironmentFileUtil.parseModified(FACTORY_ENVIRONMENT_FILE, replaceVars);

		// create executor with dependencies
		final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
		final LocalExecutor executor = new LocalExecutor(
			env,
			Collections.singletonList(dependency),
			new Configuration(),
			new DefaultCLI(new Configuration()));

		final SessionContext session = new SessionContext("test-session", new Environment());

		final TableSchema result = executor.getTableSchema(session, "TableNumber1");
		final TableSchema expected = TableSchema.builder()
			.field("IntegerField1", Types.INT())
			.field("StringField1", Types.STRING())
			.field("rowtimeField", Types.SQL_TIMESTAMP())
			.build();

		assertEquals(expected, result);
	}
}
