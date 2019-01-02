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

import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.UserDefinedFunctions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Environment}.
 */
public class EnvironmentTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static Map<String, String> defaultVars() {
		final Map<String, String> replaceVars1 = new HashMap<>();
		replaceVars1.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars1.put("$VAR_RESULT_MODE", "table");
		replaceVars1.put("$VAR_UPDATE_MODE", "");
		replaceVars1.put("$VAR_MAX_ROWS", "100");
		return replaceVars1;
	}

	@Test
	public void testMerging() throws Exception {
		final Map<String, String> replaceVars1 = defaultVars();
		final Environment env1 = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
			replaceVars1);

		final Map<String, String> replaceVars2 = new HashMap<>(replaceVars1);
		replaceVars2.put("TableNumber1", "NewTable");
		final Environment env2 = EnvironmentFileUtil.parseModified(
			FACTORY_ENVIRONMENT_FILE,
			replaceVars2);

		final Environment merged = Environment.merge(env1, env2);

		final Set<String> catalogs = new HashSet<>();
		catalogs.add("Catalog1");
		catalogs.add("Catalog2");

		final Set<String> tables = new HashSet<>();
		tables.add("TableNumber1");
		tables.add("TableNumber2");
		tables.add("NewTable");
		tables.add("TableSourceSink");
		tables.add("TestView1");
		tables.add("TestView2");

		assertEquals(catalogs, merged.getCatalogs().keySet());
		assertEquals(tables, merged.getTables().keySet());
		assertTrue(merged.getExecution().isStreamingExecution());
		assertEquals(16, merged.getExecution().getMaxParallelism());
	}

	@Test
	public void testToString() throws Exception {
		final Map<String, String> replaceVars1 = defaultVars();
		final Environment env = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
			replaceVars1);
		String toString = env.toString();
		List<String> expected = Arrays.asList(
			"Catalog1",
			"TableNumber1", "TableSourceSink", "TestView1",
			"scalarUDF", "aggregateUDF", "tableUDF");
		assertTrue(Arrays.asList(toString.split("\\s")).containsAll(expected));
	}

	@Test
	public void testDuplicateCatalog() throws Exception {
		expectedException.expect(SqlClientException.class);
		expectedException.expectMessage("Cannot create catalog 'Catalog2' because a catalog with this name is already registered.");
		Environment env = new Environment();
		env.setCatalogs(Arrays.asList(
			ImmutableMap.of("name", "Catalog1", "type",  "test"),
			ImmutableMap.of("name", "Catalog2", "type",  "test"),
			ImmutableMap.of("name", "Catalog2", "type",  "test")));
	}

	@Test
	public void testDuplicateTable() throws Exception {
		expectedException.expect(SqlClientException.class);
		expectedException.expectMessage("Cannot create table 'Table2' because a table with this name is already registered.");
		Environment env = new Environment();
		env.setTables(Arrays.asList(
			ImmutableMap.of("name", "Table1", "type",  "source-table"),
			ImmutableMap.of("name", "Table2", "type",  "source-table"),
			ImmutableMap.of("name", "Table2", "type",  "source-table")));
	}

	@Test
	public void testDuplicateFunction() throws Exception {
		expectedException.expect(SqlClientException.class);
		expectedException.expectMessage("Cannot create function 'Function2' because a function with this name is already registered.");
		Environment env = new Environment();
		env.setFunctions(Arrays.asList(scalarUdf("Function1", 5),
			scalarUdf("Function2", 5),
			scalarUdf("Function2", 5)));
	}

	private static Map<String, Object> scalarUdf(String name, Integer offset) {
		return ImmutableMap.of(
			"name", name,
			"from", "class",
			"class", UserDefinedFunctions.ScalarUDF.class.getCanonicalName(),
			"constructor", Arrays.asList(offset));
	}
}
