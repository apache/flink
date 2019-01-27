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

import org.apache.flink.table.client.catalog.CatalogType;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.CatalogEntry;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
	private static final String CATALOG_ENVIRONMENT_FILE = "test-sql-client-catalogs.yaml";

	@Test
	public void testParsingCatalog() throws IOException {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars.put("$VAR_RESULT_MODE", "table");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		final Environment env = EnvironmentFileUtil.parseModified(
			CATALOG_ENVIRONMENT_FILE,
			replaceVars);

		assertEquals(new HashSet<>(Arrays.asList("myhive", "myinmemory")), env.getCatalogs().keySet());

		CatalogEntry hive = env.getCatalogs().get("myhive");
		assertEquals(
			new HashMap<String, String>() {{
				put(CatalogEntry.CATALOG_CONNECTOR_HIVE_METASTORE_URIS, "thrift://host1:10000,thrift://host2:10000");
				put(CatalogEntry.CATALOG_CONNECTOR_HIVE_METASTORE_USERNAME, "flink");
				put(CatalogEntry.CATALOG_TYPE, CatalogType.hive.name());
				put(CatalogEntry.CATALOG_IS_DEFAULT, "true");
				put(CatalogEntry.CATALOG_DEFAULT_DB, "mydb");
			}},
			hive.getProperties().asMap());

		assertTrue(hive.isDefaultCatalog());
		assertEquals("mydb", hive.getDefaultDatabase().get());

		assertEquals(
			new HashMap<String, String>() {{
				put(CatalogEntry.CATALOG_TYPE, CatalogType.flink_in_memory.name());
			}},
			env.getCatalogs().get("myinmemory").getProperties().asMap());
	}

	@Test
	public void testMerging() throws Exception {
		final Map<String, String> replaceVars1 = new HashMap<>();
		replaceVars1.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars1.put("$VAR_RESULT_MODE", "table");
		replaceVars1.put("$VAR_UPDATE_MODE", "");
		replaceVars1.put("$VAR_MAX_ROWS", "100");
		final Environment env1 = EnvironmentFileUtil.parseModified(
			DEFAULTS_ENVIRONMENT_FILE,
			replaceVars1);

		final Map<String, String> replaceVars2 = new HashMap<>(replaceVars1);
		replaceVars2.put("TableNumber1", "NewTable");
		final Environment env2 = EnvironmentFileUtil.parseModified(
			FACTORY_ENVIRONMENT_FILE,
			replaceVars2);

		final Environment merged = Environment.merge(env1, env2);

		final Set<String> tables = new HashSet<>();
		tables.add("TableNumber1");
		tables.add("TableNumber2");
		tables.add("NewTable");
		tables.add("TableSourceSink");
		tables.add("TestView1");
		tables.add("TestView2");

		assertEquals(tables, merged.getTables().keySet());
		assertTrue(merged.getExecution().isStreamingExecution());
		assertEquals(16, merged.getExecution().getMaxParallelism());
	}
}
