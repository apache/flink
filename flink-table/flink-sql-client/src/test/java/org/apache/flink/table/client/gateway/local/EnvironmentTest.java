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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.client.config.entries.CatalogEntry.CATALOG_NAME;
import static org.apache.flink.table.client.config.entries.ModuleEntry.MODULE_NAME;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link Environment}.
 */
public class EnvironmentTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testMerging() throws Exception {
		final Map<String, String> replaceVars1 = new HashMap<>();
		replaceVars1.put("$VAR_PLANNER", "old");
		replaceVars1.put("$VAR_EXECUTION_TYPE", "batch");
		replaceVars1.put("$VAR_RESULT_MODE", "table");
		replaceVars1.put("$VAR_UPDATE_MODE", "");
		replaceVars1.put("$VAR_MAX_ROWS", "100");
		replaceVars1.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
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
		assertTrue(merged.getExecution().inStreamingMode());
		// default value is 128, will be overwritten by pipeline.max-parallelism
		assertEquals(128, merged.getExecution().getMaxParallelism());

		final Map<String, String> configuration = new HashMap<>();
		configuration.put("table.optimizer.join-reorder-enabled", "true");
		configuration.put("parallelism.default", "1");
		configuration.put("pipeline.max-parallelism", "16");
		configuration.put("pipeline.time-characteristic", "EventTime");
		configuration.put("restart-strategy.failure-rate.delay", "1000");
		configuration.put("restart-strategy.failure-rate.max-failures-per-interval", "10");
		configuration.put("pipeline.auto-watermark-interval", "99");
		configuration.put("restart-strategy", "failure-rate");
		configuration.put("restart-strategy.failure-rate.failure-rate-interval", "99000");

		assertEquals(configuration, merged.getConfiguration().asMap());
	}

	@Test
	public void testConflictInYamlFile() throws IOException {
		testConflictInYamlFile("parallelism", "parallelism.default", "1");
		testConflictInYamlFile("max-parallelism", "pipeline.max-parallelism", "2");
		testConflictInYamlFile("time-characteristic", "pipeline.time-characteristic", "event-time");
		testConflictInYamlFile("periodic-watermarks-interval", "pipeline.auto-watermark-interval", "200");
		testConflictInYamlFile("restart-strategy.type", "restart-strategy", "fixed-delay");
		testConflictInYamlFile("restart-strategy.attempts", "restart-strategy.fixed-delay.attempts", "2");
		testConflictInYamlFile("restart-strategy.delay", "restart-strategy.fixed-delay.delay", "5");
		testConflictInYamlFile("restart-strategy.delay", "restart-strategy.failure-rate.delay", "5");
		testConflictInYamlFile("restart-strategy.failure-rate-interval",
				"restart-strategy.failure-rate.failure-rate-interval", "600");
		testConflictInYamlFile("restart-strategy.max-failures-per-interval",
				"restart-strategy.failure-rate.max-failures-per-interval", "1000");
	}

	private void testConflictInYamlFile(
			String deprecatedKeyWithoutPrefix,
			String newKey,
			String value) throws IOException {
		try {
			Environment.parse(String.format(
					"execution:\n  %s: %s\n" +
							"configuration:\n  %s: %s",
					deprecatedKeyWithoutPrefix, value, newKey, value));
			fail();
		} catch (SqlClientException e) {
			assertTrue(e.getMessage().contains(
					String.format("'%s' and '%s' can not be set both", newKey, "execution." + deprecatedKeyWithoutPrefix)));
		}
	}

	@Test
	public void testMergingWithConflict() throws IOException {
		testMergingWithConflict("parallelism", "parallelism.default", "1");
		testMergingWithConflict("max-parallelism", "pipeline.max-parallelism", "2");
		testMergingWithConflict("time-characteristic", "pipeline.time-characteristic", "event-time");
		testMergingWithConflict("periodic-watermarks-interval", "pipeline.auto-watermark-interval", "200");
		testMergingWithConflict("restart-strategy.type", "restart-strategy", "fixed-delay");
		testMergingWithConflict("restart-strategy.attempts", "restart-strategy.fixed-delay.attempts", "2");
		testMergingWithConflict("restart-strategy.delay", "restart-strategy.fixed-delay.delay", "5");
		testMergingWithConflict("restart-strategy.delay", "restart-strategy.failure-rate.delay", "5");
		testMergingWithConflict("restart-strategy.failure-rate-interval",
				"restart-strategy.failure-rate.failure-rate-interval", "600");
		testMergingWithConflict("restart-strategy.max-failures-per-interval",
				"restart-strategy.failure-rate.max-failures-per-interval", "1000");
	}

	private void testMergingWithConflict(
			String deprecatedKeyWithoutPrefix,
			String newKey,
			String value) throws IOException {
		final Environment env1 = Environment.parse(String.format("execution:\n  %s: %s", deprecatedKeyWithoutPrefix, value));
		final Environment env2 = Environment.parse(String.format("configuration:\n  %s: %s", newKey, value));
		try {
			Environment.merge(env1, env2);
			fail();
		} catch (SqlClientException e) {
			assertTrue(e.getMessage().contains(
					String.format("'%s' and '%s' can not be set both", newKey, "execution." + deprecatedKeyWithoutPrefix)));
		}
	}

	@Test
	public void testDuplicateCatalog() {
		exception.expect(SqlClientException.class);
		exception.expectMessage("Cannot create catalog 'catalog2' because a catalog with this name is already registered.");
		Environment env = new Environment();
		env.setCatalogs(Arrays.asList(
			createCatalog("catalog1", "test"),
			createCatalog("catalog2", "test"),
			createCatalog("catalog2", "test")));
	}

	@Test
	public void testDuplicateModules() {
		exception.expect(SqlClientException.class);
		Environment env = new Environment();
		env.setModules(Arrays.asList(
			createModule("module1", "test"),
			createModule("module2", "test"),
			createModule("module2", "test")));
	}

	@Test
	public void testModuleOrder() {
		Environment env1 = new Environment();
		Environment env2 = new Environment();
		env1.setModules(Arrays.asList(
			createModule("b", "test"),
			createModule("d", "test")));

		env2.setModules(Arrays.asList(
			createModule("c", "test"),
			createModule("a", "test")));

		assertEquals(
			Arrays.asList("b", "d"), new ArrayList<>(env1.getModules().keySet())
		);

		assertEquals(
			Arrays.asList("c", "a"), new ArrayList<>(env2.getModules().keySet())
		);

		Environment env = Environment.merge(env1, env2);

		assertEquals(
			Arrays.asList("b", "d", "c", "a"), new ArrayList<>(env.getModules().keySet())
		);
	}

	private static Map<String, Object> createCatalog(String name, String type) {
		Map<String, Object> prop = new HashMap<>();

		prop.put(CATALOG_NAME, name);
		prop.put(CATALOG_TYPE, type);

		return prop;
	}

	private static Map<String, Object> createModule(String name, String type) {
		Map<String, Object> prop = new HashMap<>();

		prop.put(MODULE_NAME, name);
		prop.put(MODULE_TYPE, type);

		return prop;
	}
}
