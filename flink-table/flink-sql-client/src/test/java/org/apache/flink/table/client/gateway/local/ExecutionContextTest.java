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
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.CatalogEntry;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.DummyTableSourceFactory;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.util.StringUtils;

import org.apache.commons.cli.Options;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ExecutionContext}.
 */
public class ExecutionContextTest {

	private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
	private static final String MODULES_ENVIRONMENT_FILE = "test-sql-client-modules.yaml";
	private static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-catalogs.yaml";
	private static final String STREAMING_ENVIRONMENT_FILE = "test-sql-client-streaming.yaml";
	private static final String CONFIGURATION_ENVIRONMENT_FILE = "test-sql-client-configuration.yaml";

	@Test
	public void testExecutionConfig() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final ExecutionConfig config = context.getExecutionConfig();

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
	public void testModules() throws Exception {
		final ExecutionContext<?> context = createModuleExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();

		Set<String> allModules = new HashSet<>(Arrays.asList(tableEnv.listModules()));
		assertEquals(4, allModules.size());
		assertEquals(
			new HashSet<>(
				Arrays.asList(
					"core",
					"mymodule",
					"myhive",
					"myhive2")
			),
			allModules
		);
	}

	@Test
	public void testCatalogs() throws Exception {
		final String inmemoryCatalog = "inmemorycatalog";
		final String hiveCatalog = "hivecatalog";
		final String hiveDefaultVersionCatalog = "hivedefaultversion";

		final ExecutionContext<?> context = createCatalogExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();

		assertEquals(inmemoryCatalog, tableEnv.getCurrentCatalog());
		assertEquals("mydatabase", tableEnv.getCurrentDatabase());

		Catalog catalog = tableEnv.getCatalog(hiveCatalog).orElse(null);
		assertNotNull(catalog);
		assertTrue(catalog instanceof HiveCatalog);
		assertEquals("2.3.4", ((HiveCatalog) catalog).getHiveVersion());

		catalog = tableEnv.getCatalog(hiveDefaultVersionCatalog).orElse(null);
		assertNotNull(catalog);
		assertTrue(catalog instanceof HiveCatalog);
		// make sure we have assigned a default hive version
		assertFalse(StringUtils.isNullOrWhitespaceOnly(((HiveCatalog) catalog).getHiveVersion()));

		tableEnv.useCatalog(hiveCatalog);

		assertEquals(hiveCatalog, tableEnv.getCurrentCatalog());

		Set<String> allCatalogs = new HashSet<>(Arrays.asList(tableEnv.listCatalogs()));
		assertEquals(6, allCatalogs.size());
		assertEquals(
			new HashSet<>(
				Arrays.asList(
					"default_catalog",
					inmemoryCatalog,
					hiveCatalog,
					hiveDefaultVersionCatalog,
					"catalog1",
					"catalog2")
			),
			allCatalogs
		);
	}

	@Test
	public void testDatabases() throws Exception {
		final String hiveCatalog = "hivecatalog";

		final ExecutionContext<?> context = createCatalogExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();

		assertEquals(1, tableEnv.listDatabases().length);
		assertEquals("mydatabase", tableEnv.listDatabases()[0]);

		tableEnv.useCatalog(hiveCatalog);

		assertEquals(2, tableEnv.listDatabases().length);
		assertEquals(
			new HashSet<>(
				Arrays.asList(
					HiveCatalog.DEFAULT_DB,
					DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE)
			),
			new HashSet<>(Arrays.asList(tableEnv.listDatabases()))
		);

		tableEnv.useCatalog(hiveCatalog);

		assertEquals(HiveCatalog.DEFAULT_DB, tableEnv.getCurrentDatabase());

		tableEnv.useDatabase(DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE);

		assertEquals(DependencyTest.TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE, tableEnv.getCurrentDatabase());
	}

	@Test
	public void testFunctions() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();
		final String[] expected = new String[]{"scalarudf", "tableudf", "aggregateudf"};
		final String[] actual = tableEnv.listUserDefinedFunctions();
		Arrays.sort(expected);
		Arrays.sort(actual);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testTables() throws Exception {
		final ExecutionContext<?> context = createDefaultExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();

		assertArrayEquals(
			new String[]{"TableNumber1", "TableNumber2", "TableSourceSink", "TestView1", "TestView2"},
			tableEnv.listTables());
	}

	@Test
	public void testTemporalTables() throws Exception {
		final ExecutionContext<?> context = createStreamingExecutionContext();
		final StreamTableEnvironment tableEnv = (StreamTableEnvironment) context.getTableEnvironment();

		assertArrayEquals(
			new String[]{"EnrichmentSource", "HistorySource", "HistoryView", "TemporalTableUsage"},
			tableEnv.listTables());

		assertArrayEquals(
			new String[]{"sourcetemporaltable", "viewtemporaltable"},
			tableEnv.listUserDefinedFunctions());

		assertArrayEquals(
			new String[]{"integerField", "stringField", "rowtimeField", "integerField0", "stringField0", "rowtimeField0"},
			tableEnv.scan("TemporalTableUsage").getSchema().getFieldNames());
	}

	@Test
	public void testConfiguration() throws Exception {
		final ExecutionContext<?> context = createConfigurationExecutionContext();
		final TableEnvironment tableEnv = context.getTableEnvironment();

		assertEquals(
			100,
			tableEnv.getConfig().getConfiguration().getInteger(
				ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT));
		assertTrue(
			tableEnv.getConfig().getConfiguration().getBoolean(
				ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED));
		assertEquals(
			"128kb",
			tableEnv.getConfig().getConfiguration().getString(
				ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE));

		assertTrue(
			tableEnv.getConfig().getConfiguration().getBoolean(
				OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED));

		// these options are not modified and should be equal to their default value
		assertEquals(
			ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED.defaultValue(),
			tableEnv.getConfig().getConfiguration().getBoolean(
				ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
		assertEquals(
			ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE.defaultValue(),
			tableEnv.getConfig().getConfiguration().getString(
				ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE));
		assertEquals(
			OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD.defaultValue().longValue(),
			tableEnv.getConfig().getConfiguration().getLong(
				OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD));
	}

	@Test
	public void testInitCatalogs() throws Exception{
		final Map<String, String> replaceVars = createDefaultReplaceVars();
		Environment env = EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars);

		Map<String, Object> catalogProps = new HashMap<>();
		catalogProps.put("name", "test");
		catalogProps.put("type", "test_cl_catalog");
		env.getCatalogs().clear();
		env.getCatalogs().put("test", CatalogEntry.create(catalogProps));
		Configuration flinkConfig = new Configuration();
		ExecutionContext.builder(env,
				new SessionContext("test-session", new Environment()),
				Collections.emptyList(),
				flinkConfig,
				new DefaultClusterClientServiceLoader(),
				new Options(),
				Collections.singletonList(new DefaultCLI(flinkConfig))).build();
	}

	@SuppressWarnings("unchecked")
	private <T> ExecutionContext<T> createExecutionContext(String file, Map<String, String> replaceVars) throws Exception {
		final Environment env = EnvironmentFileUtil.parseModified(
			file,
			replaceVars);
		final Configuration flinkConfig = new Configuration();
		return (ExecutionContext<T>) ExecutionContext.builder(
				env,
				new SessionContext("test-session", new Environment()),
				Collections.emptyList(),
				flinkConfig,
				new DefaultClusterClientServiceLoader(),
				new Options(),
				Collections.singletonList(new DefaultCLI(flinkConfig)))
				.build();
	}

	private Map<String, String> createDefaultReplaceVars() {
		Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", "old");
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
		return replaceVars;
	}

	private <T> ExecutionContext<T> createDefaultExecutionContext() throws Exception {
		final Map<String, String> replaceVars = createDefaultReplaceVars();
		return createExecutionContext(DEFAULTS_ENVIRONMENT_FILE, replaceVars);
	}

	private <T> ExecutionContext<T> createModuleExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", "old");
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		return createExecutionContext(MODULES_ENVIRONMENT_FILE, replaceVars);
	}

	private <T> ExecutionContext<T> createCatalogExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_PLANNER", "old");
		replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
		replaceVars.put("$VAR_RESULT_MODE", "changelog");
		replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
		replaceVars.put("$VAR_MAX_ROWS", "100");
		return createExecutionContext(CATALOGS_ENVIRONMENT_FILE, replaceVars);
	}

	private <T> ExecutionContext<T> createStreamingExecutionContext() throws Exception {
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_CONNECTOR_TYPE", DummyTableSourceFactory.CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY", DummyTableSourceFactory.TEST_PROPERTY);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "");
		return createExecutionContext(STREAMING_ENVIRONMENT_FILE, replaceVars);
	}

	private <T> ExecutionContext<T> createConfigurationExecutionContext() throws Exception {
		return createExecutionContext(CONFIGURATION_ENVIRONMENT_FILE, new HashMap<>());
	}

	// a catalog that requires the thread context class loader to be a user code classloader during construction and opening
	private static class TestClassLoaderCatalog extends GenericInMemoryCatalog {

		private static final Class parentFirstCL = FlinkUserCodeClassLoaders.parentFirst(
				new URL[0], TestClassLoaderCatalog.class.getClassLoader()).getClass();
		private static final Class childFirstCL = FlinkUserCodeClassLoaders.childFirst(
				new URL[0], TestClassLoaderCatalog.class.getClassLoader(), new String[0]).getClass();

		TestClassLoaderCatalog(String name) {
			super(name);
			verifyUserClassLoader();
		}

		@Override
		public void open() {
			verifyUserClassLoader();
			super.open();
		}

		private void verifyUserClassLoader() {
			ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
			assertTrue(parentFirstCL.isInstance(contextLoader) || childFirstCL.isInstance(contextLoader));
		}
	}

	/**
	 * Factory to create TestClassLoaderCatalog.
	 */
	public static class TestClassLoaderCatalogFactory implements CatalogFactory {

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			return new TestClassLoaderCatalog("test_cl");
		}

		@Override
		public Map<String, String> requiredContext() {
			Map<String, String> context = new HashMap<>();
			context.put("type", "test_cl_catalog");
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			return Collections.emptyList();
		}
	}
}
