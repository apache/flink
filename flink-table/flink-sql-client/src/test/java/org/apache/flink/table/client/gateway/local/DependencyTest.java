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
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.TestTableSinkFactoryBase;
import org.apache.flink.table.client.gateway.utils.TestTableSourceFactoryBase;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Dependency tests for {@link LocalExecutor}. Mainly for testing classloading of dependencies.
 */
public class DependencyTest {

	public static final String CONNECTOR_TYPE_VALUE = "test-connector";
	public static final String TEST_PROPERTY = "test-property";

	public static final String CATALOG_TYPE_TEST = "DependencyTest";
	public static final String MODULE_TYPE_TEST = "ModuleDependencyTest";

	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";
	private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";

	@Test
	public void testTableFactoryDiscovery() throws Exception {
		final LocalExecutor executor = createExecutor();
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			final TableResult tableResult = executor.executeSql(sessionId, "DESCRIBE TableNumber1");
			assertEquals(
					tableResult.getTableSchema(),
					TableSchema.builder().fields(
							new String[] { "name", "type", "null", "key", "extras", "watermark" },
							new DataType[] { DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(),
									DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING() }
					).build()
			);
			List<Row> schemaData = Arrays.asList(
					Row.of("IntegerField1", "INT", true, null, null, null),
					Row.of("StringField1", "STRING", true, null, null, null),
					Row.of("rowtimeField", "TIMESTAMP(3) *ROWTIME*", true, null, null, null)
			);
			assertEquals(schemaData, CollectionUtil.iteratorToList(tableResult.collect()));
		} finally {
			executor.closeSession(sessionId);
		}
	}

	@Test
	public void testSqlParseWithUserClassLoader() throws Exception {
		final LocalExecutor executor = createExecutor();
		final SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);
		try {
			final Parser sqlParser = executor.getSqlParser(sessionId);
			List<Operation> operations = sqlParser.parse("SELECT IntegerField1, StringField1 FROM TableNumber1");

			assertTrue(operations != null && operations.size() == 1);
		} finally {
			executor.closeSession(sessionId);
		}
	}

	private LocalExecutor createExecutor() throws Exception {
		// create environment
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_CONNECTOR_TYPE", CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY", TEST_PROPERTY);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "test-value");
		final Environment env = EnvironmentFileUtil.parseModified(FACTORY_ENVIRONMENT_FILE, replaceVars);

		// create executor with dependencies
		final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
		return new LocalExecutor(
			env,
			Collections.singletonList(dependency),
			new Configuration(),
			new DefaultCLI(),
			new DefaultClusterClientServiceLoader());
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Table source that can be discovered if classloading is correct.
	 */
	public static class TestTableSourceFactory extends TestTableSourceFactoryBase {

		public TestTableSourceFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * Table sink that can be discovered if classloading is correct.
	 */
	public static class TestTableSinkFactory extends TestTableSinkFactoryBase {

		public TestTableSinkFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * Module that can be discovered if classloading is correct.
	 */
	public static class TestModuleFactory implements ModuleFactory {

		@Override
		public Module createModule(Map<String, String> properties) {
			return new TestModule();
		}

		@Override
		public Map<String, String> requiredContext() {
			final Map<String, String> context = new HashMap<>();
			context.put(MODULE_TYPE, MODULE_TYPE_TEST);
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			final List<String> properties = new ArrayList<>();
			properties.add("test");
			return properties;
		}
	}

	/**
	 * Test module.
	 */
	public static class TestModule implements Module {

	}

	/**
	 * Catalog that can be discovered if classloading is correct.
	 */
	public static class TestCatalogFactory implements CatalogFactory {

		@Override
		public Map<String, String> requiredContext() {
			final Map<String, String> context = new HashMap<>();
			context.put(CATALOG_TYPE, CATALOG_TYPE_TEST);
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			final List<String> properties = new ArrayList<>();
			properties.add(CATALOG_DEFAULT_DATABASE);
			return properties;
		}

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			final DescriptorProperties params = new DescriptorProperties(true);
			params.putProperties(properties);

			final Optional<String> defaultDatabase = params.getOptionalString(CATALOG_DEFAULT_DATABASE);

			return new TestCatalog(name, defaultDatabase.orElse(GenericInMemoryCatalog.DEFAULT_DB));
		}
	}

	/**
	 * Test catalog.
	 */
	public static class TestCatalog extends GenericInMemoryCatalog {
		public TestCatalog(String name, String defaultDatabase) {
			super(name, defaultDatabase);
		}
	}

	/**
	 * A test factory that is the same as {@link HiveCatalogFactory}
	 * except returning a {@link HiveCatalog} always with an embedded Hive metastore
	 * to test logic of {@link HiveCatalogFactory}.
	 */
	public static class TestHiveCatalogFactory extends HiveCatalogFactory {
		public static final String ADDITIONAL_TEST_DATABASE = "additional_test_database";
		public static final String TEST_TABLE = "test_table";
		static final String TABLE_WITH_PARAMETERIZED_TYPES = "param_types_table";

		@Override
		public Map<String, String> requiredContext() {
			Map<String, String> context = super.requiredContext();

			// For factory discovery service to distinguish TestHiveCatalogFactory from HiveCatalogFactory
			context.put("test", "test");
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			List<String> list = super.supportedProperties();
			list.add(CatalogConfig.IS_GENERIC);

			return list;
		}

		@Override
		public Catalog createCatalog(String name, Map<String, String> properties) {
			// Developers may already have their own production/testing hive-site.xml set in their environment,
			// and Flink tests should avoid using those hive-site.xml.
			// Thus, explicitly create a testing HiveConf for unit tests here
			Catalog hiveCatalog = HiveTestUtils.createHiveCatalog(name, properties.get(HiveCatalogValidator.CATALOG_HIVE_VERSION));

			// Creates an additional database to test tableEnv.useDatabase() will switch current database of the catalog
			hiveCatalog.open();
			try {
				hiveCatalog.createDatabase(
					ADDITIONAL_TEST_DATABASE,
					new CatalogDatabaseImpl(new HashMap<>(), null),
					false);
				hiveCatalog.createTable(
					new ObjectPath(ADDITIONAL_TEST_DATABASE, TEST_TABLE),
					new CatalogTableImpl(
						TableSchema.builder()
							.field("testcol", DataTypes.INT())
							.build(),
						new HashMap<String, String>() {{
							put(CatalogConfig.IS_GENERIC, String.valueOf(false));
						}},
						""
					),
					false
				);
				// create a table to test parameterized types
				hiveCatalog.createTable(new ObjectPath("default", TABLE_WITH_PARAMETERIZED_TYPES),
						tableWithParameterizedTypes(),
						false);
			} catch (DatabaseAlreadyExistException | TableAlreadyExistException | DatabaseNotExistException e) {
				throw new CatalogException(e);
			}

			return hiveCatalog;
		}

		private CatalogTable tableWithParameterizedTypes() {
			TableSchema tableSchema = TableSchema.builder().fields(new String[]{"dec", "ch", "vch"},
					new DataType[]{DataTypes.DECIMAL(10, 10), DataTypes.CHAR(5), DataTypes.VARCHAR(15)}).build();
			return new CatalogTableImpl(
				tableSchema,
				new HashMap<String, String>() {{
					put(CatalogConfig.IS_GENERIC, String.valueOf(false));
				}},
				"");
		}
	}
}
