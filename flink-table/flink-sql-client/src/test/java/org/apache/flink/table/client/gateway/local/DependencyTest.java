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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Dependency tests for {@link LocalExecutor}. Mainly for testing classloading of dependencies. */
public class DependencyTest {

    public static final String CONNECTOR_TYPE_VALUE = "test-connector";
    public static final String TEST_PROPERTY = "test-property";
    private static final String TEST_PROPERTY_VALUE = "test-value";

    public static final String CATALOG_TYPE_TEST = "DependencyTest";

    private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";
    private static final List<String> INIT_SQL =
            Arrays.asList(
                    String.format(
                            "CREATE TABLE TableNumber1 (\n"
                                    + "  IntegerField1 INT,\n"
                                    + "  StringField1 STRING,\n"
                                    + "  rowtimeField TIMESTAMP(3),\n"
                                    + "  WATERMARK FOR rowtimeField AS rowtimeField\n"
                                    + ") WITH (\n"
                                    + "  'connector' = '%s',\n"
                                    + "  '%s' = '%s'\n"
                                    + ")",
                            CONNECTOR_TYPE_VALUE, TEST_PROPERTY, TEST_PROPERTY_VALUE),
                    String.format(
                            "CREATE CATALOG TestCatalog WITH ('type' = '%s')", CATALOG_TYPE_TEST));

    private static final String SESSION_ID = "test-session";

    @Test
    public void testTableFactoryDiscovery() throws Exception {
        final LocalExecutor executor = createLocalExecutor();
        try {
            final TableResult tableResult =
                    executeSql(executor, SESSION_ID, "DESCRIBE TableNumber1");
            assertEquals(
                    tableResult.getResolvedSchema(),
                    ResolvedSchema.physical(
                            new String[] {"name", "type", "null", "key", "extras", "watermark"},
                            new DataType[] {
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.BOOLEAN(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING()
                            }));
            List<Row> schemaData =
                    Arrays.asList(
                            Row.of("IntegerField1", "INT", true, null, null, null),
                            Row.of("StringField1", "STRING", true, null, null, null),
                            Row.of(
                                    "rowtimeField",
                                    "TIMESTAMP(3) *ROWTIME*",
                                    true,
                                    null,
                                    null,
                                    "`rowtimeField`"));
            assertEquals(schemaData, CollectionUtil.iteratorToList(tableResult.collect()));
        } finally {
            executor.closeSession(SESSION_ID);
        }
    }

    @Test
    public void testSqlParseWithUserClassLoader() throws Exception {
        final LocalExecutor executor = createLocalExecutor();
        try {
            Operation operation =
                    executor.parseStatement(
                            SESSION_ID, "SELECT IntegerField1, StringField1 FROM TableNumber1");

            assertTrue(operation instanceof QueryOperation);
        } finally {
            executor.closeSession(SESSION_ID);
        }
    }

    private LocalExecutor createLocalExecutor() throws Exception {
        // create executor with dependencies
        final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
        // create default context
        DefaultContext defaultContext =
                new DefaultContext(
                        Collections.singletonList(dependency),
                        new Configuration(),
                        Collections.singletonList(new DefaultCLI()));
        LocalExecutor executor = new LocalExecutor(defaultContext);
        executor.openSession(SESSION_ID);
        for (String line : INIT_SQL) {
            executor.executeOperation(SESSION_ID, executor.parseStatement(SESSION_ID, line));
        }
        return executor;
    }

    private TableResult executeSql(Executor executor, String sessionId, String sql) {
        Operation operation = executor.parseStatement(sessionId, sql);
        return executor.executeOperation(sessionId, operation);
    }

    // --------------------------------------------------------------------------------------------

    /** Table source that can be discovered if classloading is correct. */
    public static class TestTableSourceFactory implements DynamicTableSourceFactory {

        @Override
        public String factoryIdentifier() {
            return CONNECTOR_TYPE_VALUE;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.singleton(
                    ConfigOptions.key(CONNECTOR_TYPE_VALUE).stringType().noDefaultValue());
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }

        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            return null;
        }
    }

    /** Catalog that can be discovered if classloading is correct. */
    public static class TestCatalogFactory implements CatalogFactory {

        private static final ConfigOption<String> DEFAULT_DATABASE =
                ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                        .stringType()
                        .defaultValue(GenericInMemoryCatalog.DEFAULT_DB);

        @Override
        public String factoryIdentifier() {
            return CATALOG_TYPE_TEST;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.add(DEFAULT_DATABASE);
            return options;
        }

        @Override
        public Catalog createCatalog(Context context) {
            final Configuration configuration = Configuration.fromMap(context.getOptions());
            return new TestCatalog(context.getName(), configuration.getString(DEFAULT_DATABASE));
        }
    }

    /** Test catalog. */
    public static class TestCatalog extends GenericInMemoryCatalog {
        public TestCatalog(String name, String defaultDatabase) {
            super(name, defaultDatabase);
        }
    }

    /**
     * A test factory that is the same as {@link HiveCatalogFactory} except returning a {@link
     * HiveCatalog} always with an embedded Hive metastore to test logic of {@link
     * HiveCatalogFactory}.
     */
    public static class TestHiveCatalogFactory extends HiveCatalogFactory {
        public static final String ADDITIONAL_TEST_DATABASE = "additional_test_database";
        public static final String TEST_TABLE = "test_table";
        static final String TABLE_WITH_PARAMETERIZED_TYPES = "param_types_table";

        @Override
        public String factoryIdentifier() {
            return "hive-test";
        }

        @Override
        public Catalog createCatalog(Context context) {
            final Configuration configuration = Configuration.fromMap(context.getOptions());

            // Developers may already have their own production/testing hive-site.xml set in their
            // environment,
            // and Flink tests should avoid using those hive-site.xml.
            // Thus, explicitly create a testing HiveConf for unit tests here
            Catalog hiveCatalog =
                    HiveTestUtils.createHiveCatalog(
                            context.getName(),
                            configuration.getString(HiveCatalogFactoryOptions.HIVE_VERSION));

            // Creates an additional database to test tableEnv.useDatabase() will switch current
            // database of the catalog
            hiveCatalog.open();
            try {
                hiveCatalog.createDatabase(
                        ADDITIONAL_TEST_DATABASE,
                        new CatalogDatabaseImpl(new HashMap<>(), null),
                        false);
                hiveCatalog.createTable(
                        new ObjectPath(ADDITIONAL_TEST_DATABASE, TEST_TABLE),
                        createResolvedTable(
                                new String[] {"testcol"}, new DataType[] {DataTypes.INT()}),
                        false);
                // create a table to test parameterized types
                hiveCatalog.createTable(
                        new ObjectPath("default", TABLE_WITH_PARAMETERIZED_TYPES),
                        createResolvedTable(
                                new String[] {"dec", "ch", "vch"},
                                new DataType[] {
                                    DataTypes.DECIMAL(10, 10),
                                    DataTypes.CHAR(5),
                                    DataTypes.VARCHAR(15)
                                }),
                        false);
            } catch (DatabaseAlreadyExistException
                    | TableAlreadyExistException
                    | DatabaseNotExistException e) {
                throw new CatalogException(e);
            }

            return hiveCatalog;
        }

        private ResolvedCatalogTable createResolvedTable(
                String[] fieldNames, DataType[] fieldDataTypes) {
            final Map<String, String> options = new HashMap<>();
            options.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
            final CatalogTable origin =
                    CatalogTable.of(
                            Schema.newBuilder().fromFields(fieldNames, fieldDataTypes).build(),
                            null,
                            Collections.emptyList(),
                            options);
            final List<Column> resolvedColumns =
                    IntStream.range(0, fieldNames.length)
                            .mapToObj(i -> Column.physical(fieldNames[i], fieldDataTypes[i]))
                            .collect(Collectors.toList());
            return new ResolvedCatalogTable(
                    origin, new ResolvedSchema(resolvedColumns, Collections.emptyList(), null));
        }
    }
}
