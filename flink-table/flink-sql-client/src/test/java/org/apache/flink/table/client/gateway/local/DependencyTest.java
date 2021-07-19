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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.TestTableSinkFactoryBase;
import org.apache.flink.table.client.gateway.utils.TestTableSourceFactoryBase;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.descriptors.ModuleDescriptorValidator.MODULE_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Dependency tests for {@link LocalExecutor}. Mainly for testing classloading of dependencies. */
public class DependencyTest {

    public static final String CONNECTOR_TYPE_VALUE = "test-connector";
    public static final String TEST_PROPERTY = "test-property";

    public static final String CATALOG_TYPE_TEST = "DependencyTest";
    public static final String MODULE_TYPE_TEST = "ModuleDependencyTest";

    private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";
    private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";

    @Test
    public void testTableFactoryDiscovery() throws Exception {
        final LocalExecutor executor = createLocalExecutor();
        String sessionId = executor.openSession("test-session");
        try {
            final TableResult tableResult =
                    executeSql(executor, sessionId, "DESCRIBE TableNumber1");
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
                                    null));
            assertEquals(schemaData, CollectionUtil.iteratorToList(tableResult.collect()));
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test
    public void testSqlParseWithUserClassLoader() throws Exception {
        final LocalExecutor executor = createLocalExecutor();
        String sessionId = executor.openSession("test-session");
        try {
            Operation operation =
                    executor.parseStatement(
                            sessionId, "SELECT IntegerField1, StringField1 FROM TableNumber1");

            assertTrue(operation instanceof QueryOperation);
        } finally {
            executor.closeSession(sessionId);
        }
    }

    private LocalExecutor createLocalExecutor() throws Exception {
        // create environment
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_CONNECTOR_TYPE", CONNECTOR_TYPE_VALUE);
        replaceVars.put("$VAR_CONNECTOR_PROPERTY", TEST_PROPERTY);
        replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "test-value");
        final Environment env =
                EnvironmentFileUtil.parseModified(FACTORY_ENVIRONMENT_FILE, replaceVars);

        // create executor with dependencies
        final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
        // create default context
        DefaultContext defaultContext =
                new DefaultContext(
                        env,
                        Collections.singletonList(dependency),
                        new Configuration(),
                        Collections.singletonList(new DefaultCLI()));
        return new LocalExecutor(defaultContext);
    }

    private TableResult executeSql(Executor executor, String sessionId, String sql) {
        Operation operation = executor.parseStatement(sessionId, sql);
        return executor.executeOperation(sessionId, operation);
    }

    // --------------------------------------------------------------------------------------------

    /** Table source that can be discovered if classloading is correct. */
    public static class TestTableSourceFactory extends TestTableSourceFactoryBase {

        public TestTableSourceFactory() {
            super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
        }
    }

    /** Table sink that can be discovered if classloading is correct. */
    public static class TestTableSinkFactory extends TestTableSinkFactoryBase {

        public TestTableSinkFactory() {
            super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
        }
    }

    /** Module that can be discovered if classloading is correct. */
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

    /** Test module. */
    public static class TestModule implements Module {}

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
}
