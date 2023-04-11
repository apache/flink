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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.connector.datagen.table.DataGenTableSourceFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX;
import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for HiveCatalog. */
public class HiveCatalogTest {

    ResolvedSchema resolvedSchema =
            ResolvedSchema.of(
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("age", DataTypes.INT()));

    Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();

    private static HiveCatalog hiveCatalog;
    private final ObjectPath tablePath = new ObjectPath("default", "test");

    @BeforeClass
    public static void createCatalog() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @After
    public void after() throws Exception {
        hiveCatalog.dropTable(tablePath, true);
    }

    @Test
    public void testCreateAndGetFlinkManagedTable() throws Exception {
        CatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema,
                                "Flink managed table",
                                new ArrayList<>(),
                                Collections.emptyMap()),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, table, false);
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getParameters())
                .containsEntry(
                        FLINK_PROPERTY_PREFIX + CONNECTOR.key(),
                        ManagedTableFactory.DEFAULT_IDENTIFIER);
        CatalogBaseTable retrievedTable = hiveCatalog.instantiateCatalogTable(hiveTable);
        assertThat(retrievedTable.getOptions()).isEmpty();
    }

    @Test
    public void testAlterFlinkNonManagedTableToFlinkManagedTable() throws Exception {
        Map<String, String> originOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema,
                                "Flink non-managed table",
                                new ArrayList<>(),
                                originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = Collections.emptyMap();

        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink managed table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'FLINK_NON_MANAGED_TABLE', but new table type is 'FLINK_MANAGED_TABLE'");
    }

    @Test
    public void testAlterFlinkNonManagedTableToHiveTable() throws Exception {
        Map<String, String> originOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema,
                                "Flink non-managed table",
                                new ArrayList<>(),
                                originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = getLegacyFileSystemConnectorOptions("/test_path");
        newOptions.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "Hive table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'FLINK_NON_MANAGED_TABLE', but new table type is 'HIVE_TABLE'");
    }

    @Test
    public void testAlterFlinkManagedTableToFlinkManagedTable() throws Exception {
        Map<String, String> originOptions = Collections.emptyMap();
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink managed table", new ArrayList<>(), originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink non-managed table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'FLINK_MANAGED_TABLE', but new table type is 'FLINK_NON_MANAGED_TABLE'");
    }

    @Test
    public void testAlterFlinkManagedTableToHiveTable() throws Exception {
        Map<String, String> originOptions = Collections.emptyMap();
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink managed table", new ArrayList<>(), originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = getLegacyFileSystemConnectorOptions("/test_path");
        newOptions.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "Hive table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'FLINK_MANAGED_TABLE', but new table type is 'HIVE_TABLE'");
    }

    @Test
    public void testAlterHiveTableToFlinkManagedTable() throws Exception {
        Map<String, String> originOptions = getLegacyFileSystemConnectorOptions("/test_path");
        originOptions.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "Hive table", new ArrayList<>(), originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = Collections.emptyMap();
        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink managed table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'HIVE_TABLE', but new table type is 'FLINK_MANAGED_TABLE'");
    }

    @Test
    public void testAlterHiveTableToFlinkNonManagedTable() throws Exception {
        Map<String, String> originOptions = getLegacyFileSystemConnectorOptions("/test_path");
        originOptions.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        CatalogTable originTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "Hive table", new ArrayList<>(), originOptions),
                        resolvedSchema);
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable newTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                schema, "Flink managed table", new ArrayList<>(), newOptions),
                        resolvedSchema);
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'HIVE_TABLE', but new table type is 'FLINK_NON_MANAGED_TABLE'");
    }

    @Test
    public void testCreateGenericTable() {
        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        schema,
                                        null,
                                        new ArrayList<>(),
                                        getLegacyFileSystemConnectorOptions("/test_path")),
                                resolvedSchema),
                        HiveTestUtils.createHiveConf(),
                        false);

        Map<String, String> prop = hiveTable.getParameters();
        assertThat(HiveCatalog.isHiveTable(prop)).isFalse();
        assertThat(prop.keySet())
                .allMatch(k -> k.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX));
    }

    @Test
    public void testCreateHiveTable() {
        Map<String, String> options = getLegacyFileSystemConnectorOptions("/test_path");
        options.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);

        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new ResolvedCatalogTable(
                                CatalogTable.of(schema, null, new ArrayList<>(), options),
                                resolvedSchema),
                        HiveTestUtils.createHiveConf(),
                        false);

        Map<String, String> prop = hiveTable.getParameters();
        assertThat(HiveCatalog.isHiveTable(prop)).isTrue();
        assertThat(prop.keySet())
                .noneMatch(k -> k.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX));
    }

    @Test
    public void testRetrieveFlinkProperties() throws Exception {
        ObjectPath hiveObjectPath =
                new ObjectPath(HiveCatalog.DEFAULT_DB, "testRetrieveProperties");

        Map<String, String> options = getLegacyFileSystemConnectorOptions("/test_path");
        options.put(CONNECTOR.key(), "jdbc");
        options.put("url", "jdbc:clickhouse://host:port/testUrl1");
        options.put("flink.url", "jdbc:clickhouse://host:port/testUrl2");

        hiveCatalog.createTable(
                hiveObjectPath,
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, null, new ArrayList<>(), options), resolvedSchema),
                false);

        CatalogBaseTable hiveTable = hiveCatalog.getTable(hiveObjectPath);
        assertThat(hiveTable.getOptions())
                .containsEntry("url", "jdbc:clickhouse://host:port/testUrl1");
        assertThat(hiveTable.getOptions())
                .containsEntry("flink.url", "jdbc:clickhouse://host:port/testUrl2");
    }

    @Test
    public void testCreateHiveConf() {
        // hive-conf-dir not specified, should read hive-site from classpath
        HiveConf hiveConf = HiveCatalog.createHiveConf(null, null);
        assertThat(hiveConf.get("common-key")).isEqualTo("common-val");
        // hive-conf-dir specified, shouldn't read hive-site from classpath
        String hiveConfDir =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-catalog-factory-conf")
                        .getPath();
        hiveConf = HiveCatalog.createHiveConf(hiveConfDir, null);
        assertThat(hiveConf.get("common-key")).isNull();
    }

    @Test
    public void testGetNoSchemaGenericTable() throws Exception {
        ObjectPath hiveObjectPath =
                new ObjectPath(HiveCatalog.DEFAULT_DB, "testGetNoSchemaGenericTable");

        Map<String, String> properties = new HashMap<>();

        properties.put(CONNECTOR.key(), "jdbc");

        hiveCatalog.createTable(
                hiveObjectPath,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().build(), null, new ArrayList<>(), properties),
                        ResolvedSchema.of()),
                false);

        CatalogBaseTable catalogTable = hiveCatalog.getTable(hiveObjectPath);
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(Schema.newBuilder().build());
    }

    @Test
    public void testFunction() throws Exception {
        List<ResourceUri> resourceUris =
                Arrays.asList(
                        new ResourceUri(ResourceType.JAR, "jar/path"),
                        new ResourceUri(ResourceType.FILE, "file/path"),
                        new ResourceUri(ResourceType.ARCHIVE, "archive/path"));
        for (FunctionLanguage functionLanguage : FunctionLanguage.values()) {
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(
                            "test-catalog-function", functionLanguage, resourceUris);
            ObjectPath functionPath = new ObjectPath("default", "test_f");
            hiveCatalog.createFunction(functionPath, catalogFunction, false);
            checkCatalogFunction(catalogFunction, hiveCatalog.getFunction(functionPath));
            hiveCatalog.dropFunction(functionPath, false);
        }
    }

    private static Map<String, String> getLegacyFileSystemConnectorOptions(String path) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector.type", "filesystem");
        options.put("connector.path", path);

        return options;
    }

    private void checkCatalogFunction(
            CatalogFunction expectFunction, CatalogFunction actualFunction) {
        assertThat(actualFunction.getFunctionLanguage())
                .isEqualTo(expectFunction.getFunctionLanguage());
        assertThat(actualFunction.getClassName()).isEqualTo(expectFunction.getClassName());
        assertThat(actualFunction.getFunctionResources())
                .isEqualTo(expectFunction.getFunctionResources());
        assertThat(actualFunction.getFunctionLanguage())
                .isEqualTo(expectFunction.getFunctionLanguage());
    }
}
