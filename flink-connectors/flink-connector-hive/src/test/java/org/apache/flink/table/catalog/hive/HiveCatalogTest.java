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
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ManagedTableFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.catalog.CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for HiveCatalog. */
public class HiveCatalogTest {

    TableSchema schema =
            TableSchema.builder()
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .build();
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
                new CatalogTableImpl(schema, Collections.emptyMap(), "Flink managed table");
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
                new CatalogTableImpl(schema, originOptions, "Flink non-managed table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = Collections.emptyMap();
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Flink managed table");
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
                new CatalogTableImpl(schema, originOptions, "Flink non-managed table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = getLegacyFileSystemConnectorOptions("/test_path");
        newOptions.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Hive table");
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
                new CatalogTableImpl(schema, originOptions, "Flink managed table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Flink non-managed table");
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
                new CatalogTableImpl(schema, originOptions, "Flink managed table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = getLegacyFileSystemConnectorOptions("/test_path");
        newOptions.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Hive table");
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'FLINK_MANAGED_TABLE', but new table type is 'HIVE_TABLE'");
    }

    @Test
    public void testAlterHiveTableToFlinkManagedTable() throws Exception {
        Map<String, String> originOptions = getLegacyFileSystemConnectorOptions("/test_path");
        originOptions.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        CatalogTable originTable = new CatalogTableImpl(schema, originOptions, "Hive table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions = Collections.emptyMap();
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Flink managed table");
        assertThatThrownBy(() -> hiveCatalog.alterTable(tablePath, newTable, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Changing catalog table type is not allowed. "
                                + "Existing table type is 'HIVE_TABLE', but new table type is 'FLINK_MANAGED_TABLE'");
    }

    @Test
    public void testAlterHiveTableToFlinkNonManagedTable() throws Exception {
        Map<String, String> originOptions = getLegacyFileSystemConnectorOptions("/test_path");
        originOptions.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        CatalogTable originTable = new CatalogTableImpl(schema, originOptions, "Hive table");
        hiveCatalog.createTable(tablePath, originTable, false);

        Map<String, String> newOptions =
                Collections.singletonMap(
                        FactoryUtil.CONNECTOR.key(), DataGenTableSourceFactory.IDENTIFIER);
        CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "Flink managed table");
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
                        new CatalogTableImpl(
                                schema, getLegacyFileSystemConnectorOptions("/test_path"), null),
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
        options.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);

        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new CatalogTableImpl(schema, options, null),
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

        hiveCatalog.createTable(hiveObjectPath, new CatalogTableImpl(schema, options, null), false);

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
                new CatalogTableImpl(TableSchema.builder().build(), properties, null),
                false);

        CatalogBaseTable catalogTable = hiveCatalog.getTable(hiveObjectPath);
        assertThat(catalogTable.getSchema()).isEqualTo(TableSchema.builder().build());
    }

    private static Map<String, String> getLegacyFileSystemConnectorOptions(String path) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector.type", "filesystem");
        options.put("connector.path", path);

        return options;
    }
}
