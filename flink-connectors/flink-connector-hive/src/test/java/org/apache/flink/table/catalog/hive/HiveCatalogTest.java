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

import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for HiveCatalog. */
public class HiveCatalogTest {

    TableSchema schema =
            TableSchema.builder()
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .build();
    private static HiveCatalog hiveCatalog;

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

    @Test
    public void testCreateGenericTable() {
        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new CatalogTableImpl(
                                schema, getLegacyFileSystemConnectorOptions("/test_path"), null),
                        HiveTestUtils.createHiveConf());

        Map<String, String> prop = hiveTable.getParameters();
        assertFalse(HiveCatalog.isHiveTable(prop));
        assertTrue(
                prop.keySet().stream()
                        .allMatch(k -> k.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX)));
    }

    @Test
    public void testCreateHiveTable() {
        Map<String, String> options = getLegacyFileSystemConnectorOptions("/test_path");
        options.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);

        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new CatalogTableImpl(schema, options, null),
                        HiveTestUtils.createHiveConf());

        Map<String, String> prop = hiveTable.getParameters();
        assertTrue(HiveCatalog.isHiveTable(prop));
        assertTrue(
                prop.keySet().stream()
                        .noneMatch(k -> k.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX)));
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
        assertEquals(hiveTable.getOptions().get("url"), "jdbc:clickhouse://host:port/testUrl1");
        assertEquals(
                hiveTable.getOptions().get("flink.url"), "jdbc:clickhouse://host:port/testUrl2");
    }

    @Test
    public void testCreateHiveConf() {
        // hive-conf-dir not specified, should read hive-site from classpath
        HiveConf hiveConf = HiveCatalog.createHiveConf(null, null);
        assertEquals("common-val", hiveConf.get("common-key"));
        // hive-conf-dir specified, shouldn't read hive-site from classpath
        String hiveConfDir =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-catalog-factory-conf")
                        .getPath();
        hiveConf = HiveCatalog.createHiveConf(hiveConfDir, null);
        assertNull(hiveConf.get("common-key", null));
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
        assertEquals(TableSchema.builder().build(), catalogTable.getSchema());
    }

    private static Map<String, String> getLegacyFileSystemConnectorOptions(String path) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector.type", "filesystem");
        options.put("connector.path", path);

        return options;
    }
}
