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
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
                                schema, new FileSystem().path("/test_path").toProperties(), null),
                        HiveTestUtils.createHiveConf());

        Map<String, String> prop = hiveTable.getParameters();
        assertFalse(HiveCatalog.isHiveTable(prop));
        assertTrue(
                prop.keySet().stream()
                        .allMatch(k -> k.startsWith(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX)));
    }

    @Test
    public void testCreateHiveTable() {
        Map<String, String> map = new HashMap<>(new FileSystem().path("/test_path").toProperties());

        map.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);

        Table hiveTable =
                HiveTableUtil.instantiateHiveTable(
                        new ObjectPath("test", "test"),
                        new CatalogTableImpl(schema, map, null),
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

        Map<String, String> properties =
                new HashMap<>(new FileSystem().path("/test_path").toProperties());

        properties.put(CONNECTOR.key(), "jdbc");
        properties.put("url", "jdbc:clickhouse://host:port/testUrl1");
        properties.put("flink.url", "jdbc:clickhouse://host:port/testUrl2");

        hiveCatalog.createTable(
                hiveObjectPath, new CatalogTableImpl(schema, properties, null), false);

        CatalogBaseTable hiveTable = hiveCatalog.getTable(hiveObjectPath);
        assertEquals(hiveTable.getOptions().get("url"), "jdbc:clickhouse://host:port/testUrl1");
        assertEquals(
                hiveTable.getOptions().get("flink.url"), "jdbc:clickhouse://host:port/testUrl2");
    }
}
