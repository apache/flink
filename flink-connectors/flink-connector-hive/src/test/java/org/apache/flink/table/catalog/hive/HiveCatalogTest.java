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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;

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
                        HiveTestUtils.createHiveConf());

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
                        HiveTestUtils.createHiveConf());

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
