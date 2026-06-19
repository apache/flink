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

package org.apache.flink.table.file.testutils.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TestFileSystemCatalog} created by {@link TestFileSystemCatalogFactory}. */
public class TestFileSystemCatalogFactoryTest {

    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_DEFAULT_DATABASE = "test_db";

    @TempDir File tempFile;

    @BeforeEach
    void before() {
        File testDb = new File(tempFile, TEST_DEFAULT_DATABASE);
        testDb.mkdir();
    }

    @Test
    public void testCreateCatalogWithDDL() {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String catalogDDL =
                String.format(
                        "CREATE CATALOG %s\n"
                                + "WITH (\n"
                                + "  'type' = 'test-filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'default-database' = '%s'\n"
                                + "  )",
                        TEST_CATALOG, tempFile.getAbsolutePath(), TEST_DEFAULT_DATABASE);

        tEnv.executeSql(catalogDDL);

        List<Row> result =
                CollectionUtil.iteratorToList(tEnv.executeSql("SHOW CATALOGS").collect());
        assertThat(result.contains(Row.of(TEST_CATALOG))).isTrue();
    }

    @Test
    public void testCreateCatalogWithFactory() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("type", "test-filesystem");
        properties.put("default-database", TEST_DEFAULT_DATABASE);
        properties.put("path", tempFile.getAbsolutePath());
        final TestFileSystemCatalog actualCatalog =
                (TestFileSystemCatalog)
                        FactoryUtil.createCatalog(
                                TEST_CATALOG,
                                properties,
                                new Configuration(),
                                TestFileSystemCatalogFactoryTest.class.getClassLoader());

        assertThat(actualCatalog.getCatalogPathStr()).isEqualTo(tempFile.getAbsolutePath());
        assertThat(actualCatalog.getName()).isEqualTo(TEST_CATALOG);
        assertThat(actualCatalog.getDefaultDatabase()).isEqualTo(TEST_DEFAULT_DATABASE);
    }
}
