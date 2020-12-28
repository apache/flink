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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URLClassLoader;

import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** IT Case for catalog ddl. */
public class CatalogITCase {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateCatalog() {
        String name = "c1";
        TableEnvironment tableEnv = getTableEnvironment();
        String ddl =
                String.format(
                        "create catalog %s with('type'='%s')",
                        name, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);

        tableEnv.executeSql(ddl);

        assertTrue(tableEnv.getCatalog(name).isPresent());
        assertTrue(tableEnv.getCatalog(name).get() instanceof GenericInMemoryCatalog);
    }

    @Test
    public void testDropCatalog() {
        String name = "c1";
        TableEnvironment tableEnv = getTableEnvironment();

        String ddl =
                String.format(
                        "create catalog %s with('type'='%s')",
                        name, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);
        tableEnv.executeSql(ddl);
        assertTrue(tableEnv.getCatalog(name).isPresent());

        ddl = String.format("drop catalog %s", name);
        tableEnv.executeSql(ddl);
        assertFalse(tableEnv.getCatalog(name).isPresent());
    }

    @Test
    public void testCreateCatalogFromUserClassLoader() throws Exception {
        final String className = "UserCatalogFactory";
        URLClassLoader classLoader =
                ClassLoaderUtils.withRoot(temporaryFolder.newFolder())
                        .addResource(
                                "META-INF/services/org.apache.flink.table.factories.TableFactory",
                                "UserCatalogFactory")
                        .addClass(
                                className,
                                "import org.apache.flink.table.catalog.GenericInMemoryCatalog;\n"
                                        + "import org.apache.flink.table.factories.CatalogFactory;\n"
                                        + "import java.util.Collections;\n"
                                        + "import org.apache.flink.table.catalog.Catalog;\n"
                                        + "import java.util.HashMap;\n"
                                        + "import java.util.List;\n"
                                        + "import java.util.Map;\n"
                                        + "\tpublic class UserCatalogFactory implements CatalogFactory {\n"
                                        + "\t\t@Override\n"
                                        + "\t\tpublic Catalog createCatalog(\n"
                                        + "\t\t\t\tString name,\n"
                                        + "\t\t\t\tMap<String, String> properties) {\n"
                                        + "\t\t\treturn new GenericInMemoryCatalog(name);\n"
                                        + "\t\t}\n"
                                        + "\n"
                                        + "\t\t@Override\n"
                                        + "\t\tpublic Map<String, String> requiredContext() {\n"
                                        + "\t\t\tHashMap<String, String> hashMap = new HashMap<>();\n"
                                        + "\t\t\thashMap.put(\"type\", \"userCatalog\");\n"
                                        + "\t\t\treturn hashMap;\n"
                                        + "\t\t}\n"
                                        + "\n"
                                        + "\t\t@Override\n"
                                        + "\t\tpublic List<String> supportedProperties() {\n"
                                        + "\t\t\treturn Collections.emptyList();\n"
                                        + "\t\t}\n"
                                        + "\t}")
                        .build();

        try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
            TableEnvironment tableEnvironment = getTableEnvironment();
            tableEnvironment.executeSql("CREATE CATALOG cat WITH ('type'='userCatalog')");

            assertTrue(tableEnvironment.getCatalog("cat").isPresent());
        }
    }

    private TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(env, settings);
    }
}
