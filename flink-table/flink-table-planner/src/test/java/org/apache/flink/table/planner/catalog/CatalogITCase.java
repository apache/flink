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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactoryOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                        name, GenericInMemoryCatalogFactoryOptions.IDENTIFIER);

        tableEnv.executeSql(ddl);

        assertThat(tableEnv.getCatalog(name)).isPresent();
        assertThat(tableEnv.getCatalog(name)).containsInstanceOf(GenericInMemoryCatalog.class);
    }

    @Test
    public void testDropCatalog() {
        String name = "c1";
        TableEnvironment tableEnv = getTableEnvironment();

        String createDdl =
                String.format(
                        "create catalog %s with('type'='%s')",
                        name, GenericInMemoryCatalogFactoryOptions.IDENTIFIER);
        tableEnv.executeSql(createDdl);
        assertThat(tableEnv.getCatalog(name)).isPresent();

        String dropDdl = String.format("drop catalog %s", name);
        tableEnv.executeSql(String.format("use catalog %s", name));
        assertThatThrownBy(() -> tableEnv.executeSql(dropDdl))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseExactlyInstanceOf(CatalogException.class)
                .hasRootCauseMessage("Cannot drop a catalog which is currently in use.");
        assertThat(tableEnv.getCatalog(name)).isPresent();

        tableEnv.executeSql("use catalog default_catalog");
        tableEnv.executeSql(dropDdl);
        assertThat(tableEnv.getCatalog(name)).isNotPresent();
    }

    @Test
    public void testCreateLegacyCatalogFromUserClassLoader() throws Exception {
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

            assertThat(tableEnvironment.getCatalog("cat")).isPresent();
        }
    }

    @Test
    public void testCreateCatalogFromUserClassLoader() throws Exception {
        final String className = "UserCatalogFactory";
        URLClassLoader classLoader =
                ClassLoaderUtils.withRoot(temporaryFolder.newFolder())
                        .addResource(
                                "META-INF/services/org.apache.flink.table.factories.Factory",
                                "UserCatalogFactory")
                        .addClass(
                                className,
                                "import org.apache.flink.configuration.ConfigOption;\n"
                                        + "import org.apache.flink.table.catalog.Catalog;\n"
                                        + "import org.apache.flink.table.catalog.GenericInMemoryCatalog;\n"
                                        + "import org.apache.flink.table.factories.CatalogFactory;\n"
                                        + "\n"
                                        + "import java.util.Collections;\n"
                                        + "import java.util.Set;\n"
                                        + "\n"
                                        + "public class UserCatalogFactory implements CatalogFactory {\n"
                                        + "    @Override\n"
                                        + "    public Catalog createCatalog(Context context) {\n"
                                        + "        return new GenericInMemoryCatalog(context.getName());\n"
                                        + "    }\n"
                                        + "\n"
                                        + "    @Override\n"
                                        + "    public String factoryIdentifier() {\n"
                                        + "        return \"userCatalog\";\n"
                                        + "    }\n"
                                        + "\n"
                                        + "    @Override\n"
                                        + "    public Set<ConfigOption<?>> requiredOptions() {\n"
                                        + "        return Collections.emptySet();\n"
                                        + "    }\n"
                                        + "\n"
                                        + "    @Override\n"
                                        + "    public Set<ConfigOption<?>> optionalOptions() {\n"
                                        + "        return Collections.emptySet();\n"
                                        + "    }\n"
                                        + "}")
                        .build();

        try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
            TableEnvironment tableEnvironment = getTableEnvironment();
            tableEnvironment.executeSql("CREATE CATALOG cat WITH ('type'='userCatalog')");

            assertThat(tableEnvironment.getCatalog("cat")).isPresent();
        }
    }

    @Test
    public void testGetTablesFromGivenCatalogDatabase() throws Exception {
        final Catalog c1 = new GenericInMemoryCatalog("c1", "default");
        final Catalog c2 = new GenericInMemoryCatalog("c2", "d2");

        final CatalogManager catalogManager =
                CatalogManagerMocks.preparedCatalogManager().defaultCatalog("c2", c2).build();
        catalogManager.registerCatalog("c1", c1);

        final CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().build(), null, new ArrayList<>(), new HashMap<>());

        c1.createDatabase("d1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        c1.createTable(new ObjectPath("d1", "t1"), catalogTable, true);

        c2.createTable(
                new ObjectPath(catalogManager.getCurrentDatabase(), "t2"), catalogTable, true);

        assertThat(catalogManager.getCurrentCatalog()).isEqualTo("c2");
        assertThat(catalogManager.getCurrentDatabase()).isEqualTo("d2");

        assertThat(catalogManager.listTables()).containsExactlyInAnyOrder("t2");
        assertThat(catalogManager.listTables("c1", "d1")).containsExactlyInAnyOrder("t1");
    }

    private TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(env, settings);
    }
}
