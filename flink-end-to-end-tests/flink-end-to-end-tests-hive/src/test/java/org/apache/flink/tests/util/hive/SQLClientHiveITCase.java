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

package org.apache.flink.tests.util.hive;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.ExecutionContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** End-to-end test for the hive connectors. */
public class SQLClientHiveITCase extends TestLogger {

    public static final String CATALOGS_ENVIRONMENT_FILE = "test-sql-client-catalogs.yaml";

    private SessionContext sessionContext;

    @Test
    @Category(FailsOnJava11.class)
    public void testCatalogs() throws Exception {
        final String inmemoryCatalog = "inmemorycatalog";
        final String hiveCatalog = "hivecatalog";
        final String hiveDefaultVersionCatalog = "hivedefaultversion";

        final ExecutionContext context = createCatalogExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        assertEquals(inmemoryCatalog, tableEnv.getCurrentCatalog());
        assertEquals("mydatabase", tableEnv.getCurrentDatabase());

        Catalog catalog = tableEnv.getCatalog(hiveCatalog).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof HiveCatalog);
        assertEquals("2.3.4", ((HiveCatalog) catalog).getHiveVersion());

        catalog = tableEnv.getCatalog(hiveDefaultVersionCatalog).orElse(null);
        assertNotNull(catalog);
        assertTrue(catalog instanceof HiveCatalog);
        // make sure we have assigned a default hive version
        assertFalse(StringUtils.isNullOrWhitespaceOnly(((HiveCatalog) catalog).getHiveVersion()));

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(hiveCatalog, tableEnv.getCurrentCatalog());

        Set<String> allCatalogs = new HashSet<>(Arrays.asList(tableEnv.listCatalogs()));
        assertEquals(6, allCatalogs.size());
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "default_catalog",
                                inmemoryCatalog,
                                hiveCatalog,
                                hiveDefaultVersionCatalog,
                                "catalog1",
                                "catalog2")),
                allCatalogs);

        sessionContext.close();
    }

    @Test
    @Category(FailsOnJava11.class)
    public void testDatabases() throws Exception {
        final String hiveCatalog = "hivecatalog";

        final ExecutionContext context = createCatalogExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();

        assertEquals(1, tableEnv.listDatabases().length);
        assertEquals("mydatabase", tableEnv.listDatabases()[0]);

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(2, tableEnv.listDatabases().length);
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                HiveCatalog.DEFAULT_DB,
                                TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE)),
                new HashSet<>(Arrays.asList(tableEnv.listDatabases())));

        tableEnv.useCatalog(hiveCatalog);

        assertEquals(HiveCatalog.DEFAULT_DB, tableEnv.getCurrentDatabase());

        tableEnv.useDatabase(TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE);

        assertEquals(
                TestHiveCatalogFactory.ADDITIONAL_TEST_DATABASE, tableEnv.getCurrentDatabase());

        sessionContext.close();
    }

    private ExecutionContext createCatalogExecutionContext() throws Exception {
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        return createExecutionContext(CATALOGS_ENVIRONMENT_FILE, replaceVars);
    }

    private ExecutionContext createExecutionContext(String file, Map<String, String> replaceVars)
            throws Exception {
        final Environment env = parseModified(file, replaceVars);
        return createExecutionContext(env);
    }

    private Environment parseModified(String fileName, Map<String, String> replaceVars)
            throws IOException {
        final URL url = this.getClass().getResource(fileName);
        Objects.requireNonNull(url);
        String schema = FileUtils.readFileUtf8(new File(url.getFile()));

        for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
            schema = schema.replace(replaceVar.getKey(), replaceVar.getValue());
        }

        return Environment.parse(schema);
    }

    private ExecutionContext createExecutionContext(Environment env) throws Exception {
        final Configuration configuration = new Configuration();
        DefaultContext defaultContext =
                new DefaultContext(
                        env,
                        new ArrayList<>(),
                        configuration,
                        Collections.singletonList(new DefaultCLI()));
        sessionContext = SessionContext.create(defaultContext, "test-session");
        return sessionContext.getExecutionContext();
    }
}
