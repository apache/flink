/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ExecutionEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.SimpleCatalogFactory;
import org.apache.flink.table.client.gateway.utils.TestUserClassLoaderJar;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.ModuleEntry;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.table.client.gateway.local.ExecutionContextTest.MODULES_ENVIRONMENT_FILE;
import static org.apache.flink.table.client.gateway.local.ExecutionContextTest.createModuleReplaceVars;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Contains basic tests for the {@link LocalExecutor}. */
@RunWith(Parameterized.class)
public class LocalExecutorITCase extends TestLogger {

    @Parameters(name = "Planner: {0}")
    public static List<String> planner() {
        return Arrays.asList(
                ExecutionEntry.EXECUTION_PLANNER_VALUE_OLD,
                ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);
    }

    private static final String DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml";
    private static final String DIALECT_ENVIRONMENT_FILE = "test-sql-client-dialect.yaml";

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfig())
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                            .build());

    private static ClusterClient<?> clusterClient;

    // a generated UDF jar used for testing classloading of dependencies
    private static URL udfDependency;

    @BeforeClass
    public static void setup() throws IOException {
        clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient();
        File udfJar =
                TestUserClassLoaderJar.createJarFile(
                        tempFolder.newFolder("test-jar"), "test-classloader-udf.jar");
        udfDependency = udfJar.toURI().toURL();
    }

    private static Configuration getConfig() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        return config;
    }

    @Parameter public String planner;

    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testCompleteStatement() throws Exception {
        final Executor executor = createDefaultExecutor(clusterClient);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        final List<String> expectedTableHints =
                Arrays.asList(
                        "default_catalog.default_database.TableNumber1",
                        "default_catalog.default_database.TableNumber2",
                        "default_catalog.default_database.TableSourceSink");
        assertEquals(
                expectedTableHints, executor.completeStatement(sessionId, "SELECT * FROM Ta", 16));

        final List<String> expectedClause = Collections.singletonList("WHERE");
        assertEquals(
                expectedClause,
                executor.completeStatement(sessionId, "SELECT * FROM TableNumber2 WH", 29));

        final List<String> expectedField = Arrays.asList("IntegerField1");
        assertEquals(
                expectedField,
                executor.completeStatement(sessionId, "SELECT * FROM TableNumber1 WHERE Inte", 37));
        executor.closeSession(sessionId);
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionChangelog() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        try {
            // start job and retrieval
            final ResultDescriptor desc =
                    executor.executeQuery(
                            sessionId,
                            "SELECT scalarUDF(IntegerField1), StringField1, 'ABC' FROM TableNumber1");

            assertFalse(desc.isMaterialized());

            final List<String> actualResults =
                    retrieveChangelogResult(executor, sessionId, desc.getResultId());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("+I[47, Hello World, ABC]");
            expectedResults.add("+I[27, Hello World, ABC]");
            expectedResults.add("+I[37, Hello World, ABC]");
            expectedResults.add("+I[37, Hello World, ABC]");
            expectedResults.add("+I[47, Hello World, ABC]");
            expectedResults.add("+I[57, Hello World!!!!, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionChangelogMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "changelog");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[27, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[57, Hello World!!!!]");

        try {
            for (int i = 0; i < 3; i++) {
                // start job and retrieval
                final ResultDescriptor desc =
                        executor.executeQuery(
                                sessionId,
                                "SELECT scalarUDF(IntegerField1), StringField1 FROM TableNumber1");

                assertFalse(desc.isMaterialized());

                final List<String> actualResults =
                        retrieveChangelogResult(executor, sessionId, desc.getResultId());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final String query =
                "SELECT scalarUDF(IntegerField1), StringField1, 'ABC' FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World, ABC]");
        expectedResults.add("+I[27, Hello World, ABC]");
        expectedResults.add("+I[37, Hello World, ABC]");
        expectedResults.add("+I[37, Hello World, ABC]");
        expectedResults.add("+I[47, Hello World, ABC]");
        expectedResults.add("+I[57, Hello World!!!!, ABC]");

        executeStreamQueryTable(replaceVars, query, expectedResults);
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionTableMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final String query = "SELECT scalarUDF(IntegerField1), StringField1 FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[27, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[57, Hello World!!!!]");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        try {
            for (int i = 0; i < 3; i++) {
                executeStreamQueryTable(replaceVars, query, expectedResults);
            }
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionLimitedTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "1");

        final String query =
                "SELECT COUNT(*), StringField1 FROM TableNumber1 GROUP BY StringField1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[1, Hello World!!!!]");

        executeStreamQueryTable(replaceVars, query, expectedResults);
    }

    @Test(timeout = 90_000L)
    public void testBatchQueryExecution() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        try {
            final ResultDescriptor desc =
                    executor.executeQuery(sessionId, "SELECT *, 'ABC' FROM TestView1");

            assertTrue(desc.isMaterialized());

            final List<String> actualResults =
                    retrieveTableResult(executor, sessionId, desc.getResultId());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("+I[47, ABC]");
            expectedResults.add("+I[27, ABC]");
            expectedResults.add("+I[37, ABC]");
            expectedResults.add("+I[37, ABC]");
            expectedResults.add("+I[47, ABC]");
            expectedResults.add("+I[57, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test(timeout = 90_000L)
    public void testBatchQueryExecutionMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
        replaceVars.put("$VAR_RESULT_MODE", "table");
        replaceVars.put("$VAR_UPDATE_MODE", "");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47]");
        expectedResults.add("+I[27]");
        expectedResults.add("+I[37]");
        expectedResults.add("+I[37]");
        expectedResults.add("+I[47]");
        expectedResults.add("+I[57]");

        try {
            for (int i = 0; i < 3; i++) {
                final ResultDescriptor desc =
                        executor.executeQuery(sessionId, "SELECT * FROM TestView1");

                assertTrue(desc.isMaterialized());

                final List<String> actualResults =
                        retrieveTableResult(executor, sessionId, desc.getResultId());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        } finally {
            executor.closeSession(sessionId);
        }
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionSink() throws Exception {
        final String csvOutputPath =
                new File(tempFolder.newFolder().getAbsolutePath(), "test-out.csv")
                        .toURI()
                        .toString();
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_SOURCE_SINK_PATH", csvOutputPath);
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        try {
            executor.executeSql(sessionId, "CREATE FUNCTION LowerUDF AS 'LowerUDF'");
            // Case 1: Registered sink
            // Case 1.1: Registered sink with uppercase insert into keyword.
            // FLINK-18302: wrong classloader when INSERT INTO with UDF
            final String statement1 =
                    "INSERT INTO TableSourceSink SELECT IntegerField1 = 42,"
                            + " LowerUDF(StringField1), TimestampField1 FROM TableNumber1";
            executeAndVerifySinkResult(executor, sessionId, statement1, csvOutputPath);
            // Case 1.2: Registered sink with lowercase insert into keyword.
            final String statement2 =
                    "insert Into TableSourceSink \n "
                            + "SELECT IntegerField1 = 42, LowerUDF(StringField1), TimestampField1 "
                            + "FROM TableNumber1";
            executeAndVerifySinkResult(executor, sessionId, statement2, csvOutputPath);
            // Case 1.3: Execute the same statement again, the results should expect to be the same.
            executeAndVerifySinkResult(executor, sessionId, statement2, csvOutputPath);

            // Case 2: Temporary sink
            executor.executeSql(sessionId, "use catalog `simple-catalog`");
            executor.executeSql(sessionId, "use default_database");
            // create temporary sink
            executor.executeSql(
                    sessionId,
                    "CREATE TEMPORARY TABLE MySink (id int, str VARCHAR) WITH ('connector' = 'COLLECTION')");
            final String statement3 = "INSERT INTO MySink select * from `test-table`";

            // all queries are pipelined to an in-memory sink, check it is properly registered
            final ResultDescriptor otherCatalogDesc =
                    executor.executeQuery(sessionId, "SELECT * FROM `test-table`");

            final List<String> otherCatalogResults =
                    retrieveTableResult(executor, sessionId, otherCatalogDesc.getResultId());

            TestBaseUtils.compareResultCollections(
                    SimpleCatalogFactory.TABLE_CONTENTS.stream()
                            .map(Row::toString)
                            .collect(Collectors.toList()),
                    otherCatalogResults,
                    Comparator.naturalOrder());
        } finally {
            executor.closeSession(sessionId);
            TestCollectionTableFactory.reset();
        }
    }

    @Test
    public void testLoadModuleWithModuleConfEnabled() throws Exception {
        // only blink planner supports LOAD MODULE syntax
        Assume.assumeTrue(planner.equals("blink"));
        final LocalExecutor executor =
                createModifiedExecutor(
                        MODULES_ENVIRONMENT_FILE, clusterClient, createModuleReplaceVars());
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        assertThrows(
                "Could not execute statement: load module core",
                SqlExecutionException.class,
                () -> executor.executeSql(sessionId, "load module core"));

        executor.executeSql(sessionId, "load module hive");
        assertEquals(
                executor.listModules(sessionId),
                Arrays.asList("core", "mymodule", "myhive", "myhive2", "hive"));
    }

    @Test
    public void testUnloadModuleWithModuleConfEnabled() throws Exception {
        // only blink planner supports UNLOAD MODULE syntax
        Assume.assumeTrue(planner.equals("blink"));
        final LocalExecutor executor =
                createModifiedExecutor(
                        MODULES_ENVIRONMENT_FILE, clusterClient, createModuleReplaceVars());
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        executor.executeSql(sessionId, "unload module mymodule");
        assertEquals(executor.listModules(sessionId), Arrays.asList("core", "myhive", "myhive2"));

        exception.expect(SqlExecutionException.class);
        exception.expectMessage("Could not execute statement: unload module mymodule");
        executor.executeSql(sessionId, "unload module mymodule");
    }

    @Test
    public void testHiveBuiltInFunctionWithHiveModuleEnabled() throws Exception {
        // only blink planner supports LOAD MODULE syntax
        Assume.assumeTrue(planner.equals("blink"));

        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESULT_MODE", "table");

        final LocalExecutor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        // cannot use hive built-in function without loading hive module
        assertThrows(
                "Could not execute statement: select substring_index('www.apache.org', '.', 2) from TableNumber1",
                SqlExecutionException.class,
                () ->
                        executor.executeSql(
                                sessionId,
                                "select substring_index('www.apache.org', '.', 2) from TableNumber1"));

        executor.executeSql(sessionId, "load module hive");
        assertEquals(executor.listModules(sessionId), Arrays.asList("core", "hive"));

        assertShowResult(
                executor.executeSql(
                        sessionId,
                        "select substring_index('www.apache.org', '.', 2) from TableNumber1"),
                hasItems("www.apache"));
    }

    @Test
    public void testUseModulesWithModuleConfEnabled() throws Exception {
        // only blink planner supports USE MODULES syntax
        Assume.assumeTrue(planner.equals("blink"));
        final LocalExecutor executor =
                createModifiedExecutor(
                        MODULES_ENVIRONMENT_FILE, clusterClient, createModuleReplaceVars());
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);
        assertEquals(
                executor.listModules(sessionId),
                Arrays.asList("core", "mymodule", "myhive", "myhive2"));
        assertEquals(
                executor.listFullModules(sessionId),
                Arrays.asList(
                        new ModuleEntry("core", true),
                        new ModuleEntry("mymodule", true),
                        new ModuleEntry("myhive", true),
                        new ModuleEntry("myhive2", true)));

        // change resolution order
        executor.executeSql(sessionId, "use modules myhive2, core, mymodule, myhive");
        assertEquals(
                executor.listModules(sessionId),
                Arrays.asList("myhive2", "core", "mymodule", "myhive"));
        assertEquals(
                executor.listFullModules(sessionId),
                Arrays.asList(
                        new ModuleEntry("myhive2", true),
                        new ModuleEntry("core", true),
                        new ModuleEntry("mymodule", true),
                        new ModuleEntry("myhive", true)));

        // disable modules by not using
        executor.executeSql(sessionId, "use modules core, myhive");
        assertEquals(executor.listModules(sessionId), Arrays.asList("core", "myhive"));
        assertEquals(
                executor.listFullModules(sessionId),
                Arrays.asList(
                        new ModuleEntry("core", true),
                        new ModuleEntry("myhive", true),
                        new ModuleEntry("mymodule", false),
                        new ModuleEntry("myhive2", false)));

        // use duplicate module names
        assertThrows(
                "Could not execute statement: use modules core, myhive, core",
                SqlExecutionException.class,
                () -> executor.executeSql(sessionId, "use modules core, myhive, core"));

        // use non-existed module name
        assertThrows(
                "Could not execute statement: use modules core, dummy",
                SqlExecutionException.class,
                () -> executor.executeSql(sessionId, "use modules core, dummy"));
    }

    @Test
    public void testHiveBuiltInFunctionWithoutUsingHiveModule() throws Exception {
        // only blink planner supports USE MODULES syntax
        Assume.assumeTrue(planner.equals("blink"));

        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
        replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESULT_MODE", "table");

        final LocalExecutor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        executor.executeSql(sessionId, "load module hive");
        assertEquals(executor.listModules(sessionId), Arrays.asList("core", "hive"));
        assertEquals(
                executor.listFullModules(sessionId),
                Arrays.asList(new ModuleEntry("core", true), new ModuleEntry("hive", true)));

        assertShowResult(
                executor.executeSql(
                        sessionId,
                        "select substring_index('www.apache.org', '.', 2) from TableNumber1"),
                hasItems("www.apache"));

        // cannot use hive built-in function without using hive module
        executor.executeSql(sessionId, "use modules core");
        assertEquals(executor.listModules(sessionId), Collections.singletonList("core"));
        assertEquals(
                executor.listFullModules(sessionId),
                Arrays.asList(new ModuleEntry("core", true), new ModuleEntry("hive", false)));
        assertThrows(
                "Could not execute statement: select substring_index('www.apache.org', '.', 2) from TableNumber1",
                SqlExecutionException.class,
                () ->
                        executor.executeSql(
                                sessionId,
                                "select substring_index('www.apache.org', '.', 2) from TableNumber1"));
    }

    @Test
    public void testShowModules() throws Exception {
        // only blink planner supports SHOW [FULL] MODULES syntax
        Assume.assumeTrue(planner.equals("blink"));

        final LocalExecutor executor =
                createModifiedExecutor(
                        MODULES_ENVIRONMENT_FILE, clusterClient, createModuleReplaceVars());
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        verifyShowModules(
                executor,
                sessionId,
                Arrays.asList(
                        Row.of("core", true),
                        Row.of("mymodule", true),
                        Row.of("myhive", true),
                        Row.of("myhive2", true)));

        // check result after using modules
        executor.executeSql(sessionId, "USE MODULES mymodule, core");
        verifyShowModules(
                executor,
                sessionId,
                Arrays.asList(
                        Row.of("mymodule", true),
                        Row.of("core", true),
                        Row.of("myhive", false),
                        Row.of("myhive2", false)));

        // check result after unloading modules
        executor.executeSql(sessionId, "UNLOAD MODULE mymodule");
        executor.executeSql(sessionId, "UNLOAD MODULE myhive2");
        verifyShowModules(
                executor, sessionId, Arrays.asList(Row.of("core", true), Row.of("myhive", false)));
    }

    private void executeStreamQueryTable(
            Map<String, String> replaceVars, String query, List<String> expectedResults)
            throws Exception {

        final Executor executor = createModifiedExecutor(clusterClient, replaceVars);
        final SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);
        assertEquals("test-session", sessionId);

        try {
            // start job and retrieval
            final ResultDescriptor desc = executor.executeQuery(sessionId, query);

            assertTrue(desc.isMaterialized());

            final List<String> actualResults =
                    retrieveTableResult(executor, sessionId, desc.getResultId());

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession(sessionId);
        }
    }

    private void assertShowResult(TableResult showResult, Matcher<Iterable<String>> matcher) {
        List<String> actual =
                CollectionUtil.iteratorToList(showResult.collect()).stream()
                        .map(r -> checkNotNull(r.getField(0)).toString())
                        .collect(Collectors.toList());
        assertThat(actual, matcher);
    }

    private void verifySinkResult(String path) throws IOException {
        final List<String> actualResults = new ArrayList<>();
        TestBaseUtils.readAllResultLines(actualResults, path);
        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("true,hello world,2020-01-01 00:00:01.0");
        expectedResults.add("false,hello world,2020-01-01 00:00:02.0");
        expectedResults.add("false,hello world,2020-01-01 00:00:03.0");
        expectedResults.add("false,hello world,2020-01-01 00:00:04.0");
        expectedResults.add("true,hello world,2020-01-01 00:00:05.0");
        expectedResults.add("false,hello world!!!!,2020-01-01 00:00:06.0");
        TestBaseUtils.compareResultCollections(
                expectedResults, actualResults, Comparator.naturalOrder());
    }

    private void executeAndVerifySinkResult(
            Executor executor, String sessionId, String statement, String resultPath)
            throws Exception {
        final TableResult tableResult = executor.executeSql(sessionId, statement);
        checkState(tableResult.getJobClient().isPresent());
        // wait for job completion
        tableResult.await();
        // verify result
        verifySinkResult(resultPath);
    }

    private <T> LocalExecutor createDefaultExecutor(ClusterClient<T> clusterClient)
            throws Exception {
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_PLANNER", planner);
        replaceVars.put("$VAR_EXECUTION_TYPE", "batch");
        replaceVars.put("$VAR_UPDATE_MODE", "");
        replaceVars.put("$VAR_MAX_ROWS", "100");
        replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
        return new LocalExecutor(
                EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
                Collections.emptyList(),
                clusterClient.getFlinkConfiguration(),
                new DefaultCLI(),
                new DefaultClusterClientServiceLoader());
    }

    private <T> LocalExecutor createModifiedExecutor(
            ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
        replaceVars.putIfAbsent("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
        return new LocalExecutor(
                EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
                Collections.singletonList(udfDependency),
                clusterClient.getFlinkConfiguration(),
                new DefaultCLI(),
                new DefaultClusterClientServiceLoader());
    }

    private <T> LocalExecutor createModifiedExecutor(
            String yamlFile, ClusterClient<T> clusterClient, Map<String, String> replaceVars)
            throws Exception {
        replaceVars.putIfAbsent("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
        return new LocalExecutor(
                EnvironmentFileUtil.parseModified(yamlFile, replaceVars),
                Collections.singletonList(udfDependency),
                clusterClient.getFlinkConfiguration(),
                new DefaultCLI(),
                new DefaultClusterClientServiceLoader());
    }

    private List<String> retrieveTableResult(Executor executor, String sessionId, String resultID)
            throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<Integer> result = executor.snapshotResult(sessionId, resultID, 2);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                actualResults.clear();
                IntStream.rangeClosed(1, result.getPayload())
                        .forEach(
                                (page) -> {
                                    for (Row row : executor.retrieveResultPage(resultID, page)) {
                                        actualResults.add(row.toString());
                                    }
                                });
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }

        return actualResults;
    }

    private List<String> retrieveChangelogResult(
            Executor executor, String sessionId, String resultID) throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<List<Row>> result =
                    executor.retrieveResultChanges(sessionId, resultID);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                for (Row row : result.getPayload()) {
                    actualResults.add(row.toString());
                }
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }
        return actualResults;
    }

    private static TableSchema getShowModulesTableSchema(boolean requireFull) {
        return TableSchema.builder()
                .fields(
                        requireFull
                                ? new String[] {"module name", "used"}
                                : new String[] {"module name"},
                        requireFull
                                ? new DataType[] {DataTypes.STRING(), DataTypes.BOOLEAN()}
                                : new DataType[] {DataTypes.STRING()})
                .build();
    }

    private void verifyShowModules(
            LocalExecutor executor, String sessionId, List<Row> rowOfEntries) {
        TableSchema showModulesTableSchema = getShowModulesTableSchema(false);
        TableSchema showFullModulesTableSchema = getShowModulesTableSchema(true);
        List<Row> rowOfNames =
                rowOfEntries.stream()
                        .filter(row -> row.getFieldAs(1))
                        .map(row -> Row.project(row, new int[] {0}))
                        .collect(Collectors.toList());

        TableResult showModules = executor.executeSql(sessionId, "SHOW MODULES");
        TableResult showFullModules = executor.executeSql(sessionId, "SHOW FULL MODULES");

        assertEquals(showModulesTableSchema, showModules.getTableSchema());
        assertEquals(showFullModulesTableSchema, showFullModules.getTableSchema());

        assertEquals(rowOfNames, CollectionUtil.iteratorToList(showModules.collect()));
        assertEquals(rowOfEntries, CollectionUtil.iteratorToList(showFullModules.collect()));
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    /** Scala Function for test. */
    public static class TestScalaFunction extends ScalarFunction {
        public long eval(int i, long l, String s) {
            return i + l + s.length();
        }
    }
}
