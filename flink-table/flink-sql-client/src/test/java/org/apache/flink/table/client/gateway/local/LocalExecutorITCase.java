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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.utils.UserDefinedFunctions;
import org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.TableUDF;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.utils.TestUserClassLoaderJar;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

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
import java.util.stream.Stream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.apache.flink.table.client.gateway.utils.UserDefinedFunctions.ScalarUDF;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Contains basic tests for the {@link LocalExecutor}. */
public class LocalExecutorITCase extends TestLogger {

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
                        tempFolder.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        UserDefinedFunctions.GENERATED_UDF_CLASS,
                        UserDefinedFunctions.GENERATED_UDF_CODE);
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

    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testCompleteStatement() {
        final Executor executor = createLocalExecutor();
        String sessionId = executor.openSession("test-session");
        assertEquals("test-session", sessionId);
        initSession(executor, sessionId, Collections.emptyMap());

        final List<String> expectedTableHints =
                Arrays.asList(
                        "default_catalog.default_database.TableNumber1",
                        "default_catalog.default_database.TableSourceSink");
        assertEquals(
                expectedTableHints, executor.completeStatement(sessionId, "SELECT * FROM Ta", 16));

        final List<String> expectedClause = Collections.singletonList("WHERE");
        assertEquals(
                expectedClause,
                executor.completeStatement(sessionId, "SELECT * FROM TableNumber1 WH", 29));

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
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        final LocalExecutor executor =
                createLocalExecutor(Collections.singletonList(udfDependency), configuration);
        String sessionId = executor.openSession("test-session");
        assertEquals("test-session", sessionId);

        initSession(executor, sessionId, replaceVars);
        try {
            // start job and retrieval
            final ResultDescriptor desc =
                    executeQuery(
                            executor,
                            sessionId,
                            "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1");

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
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        final LocalExecutor executor =
                createLocalExecutor(Collections.singletonList(udfDependency), configuration);
        String sessionId = executor.openSession("test-session");
        assertEquals("test-session", sessionId);

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[27, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[57, Hello World!!!!]");

        initSession(executor, sessionId, replaceVars);
        try {
            for (int i = 0; i < 3; i++) {
                // start job and retrieval
                final ResultDescriptor desc =
                        executeQuery(
                                executor,
                                sessionId,
                                "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1");

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
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Map<String, String> configMap = getDefaultSessionConfigMap();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query =
                "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World, ABC]");
        expectedResults.add("+I[27, Hello World, ABC]");
        expectedResults.add("+I[37, Hello World, ABC]");
        expectedResults.add("+I[37, Hello World, ABC]");
        expectedResults.add("+I[47, Hello World, ABC]");
        expectedResults.add("+I[57, Hello World!!!!, ABC]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionTableMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query = "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[27, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[37, Hello World]");
        expectedResults.add("+I[47, Hello World]");
        expectedResults.add("+I[57, Hello World!!!!]");

        for (int i = 0; i < 3; i++) {
            executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
        }
    }

    @Test(timeout = 90_000L)
    public void testStreamQueryExecutionLimitedTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(EXECUTION_MAX_TABLE_RESULT_ROWS.key(), "1");

        final String query =
                "SELECT COUNT(*), StringField1 FROM TableNumber1 GROUP BY StringField1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("+I[1, Hello World!!!!]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test(timeout = 90_000L)
    public void testBatchQueryExecution() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        String sessionId = executor.openSession("test-session");
        assertEquals("test-session", sessionId);

        initSession(executor, sessionId, replaceVars);
        try {
            final ResultDescriptor desc =
                    executeQuery(executor, sessionId, "SELECT *, 'ABC' FROM TestView1");

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
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        String sessionId = executor.openSession("test-session");
        assertEquals("test-session", sessionId);
        initSession(executor, sessionId, replaceVars);

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
                        executeQuery(executor, sessionId, "SELECT * FROM TestView1");

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

    // --------------------------------------------------------------------------------------------
    // Helper method
    // --------------------------------------------------------------------------------------------

    private TableResult executeSql(Executor executor, String sessionId, String sql) {
        Operation operation = executor.parseStatement(sessionId, sql);
        return executor.executeOperation(sessionId, operation);
    }

    private ResultDescriptor executeQuery(Executor executor, String sessionId, String query) {
        Operation operation = executor.parseStatement(sessionId, query);
        return executor.executeQuery(sessionId, (QueryOperation) operation);
    }

    private LocalExecutor createLocalExecutor() {
        return createLocalExecutor(Collections.emptyList(), new Configuration());
    }

    private LocalExecutor createLocalExecutor(List<URL> dependencies, Configuration configuration) {
        configuration.addAll(clusterClient.getFlinkConfiguration());
        DefaultContext defaultContext =
                new DefaultContext(
                        dependencies, configuration, Collections.singletonList(new DefaultCLI()));
        return new LocalExecutor(defaultContext);
    }

    private void initSession(Executor executor, String sessionId, Map<String, String> replaceVars) {
        for (String sql : getInitSQL(replaceVars)) {
            executor.executeOperation(sessionId, executor.parseStatement(sessionId, sql));
        }
    }

    private void executeStreamQueryTable(
            Map<String, String> replaceVars,
            Map<String, String> configMap,
            String query,
            List<String> expectedResults)
            throws Exception {

        final LocalExecutor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        String sessionId = executor.openSession("test-session");

        assertEquals("test-session", sessionId);

        initSession(executor, sessionId, replaceVars);

        try {
            // start job and retrieval
            final ResultDescriptor desc = executeQuery(executor, sessionId, query);

            assertTrue(desc.isMaterialized());

            final List<String> actualResults =
                    retrieveTableResult(executor, sessionId, desc.getResultId());

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession(sessionId);
        }
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

    private void verifySinkResult(String path) throws IOException {
        final List<String> actualResults = new ArrayList<>();
        TestBaseUtils.readAllResultLines(actualResults, path);
        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("true,\"hello world\",\"2020-01-01 00:00:01\"");
        expectedResults.add("false,\"hello world\",\"2020-01-01 00:00:02\"");
        expectedResults.add("false,\"hello world\",\"2020-01-01 00:00:03\"");
        expectedResults.add("false,\"hello world\",\"2020-01-01 00:00:04\"");
        expectedResults.add("true,\"hello world\",\"2020-01-01 00:00:05\"");
        expectedResults.add("false,\"hello world!!!!\",\"2020-01-01 00:00:06\"");
        TestBaseUtils.compareResultCollections(
                expectedResults, actualResults, Comparator.naturalOrder());
    }

    private void executeAndVerifySinkResult(
            Executor executor, String sessionId, String statement, String resultPath)
            throws Exception {
        final TableResult tableResult = executeSql(executor, sessionId, statement);
        checkState(tableResult.getJobClient().isPresent());
        // wait for job completion
        tableResult.await();
        // verify result
        verifySinkResult(resultPath);
    }

    private Map<String, String> getDefaultSessionConfigMap() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.name());
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.CHANGELOG.name());
        configMap.put(EXECUTION_MAX_TABLE_RESULT_ROWS.key(), "100");
        return configMap;
    }

    private List<String> getInitSQL(final Map<String, String> replaceVars) {
        return Stream.of(
                        String.format(
                                "CREATE FUNCTION scalarUDF AS '%s'", ScalarUDF.class.getName()),
                        String.format(
                                "CREATE FUNCTION aggregateUDF AS '%s'",
                                AggregateFunction.class.getName()),
                        String.format("CREATE FUNCTION tableUDF AS '%s'", TableUDF.class.getName()),
                        "CREATE TABLE TableNumber1 (\n"
                                + "  IntegerField1 INT,\n"
                                + "  StringField1 STRING,\n"
                                + "  TimestampField1 TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_PATH1',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView1 AS SELECT scalarUDF(IntegerField1, 5) FROM TableNumber1\n",
                        "CREATE TABLE TableSourceSink (\n"
                                + "  BooleanField BOOLEAN,\n"
                                + "  StringField2 STRING,\n"
                                + "  TimestampField2 TIMESTAMP\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_SINK_PATH',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView2 AS SELECT * FROM TestView1\n")
                .map(
                        sql -> {
                            for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
                                sql = sql.replace(replaceVar.getKey(), replaceVar.getValue());
                            }
                            return sql;
                        })
                .collect(Collectors.toList());
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
