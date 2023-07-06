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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.result.ChangelogCollectResult;
import org.apache.flink.table.client.gateway.result.MaterializedResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedSqlGatewayService;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.session.OpenSessionHandler;
import org.apache.flink.table.gateway.rest.handler.util.GetApiVersionHandler;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.util.GetApiVersionHeaders;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.rest.util.TestingSqlGatewayRestEndpoint;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.utils.UserDefinedFunctions;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_CHECK_INTERVAL;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_SESSION_IDLE_TIMEOUT;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getBaseConfig;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointTestUtils.getFlinkConfig;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contains basic tests for the {@link ExecutorImpl}. */
class ExecutorImplITCase {

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    @TempDir
    @Order(1)
    public static File tempFolder;

    @RegisterExtension
    @Order(2)
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    () ->
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(getConfig())
                                    .setNumberTaskManagers(NUM_TMS)
                                    .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                                    .build());

    @RegisterExtension
    @Order(3)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(
                    () -> {
                        Configuration configuration =
                                new Configuration(MINI_CLUSTER_RESOURCE.getClientConfiguration());
                        configuration.set(SQL_GATEWAY_SESSION_IDLE_TIMEOUT, Duration.ofSeconds(3));
                        configuration.set(
                                SQL_GATEWAY_SESSION_CHECK_INTERVAL, Duration.ofSeconds(1));
                        return configuration;
                    });

    @RegisterExtension
    @Order(4)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    @RegisterExtension
    @Order(5)
    private static final SqlGatewayRestEndpointExtension TEST_SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(TestSqlGatewayService::new);

    private static RestClusterClient<?> clusterClient;

    // a generated UDF jar used for testing classloading of dependencies
    private static URL udfDependency;

    private final ThreadFactory threadFactory =
            new ExecutorThreadFactory("Executor Test Pool", IgnoreExceptionHandler.INSTANCE);

    @BeforeAll
    static void setup(@InjectClusterClient RestClusterClient<?> injectedClusterClient)
            throws Exception {
        clusterClient = injectedClusterClient;
        File udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder,
                        "test-classloader-udf.jar",
                        GENERATED_LOWER_UDF_CLASS,
                        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        udfDependency = udfJar.toURI().toURL();
    }

    private static Configuration getConfig() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, tempFolder.toURI().toString());
        return config;
    }

    @Test
    void testCompleteStatement() {
        try (Executor executor = createRestServiceExecutor()) {
            initSession(executor, Collections.emptyMap());

            final List<String> expectedTableHints =
                    Arrays.asList(
                            "default_catalog.default_database.TableNumber1",
                            "default_catalog.default_database.TableSourceSink");
            assertThat(executor.completeStatement("SELECT * FROM Ta", 16))
                    .isEqualTo(expectedTableHints);

            final List<String> expectedClause = Collections.singletonList("WHERE");
            assertThat(executor.completeStatement("SELECT * FROM TableNumber1 WH", 29))
                    .isEqualTo(expectedClause);

            final List<String> expectedField = Collections.singletonList("IntegerField1");
            assertThat(executor.completeStatement("SELECT * FROM TableNumber1 WHERE Inte", 37))
                    .isEqualTo(expectedField);
        }
    }

    @Test
    void testStreamQueryExecutionChangelog() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency), configuration)) {
            initSession(executor, replaceVars);
            // start job and retrieval
            final ResultDescriptor desc =
                    executeQuery(
                            executor,
                            "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1");

            assertThat(desc.isMaterialized()).isFalse();

            final List<String> actualResults =
                    retrieveChangelogResult(desc.createResult(), desc.getRowDataStringConverter());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("[47, Hello World, ABC]");
            expectedResults.add("[27, Hello World, ABC]");
            expectedResults.add("[37, Hello World, ABC]");
            expectedResults.add("[37, Hello World, ABC]");
            expectedResults.add("[47, Hello World, ABC]");
            expectedResults.add("[57, Hello World!!!!, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        }
    }

    @Test
    void testStreamQueryExecutionChangelogMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[27, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[57, Hello World!!!!]");

        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency), configuration)) {
            initSession(executor, replaceVars);

            for (int i = 0; i < 3; i++) {
                // start job and retrieval
                final ResultDescriptor desc =
                        executeQuery(
                                executor,
                                "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1");

                assertThat(desc.isMaterialized()).isFalse();

                final List<String> actualResults =
                        retrieveChangelogResult(
                                desc.createResult(), desc.getRowDataStringConverter());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        }
    }

    @Test
    void testStreamQueryExecutionTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Map<String, String> configMap = getDefaultSessionConfigMap();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query =
                "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World, ABC]");
        expectedResults.add("[27, Hello World, ABC]");
        expectedResults.add("[37, Hello World, ABC]");
        expectedResults.add("[37, Hello World, ABC]");
        expectedResults.add("[47, Hello World, ABC]");
        expectedResults.add("[57, Hello World!!!!, ABC]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test
    void testStreamQueryExecutionTableMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query = "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[27, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[57, Hello World!!!!]");

        for (int i = 0; i < 3; i++) {
            executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
        }
    }

    @Test
    void testStreamQueryExecutionLimitedTable() throws Exception {
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
        expectedResults.add("[1, Hello World!!!!]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test
    void testBatchQueryExecution() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency),
                        Configuration.fromMap(configMap))) {
            initSession(executor, replaceVars);
            final ResultDescriptor desc = executeQuery(executor, "SELECT *, 'ABC' FROM TestView1");

            assertThat(desc.isMaterialized()).isTrue();

            final List<String> actualResults =
                    retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("[47, ABC]");
            expectedResults.add("[27, ABC]");
            expectedResults.add("[37, ABC]");
            expectedResults.add("[37, ABC]");
            expectedResults.add("[47, ABC]");
            expectedResults.add("[57, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        }
    }

    @Test
    void testBatchQueryExecutionMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47]");
        expectedResults.add("[27]");
        expectedResults.add("[37]");
        expectedResults.add("[37]");
        expectedResults.add("[47]");
        expectedResults.add("[57]");

        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency),
                        Configuration.fromMap(configMap))) {
            initSession(executor, replaceVars);
            for (int i = 0; i < 3; i++) {
                final ResultDescriptor desc = executeQuery(executor, "SELECT * FROM TestView1");

                assertThat(desc.isMaterialized()).isTrue();

                final List<String> actualResults =
                        retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void testStopJob(boolean withSavepoint) throws Exception {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.name());
        configMap.put(TableConfigOptions.TABLE_DML_SYNC.key(), "false");

        final String srcDdl = "CREATE TABLE src (a STRING) WITH ('connector' = 'datagen')";
        final String snkDdl = "CREATE TABLE snk (a STRING) WITH ('connector' = 'blackhole')";
        final String insert = "INSERT INTO snk SELECT a FROM src;";

        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency),
                        Configuration.fromMap(configMap))) {

            executor.configureSession(srcDdl);
            executor.configureSession(snkDdl);
            StatementResult result = executor.executeStatement(insert);
            JobID jobID = result.getJobId();

            // wait till the job turns into running status or the test times out
            TestUtils.waitUntilAllTasksAreRunning(clusterClient, jobID);

            JobStatus expectedJobStatus;
            if (withSavepoint) {
                executor.configureSession(
                        String.format(
                                "SET '%s' = '%s'",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                                tempFolder.toURI()));
                StatementResult stopResult =
                        executor.executeStatement(
                                String.format("STOP JOB '%s' WITH SAVEPOINT", jobID));
                String savepointPath =
                        CollectionUtil.iteratorToList(stopResult).get(0).getString(0).toString();
                assertThat(savepointPath)
                        .matches(path -> Files.exists(Paths.get(URI.create(path))));
                expectedJobStatus = JobStatus.FINISHED;
            } else {
                executor.executeStatement(String.format("STOP JOB '%s'", jobID));
                TestUtils.waitUntilJobCanceled(jobID, clusterClient);
                expectedJobStatus = JobStatus.CANCELED;
            }
            assertThat(jobID).isNotNull();
            assertThat(
                            CollectionUtil.iteratorToList(executor.executeStatement("SHOW JOBS"))
                                    .stream()
                                    .filter(
                                            row ->
                                                    row.getString(0)
                                                            .toString()
                                                            .equals(jobID.toHexString()))
                                    .findAny())
                    .isPresent()
                    .map(row -> row.getString(2).toString())
                    .get()
                    .isEqualTo(expectedJobStatus.name());
        }
    }

    @Test
    void testInterruptSubmitting() throws Exception {
        testInterrupting(executor -> executor.executeStatement(BlockPhase.SUBMIT.name()));
    }

    @Test
    void testInterruptExecution() throws Exception {
        testInterrupting(executor -> executor.executeStatement(BlockPhase.EXECUTION.name()));
    }

    @Test
    void testInterruptFetching() throws Exception {
        testInterrupting(
                executor -> {
                    try (StatementResult result =
                            executor.executeStatement(BlockPhase.FETCHING.name())) {
                        // trigger to fetch again
                        result.hasNext();
                    }
                });
    }

    @Test
    void testHeartbeat() throws Exception {
        try (ExecutorImpl executor = (ExecutorImpl) createRestServiceExecutor()) {
            Thread.sleep(5_000);
            assertThat(
                            ((SessionManagerImpl) SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager())
                                    .isSessionAlive(executor.getSessionHandle()))
                    .isTrue();
        }
    }

    @Test
    void testConnectToEndpointWithV1Version() {
        assertThatThrownBy(() -> testNegotiateVersion(SqlGatewayRestAPIVersion.V1, executor -> {}))
                .satisfies(
                        anyCauseMatches(
                                SqlExecutionException.class,
                                "Currently, SQL Client only supports to connect to the "
                                        + "REST endpoint with API version larger than V1."));
    }

    @Test
    void testConnectToEndpointWithHigherVersion() throws Exception {
        testNegotiateVersion(
                SqlGatewayRestAPIVersion.V2,
                executor ->
                        assertThat(
                                        SQL_GATEWAY_SERVICE_EXTENSION
                                                .getService()
                                                .getSessionEndpointVersion(
                                                        ((ExecutorImpl) executor)
                                                                .getSessionHandle()))
                                .isEqualTo(SqlGatewayRestAPIVersion.V2));
    }

    @Test
    void testExecutorCloseSession() {
        SessionHandle sessionHandle;
        try (ExecutorImpl executor = (ExecutorImpl) createRestServiceExecutor()) {
            // close executor multiple times
            executor.close();
            sessionHandle = executor.getSessionHandle();
        }

        assertThat(sessionHandle).isNotNull();
        assertThat(
                        ((SessionManagerImpl) SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager())
                                .isSessionAlive(sessionHandle))
                .isFalse();
    }

    @Test
    void testCustomHeadersSupport() {
        final Map<String, String> envMap =
                Collections.singletonMap(
                        ConfigConstants.FLINK_REST_CLIENT_HEADERS,
                        "Cookie:authCookie=12:345\nCustomHeader:value1,value2\nMalformedHeaderSkipped");
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(envMap);
        try (final ExecutorImpl executor = (ExecutorImpl) createTestServiceExecutor()) {
            final List<HttpHeader> customHttpHeaders =
                    new ArrayList<>(executor.getCustomHttpHeaders());
            final HttpHeader expectedHeader1 = new HttpHeader("Cookie", "authCookie=12:345");
            final HttpHeader expectedHeader2 = new HttpHeader("CustomHeader", "value1,value2");
            assertThat(customHttpHeaders).hasSize(2);
            assertThat(customHttpHeaders.get(0)).isEqualTo(expectedHeader1);
            assertThat(customHttpHeaders.get(1)).isEqualTo(expectedHeader2);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper method
    // --------------------------------------------------------------------------------------------

    private void testNegotiateVersion(
            SqlGatewayRestAPIVersion version, Consumer<Executor> validator) throws Exception {
        final String address = InetAddress.getLoopbackAddress().getHostAddress();
        Configuration config = getBaseConfig(getFlinkConfig(address, address, "0"));
        try (TestingSqlGatewayRestEndpoint endpoint =
                        TestingSqlGatewayRestEndpoint.builder(
                                        config, SQL_GATEWAY_SERVICE_EXTENSION.getService())
                                .withHandler(
                                        GetApiVersionHeaders.getInstance(),
                                        new GetApiVersionHandler(
                                                SQL_GATEWAY_SERVICE_EXTENSION.getService(),
                                                Collections.emptyMap(),
                                                GetApiVersionHeaders.getInstance(),
                                                Collections.singletonList(version)))
                                .withHandler(
                                        OpenSessionHeaders.getInstance(),
                                        new OpenSessionHandler(
                                                SQL_GATEWAY_SERVICE_EXTENSION.getService(),
                                                Collections.emptyMap(),
                                                OpenSessionHeaders.getInstance()))
                                .buildAndStart();
                Executor executor =
                        createExecutor(
                                Collections.emptyList(), config, endpoint.getServerAddress())) {
            validator.accept(executor);
        }
    }

    private void testInterrupting(Consumer<Executor> task) throws Exception {
        try (Executor executor = createTestServiceExecutor()) {
            TestSqlGatewayService service =
                    (TestSqlGatewayService)
                            TEST_SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getSqlGatewayService();
            Thread t =
                    threadFactory.newThread(
                            () -> {
                                try {
                                    task.accept(executor);
                                } finally {
                                    // notify server to return results until the executor finishes
                                    // exception processing.
                                    service.latch.countDown();
                                }
                            });
            t.start();
            CommonTestUtils.waitUntilCondition(() -> service.isBlocking, 100L);

            // interrupt the submission
            t.interrupt();

            CommonTestUtils.waitUntilCondition(() -> service.isClosed, 100L);
        }
    }

    private ResultDescriptor executeQuery(Executor executor, String query) {
        return new ResultDescriptor(executor.executeStatement(query), executor.getSessionConfig());
    }

    private Executor createRestServiceExecutor() {
        return createRestServiceExecutor(Collections.emptyList(), new Configuration());
    }

    private Executor createRestServiceExecutor(
            List<URL> dependencies, Configuration configuration) {
        return createExecutor(
                dependencies,
                configuration,
                InetSocketAddress.createUnresolved(
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));
    }

    private Executor createTestServiceExecutor() {
        return createExecutor(
                Collections.emptyList(),
                new Configuration(),
                InetSocketAddress.createUnresolved(
                        TEST_SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        TEST_SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));
    }

    private Executor createExecutor(
            List<URL> dependencies, Configuration configuration, InetSocketAddress address) {
        configuration.addAll(clusterClient.getFlinkConfiguration());
        DefaultContext defaultContext = new DefaultContext(configuration, dependencies);
        // frequently trigger heartbeat
        return new ExecutorImpl(defaultContext, address, "test-session", 1_000);
    }

    private void initSession(Executor executor, Map<String, String> replaceVars) {
        for (String sql : getInitSQL(replaceVars)) {
            executor.configureSession(sql);
        }
    }

    private void executeStreamQueryTable(
            Map<String, String> replaceVars,
            Map<String, String> configMap,
            String query,
            List<String> expectedResults)
            throws Exception {
        try (final Executor executor =
                createRestServiceExecutor(
                        Collections.singletonList(udfDependency),
                        Configuration.fromMap(configMap))) {
            initSession(executor, replaceVars);
            // start job and retrieval
            final ResultDescriptor desc = executeQuery(executor, query);

            assertThat(desc.isMaterialized()).isTrue();

            final List<String> actualResults =
                    retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        }
    }

    private List<String> retrieveTableResult(
            MaterializedResult materializedResult,
            RowDataToStringConverter rowDataToStringConverter)
            throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<Integer> result = materializedResult.snapshot(2);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                actualResults.clear();
                IntStream.rangeClosed(1, result.getPayload())
                        .forEach(
                                (page) -> {
                                    for (RowData row : materializedResult.retrievePage(page)) {
                                        actualResults.add(
                                                StringUtils.arrayAwareToString(
                                                        rowDataToStringConverter.convert(row)));
                                    }
                                });
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }

        return actualResults;
    }

    private List<String> retrieveChangelogResult(
            ChangelogCollectResult collectResult, RowDataToStringConverter rowDataToStringConverter)
            throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<List<RowData>> result = collectResult.retrieveChanges();
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                for (RowData row : result.getPayload()) {
                    actualResults.add(
                            StringUtils.arrayAwareToString(rowDataToStringConverter.convert(row)));
                }
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }
        return actualResults;
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
                                "CREATE FUNCTION scalarUDF AS '%s'",
                                UserDefinedFunctions.ScalarUDF.class.getName()),
                        String.format(
                                "CREATE FUNCTION aggregateUDF AS '%s'",
                                AggregateFunction.class.getName()),
                        String.format(
                                "CREATE FUNCTION tableUDF AS '%s'",
                                UserDefinedFunctions.TableUDF.class.getName()),
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
    // Test SqlGatewayService
    // --------------------------------------------------------------------------------------------

    private static class TestSqlGatewayService extends MockedSqlGatewayService {

        private CountDownLatch latch = new CountDownLatch(1);
        private @Nullable volatile BlockPhase blockPhase;
        private volatile boolean isBlocking;
        private volatile boolean isClosed;

        @Override
        public SessionHandle openSession(SessionEnvironment environment)
                throws SqlGatewayException {
            this.isClosed = false;
            this.isBlocking = false;
            return SessionHandle.create();
        }

        @Override
        public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
            // do nothing
        }

        @Override
        public OperationHandle executeStatement(
                SessionHandle sessionHandle,
                String statement,
                long executionTimeoutMs,
                Configuration executionConfig)
                throws SqlGatewayException {
            this.isClosed = false;
            this.isBlocking = false;
            this.latch = new CountDownLatch(1);
            this.blockPhase = BlockPhase.valueOf(statement);
            if (this.blockPhase == BlockPhase.SUBMIT) {
                try {
                    isBlocking = true;
                    latch.await();
                } catch (Exception e) {
                    throw new SqlGatewayException(e);
                }
            }
            return OperationHandle.create();
        }

        @Override
        public void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
                throws SqlGatewayException {
            // do nothing
        }

        @Override
        public void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
                throws SqlGatewayException {
            this.isClosed = true;
        }

        @Override
        public ResultSet fetchResults(
                SessionHandle sessionHandle,
                OperationHandle operationHandle,
                long token,
                int maxRows) {
            try {
                if (token == 0 && blockPhase == BlockPhase.EXECUTION) {
                    isBlocking = true;
                    latch.await();
                } else if (token > 0 && blockPhase == BlockPhase.FETCHING) {
                    isBlocking = true;
                    latch.await();
                }
                return new ResultSetImpl(
                        ResultSet.ResultType.PAYLOAD,
                        token + 1,
                        ResolvedSchema.of(Column.physical("result", DataTypes.INT())),
                        Collections.emptyList(),
                        SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                        true,
                        JobID.generate(),
                        ResultKind.SUCCESS_WITH_CONTENT);
            } catch (Exception e) {
                throw new SqlGatewayException(e);
            }
        }
    }

    enum BlockPhase {
        SUBMIT,

        EXECUTION,

        FETCHING
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
