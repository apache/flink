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

package org.apache.flink.table.gateway.service;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.result.NotReadyResult;
import org.apache.flink.table.gateway.service.session.SessionManagerImpl;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.planner.runtime.batch.sql.TestModule;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.planner.utils.TableFunc0;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.UserClassLoaderJarTestUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.apache.flink.table.api.ResultKind.SUCCESS_WITH_CONTENT;
import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.OTHER;
import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.PAYLOAD;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.createInitializedSession;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.fetchResults;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** ITCase for {@link SqlGatewayServiceImpl}. */
public class SqlGatewayServiceITCase {

    @RegisterExtension
    @Order(1)
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @RegisterExtension
    @Order(2)
    static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    private static SessionManagerImpl sessionManager;
    private static SqlGatewayServiceImpl service;

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();

    @BeforeAll
    static void setUp() {
        sessionManager = (SessionManagerImpl) SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @Test
    void testOpenSessionWithConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "val1");
        options.put("key2", "val2");
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .addSessionConfig(options)
                        .build();

        SessionHandle sessionHandle = service.openSession(environment);
        Map<String, String> actualConfig = service.getSessionConfig(sessionHandle);

        assertThat(actualConfig).containsAllEntriesOf(options);
    }

    @Test
    void testOpenSessionWithEnvironment() {
        String catalogName = "default";
        String databaseName = "testDb";
        String moduleName = "testModule";
        GenericInMemoryCatalog defaultCatalog =
                new GenericInMemoryCatalog(catalogName, databaseName);
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog(catalogName, defaultCatalog)
                        .registerModuleAtHead(moduleName, new TestModule())
                        .setDefaultCatalog(catalogName)
                        .build();

        SessionHandle sessionHandle = service.openSession(environment);
        TableEnvironmentInternal tableEnv =
                service.getSession(sessionHandle)
                        .createExecutor(new Configuration())
                        .getTableEnvironment();
        assertThat(tableEnv.getCurrentCatalog()).isEqualTo(catalogName);
        assertThat(tableEnv.getCurrentDatabase()).isEqualTo(databaseName);
        assertThat(tableEnv.listModules()).contains(moduleName);
    }

    @Test
    void testConfigureSessionWithLegalStatement(@TempDir java.nio.file.Path tmpDir)
            throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        // SET & RESET
        service.configureSession(sessionHandle, "SET 'key1' = 'value1';", 0);
        Map<String, String> config = new HashMap<>();
        config.put("key1", "value1");
        assertThat(service.getSessionConfig(sessionHandle)).containsAllEntriesOf(config);

        service.configureSession(sessionHandle, "RESET 'key1';", 0);
        assertThat(service.getSessionConfig(sessionHandle)).doesNotContainEntry("key1", "value1");

        // CREATE & USE & ALTER & DROP
        service.configureSession(
                sessionHandle,
                "CREATE CATALOG mycat with ('type' = 'generic_in_memory', 'default-database' = 'db');",
                0);

        service.configureSession(sessionHandle, "USE CATALOG mycat;", 0);
        assertThat(service.getCurrentCatalog(sessionHandle)).isEqualTo("mycat");

        service.configureSession(
                sessionHandle,
                "CREATE TABLE db.tbl (score INT) WITH ('connector' = 'datagen');",
                0);

        Set<TableKind> tableKinds = new HashSet<>();
        tableKinds.add(TableKind.TABLE);
        assertThat(service.listTables(sessionHandle, "mycat", "db", tableKinds))
                .contains(
                        new TableInfo(ObjectIdentifier.of("mycat", "db", "tbl"), TableKind.TABLE));

        service.configureSession(sessionHandle, "ALTER TABLE db.tbl RENAME TO tbl1;", 0);
        assertThat(service.listTables(sessionHandle, "mycat", "db", tableKinds))
                .doesNotContain(
                        new TableInfo(ObjectIdentifier.of("mycat", "db", "tbl"), TableKind.TABLE))
                .contains(
                        new TableInfo(ObjectIdentifier.of("mycat", "db", "tbl1"), TableKind.TABLE));

        service.configureSession(sessionHandle, "USE CATALOG default_catalog;", 0);
        service.configureSession(sessionHandle, "DROP CATALOG mycat;", 0);
        assertThat(service.listCatalogs(sessionHandle)).doesNotContain("mycat");

        // LOAD & UNLOAD MODULE
        validateStatementResult(
                sessionHandle,
                "SHOW FULL MODULES",
                Collections.singletonList(GenericRowData.of(StringData.fromString("core"), true)));

        service.configureSession(sessionHandle, "UNLOAD MODULE core;", 0);
        validateStatementResult(sessionHandle, "SHOW FULL MODULES", Collections.emptyList());

        service.configureSession(sessionHandle, "LOAD MODULE core;", 0);
        validateStatementResult(
                sessionHandle,
                "SHOW FULL MODULES",
                Collections.singletonList(GenericRowData.of(StringData.fromString("core"), true)));

        // ADD JAR
        String udfClassName = GENERATED_LOWER_UDF_CLASS + new Random().nextInt(50);
        String jarPath =
                UserClassLoaderJarTestUtils.createJarFile(
                                new File(tmpDir.toUri()),
                                "test-add-jar.jar",
                                udfClassName,
                                String.format(GENERATED_LOWER_UDF_CODE, udfClassName))
                        .toURI()
                        .getPath();
        service.configureSession(sessionHandle, String.format("ADD JAR '%s';", jarPath), 0);
        validateStatementResult(
                sessionHandle,
                "SHOW JARS",
                Collections.singletonList(GenericRowData.of(StringData.fromString(jarPath))));
    }

    @Test
    void testFetchResultsInRunning() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch startRunningLatch = new CountDownLatch(1);
        CountDownLatch endRunningLatch = new CountDownLatch(1);
        OperationHandle operationHandle =
                submitDefaultOperation(
                        sessionHandle,
                        () -> {
                            startRunningLatch.countDown();
                            endRunningLatch.await();
                        });

        startRunningLatch.await();
        assertThat(fetchResults(service, sessionHandle, operationHandle))
                .isEqualTo(NotReadyResult.INSTANCE);
        endRunningLatch.countDown();
    }

    @Test
    void testGetOperationFinishedAndFetchResults() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch startRunningLatch = new CountDownLatch(1);
        CountDownLatch endRunningLatch = new CountDownLatch(1);

        OperationHandle operationHandle =
                submitDefaultOperation(
                        sessionHandle,
                        () -> {
                            startRunningLatch.countDown();
                            endRunningLatch.await();
                        });

        startRunningLatch.await();
        assertThat(service.getOperationInfo(sessionHandle, operationHandle))
                .isEqualTo(new OperationInfo(OperationStatus.RUNNING));

        endRunningLatch.countDown();
        awaitOperationTermination(service, sessionHandle, operationHandle);

        List<RowData> expectedData = getDefaultResultSet().getData();
        List<RowData> actualData = fetchAllResults(sessionHandle, operationHandle);
        assertThat(actualData).isEqualTo(expectedData);

        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    void testCancelOperation() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch startRunningLatch = new CountDownLatch(1);
        CountDownLatch endRunningLatch = new CountDownLatch(1);

        OperationHandle operationHandle =
                submitDefaultOperation(
                        sessionHandle,
                        () -> {
                            startRunningLatch.countDown();
                            endRunningLatch.await();
                        });

        startRunningLatch.await();
        assertThat(service.getOperationInfo(sessionHandle, operationHandle))
                .isEqualTo(new OperationInfo(OperationStatus.RUNNING));

        service.cancelOperation(sessionHandle, operationHandle);

        assertThat(service.getOperationInfo(sessionHandle, operationHandle))
                .isEqualTo(new OperationInfo(OperationStatus.CANCELED));
        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    void testOperationGetErrorAndFetchError() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch startRunningLatch = new CountDownLatch(1);

        String msg = "Artificial Exception.";
        OperationHandle operationHandle =
                submitDefaultOperation(
                        sessionHandle,
                        () -> {
                            startRunningLatch.countDown();
                            throw new SqlExecutionException(msg);
                        });
        startRunningLatch.await();

        CommonTestUtils.waitUtil(
                () ->
                        service.getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .equals(OperationStatus.ERROR),
                Duration.ofSeconds(10),
                "Failed to get expected operation status.");

        assertThatThrownBy(() -> fetchResults(service, sessionHandle, operationHandle))
                .satisfies(anyCauseMatches(SqlExecutionException.class, msg));

        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    void testExecuteSqlWithConfig() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String key = "username";
        String value = "Flink";
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        "SET",
                        -1,
                        Configuration.fromMap(Collections.singletonMap(key, value)));

        List<RowData> settings = fetchAllResults(sessionHandle, operationHandle);

        assertThat(settings)
                .contains(
                        GenericRowData.of(
                                StringData.fromString(key), StringData.fromString(value)));
    }

    @ParameterizedTest
    @CsvSource({"WITH SAVEPOINT,true", "WITH SAVEPOINT WITH DRAIN,true", "'',false"})
    void testStopJobStatementWithSavepoint(
            String option,
            boolean hasSavepoint,
            @InjectClusterClient RestClusterClient<?> restClusterClient,
            @TempDir File tmpDir)
            throws Exception {
        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.setBoolean(TableConfigOptions.TABLE_DML_SYNC, false);
        File savepointDir = new File(tmpDir, "savepoints");
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        String sourceDdl = "CREATE TABLE source (a STRING) WITH ('connector'='datagen');";
        String sinkDdl = "CREATE TABLE sink (a STRING) WITH ('connector'='blackhole');";
        String insertSql = "INSERT INTO sink SELECT * FROM source;";
        String stopSqlTemplate = "STOP JOB '%s' %s;";

        service.executeStatement(sessionHandle, sourceDdl, -1, configuration);
        service.executeStatement(sessionHandle, sinkDdl, -1, configuration);

        OperationHandle insertOperationHandle =
                service.executeStatement(sessionHandle, insertSql, -1, configuration);

        List<RowData> results = fetchAllResults(sessionHandle, insertOperationHandle);
        assertThat(results.size()).isEqualTo(1);
        String jobId = results.get(0).getString(0).toString();

        TestUtils.waitUntilAllTasksAreRunning(restClusterClient, JobID.fromHexString(jobId));

        String stopSql = String.format(stopSqlTemplate, jobId, option);
        OperationHandle stopOperationHandle =
                service.executeStatement(sessionHandle, stopSql, -1, configuration);

        List<RowData> stopResults = fetchAllResults(sessionHandle, stopOperationHandle);
        assertThat(stopResults.size()).isEqualTo(1);
        if (hasSavepoint) {
            String savepoint = stopResults.get(0).getString(0).toString();
            Path savepointPath = Paths.get(savepoint);
            assertThat(savepointPath.getFileName().toString()).startsWith("savepoint-");
        } else {
            assertThat(stopResults.get(0).getString(0)).hasToString("OK");
        }
    }

    @Test
    void testGetOperationSchemaUntilOperationIsReady() throws Exception {
        runGetOperationSchemaUntilOperationIsReadyOrError(
                this::getDefaultResultSet,
                task -> assertThat(task.get()).isEqualTo(getDefaultResultSet().getResultSchema()));
    }

    @Test
    void testShowJobsOperation(@InjectClusterClient RestClusterClient<?> restClusterClient)
            throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());

        String pipelineName = "test-job";
        configuration.setString(PipelineOptions.NAME, pipelineName);

        // running jobs
        String sourceDdl = "CREATE TABLE source (a STRING) WITH ('connector'='datagen');";
        String sinkDdl = "CREATE TABLE sink (a STRING) WITH ('connector'='blackhole');";
        String insertSql = "INSERT INTO sink SELECT * FROM source;";

        service.executeStatement(sessionHandle, sourceDdl, -1, configuration);
        service.executeStatement(sessionHandle, sinkDdl, -1, configuration);

        long timeOpStart = System.currentTimeMillis();
        OperationHandle insertsOperationHandle =
                service.executeStatement(sessionHandle, insertSql, -1, configuration);
        String jobId =
                fetchAllResults(sessionHandle, insertsOperationHandle)
                        .get(0)
                        .getString(0)
                        .toString();

        TestUtils.waitUntilAllTasksAreRunning(restClusterClient, JobID.fromHexString(jobId));
        long timeOpSucceed = System.currentTimeMillis();

        OperationHandle showJobsOperationHandle1 =
                service.executeStatement(sessionHandle, "SHOW JOBS", -1, configuration);

        List<RowData> result = fetchAllResults(sessionHandle, showJobsOperationHandle1);
        RowData jobRow =
                result.stream()
                        .filter(row -> jobId.equals(row.getString(0).toString()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Test job " + jobId + " not found."));
        assertThat(jobRow.getString(1)).hasToString(pipelineName);
        assertThat(jobRow.getString(2)).hasToString("RUNNING");
        assertThat(jobRow.getTimestamp(3, 3).getMillisecond())
                .isBetween(timeOpStart, timeOpSucceed);
    }

    // --------------------------------------------------------------------------------------------
    // Catalog API tests
    // --------------------------------------------------------------------------------------------

    @Test
    void testGetCurrentCatalog() {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
                        .registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
                        .setDefaultCatalog("cat2")
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);
        assertThat(service.getCurrentCatalog(sessionHandle)).isEqualTo("cat2");
    }

    @Test
    void testListCatalogs() {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
                        .registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);
        assertThat(service.listCatalogs(sessionHandle)).contains("cat1", "cat2");
    }

    @Test
    void testListDatabases() throws Exception {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat", new GenericInMemoryCatalog("cat"))
                        .setDefaultCatalog("cat")
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);
        Configuration configuration =
                Configuration.fromMap(service.getSessionConfig(sessionHandle));

        service.executeStatement(sessionHandle, "CREATE DATABASE db1", -1, configuration);
        OperationHandle operationHandle =
                service.executeStatement(sessionHandle, "CREATE DATABASE db2", -1, configuration);

        awaitOperationTermination(service, sessionHandle, operationHandle);
        assertThat(service.listDatabases(sessionHandle, "cat")).contains("db1", "db2");
    }

    @Test
    void testListTables() {
        SessionHandle sessionHandle = createInitializedSession(service);
        assertThat(
                        service.listTables(
                                sessionHandle,
                                "cat1",
                                "db1",
                                new HashSet<>(Arrays.asList(TableKind.TABLE, TableKind.VIEW))))
                .containsExactlyInAnyOrder(
                        new TableInfo(ObjectIdentifier.of("cat1", "db1", "tbl1"), TableKind.TABLE),
                        new TableInfo(ObjectIdentifier.of("cat1", "db1", "tbl2"), TableKind.TABLE),
                        new TableInfo(ObjectIdentifier.of("cat1", "db1", "tbl3"), TableKind.VIEW),
                        new TableInfo(ObjectIdentifier.of("cat1", "db1", "tbl4"), TableKind.VIEW));
        assertThat(
                        service.listTables(
                                sessionHandle,
                                "cat1",
                                "db2",
                                Collections.singleton(TableKind.TABLE)))
                .containsExactly(
                        new TableInfo(ObjectIdentifier.of("cat1", "db2", "tbl1"), TableKind.TABLE));
        assertThat(
                        service.listTables(
                                sessionHandle,
                                "cat2",
                                "db0",
                                Collections.singleton(TableKind.VIEW)))
                .isEmpty();
    }

    @Test
    void testListSystemFunctions() {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
                        .registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);

        assertThat(service.listSystemFunctions(sessionHandle))
                .contains(
                        new FunctionInfo(FunctionIdentifier.of("sin"), SCALAR),
                        new FunctionInfo(FunctionIdentifier.of("sum"), AGGREGATE),
                        new FunctionInfo(FunctionIdentifier.of("as"), OTHER));
    }

    @Test
    void testListUserDefinedFunctions() {
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
                        .registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
                        .build();
        SessionHandle sessionHandle = service.openSession(environment);
        TableEnvironment tEnv =
                service.getSession(sessionHandle).createExecutor().getTableEnvironment();
        tEnv.createTemporarySystemFunction(
                "count_distinct", JavaUserDefinedAggFunctions.CountDistinct.class);
        tEnv.createFunction("java1", JavaUserDefinedScalarFunctions.JavaFunc1.class);
        tEnv.createTemporaryFunction("table_func0", TableFunc0.class);

        // register catalog function in another catalog
        tEnv.createFunction(
                "cat1.default.filter_out_function", JavaUserDefinedScalarFunctions.JavaFunc1.class);

        assertThat(
                        service.listUserDefinedFunctions(
                                sessionHandle, "default_catalog", "default_database"))
                .contains(
                        new FunctionInfo(FunctionIdentifier.of("count_distinct")),
                        new FunctionInfo(
                                FunctionIdentifier.of(
                                        ObjectIdentifier.of(
                                                "default_catalog", "default_database", "java1"))),
                        new FunctionInfo(
                                FunctionIdentifier.of(
                                        ObjectIdentifier.of(
                                                "default_catalog",
                                                "default_database",
                                                "table_func0"))));
    }

    @Test
    void testCompleteStatement() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        String createTable1 =
                "CREATE TABLE Table1 (\n"
                        + "  IntegerField1 INT,\n"
                        + "  StringField1 STRING,\n"
                        + "  TimestampField1 TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen'\n"
                        + ")\n";
        String createTable2 =
                "CREATE TABLE Table2 (\n"
                        + "  BooleanField BOOLEAN,\n"
                        + "  StringField2 STRING,\n"
                        + "  TimestampField2 TIMESTAMP\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")\n";

        service.getSession(sessionHandle)
                .createExecutor()
                .getTableEnvironment()
                .executeSql(createTable1);
        service.getSession(sessionHandle)
                .createExecutor()
                .getTableEnvironment()
                .executeSql(createTable2);

        validateCompletionHints(
                sessionHandle,
                "SELECT * FROM Ta",
                Arrays.asList(
                        "default_catalog.default_database.Table1",
                        "default_catalog.default_database.Table2"));

        validateCompletionHints(
                sessionHandle, "SELECT * FROM Table1 WH", Collections.singletonList("WHERE"));

        validateCompletionHints(
                sessionHandle,
                "SELECT * FROM Table1 WHERE Inte",
                Collections.singletonList("IntegerField1"));
    }

    @Test
    void testGetTable() {
        SessionHandle sessionHandle = createInitializedSession(service);
        ResolvedCatalogTable actualTable =
                (ResolvedCatalogTable)
                        service.getTable(sessionHandle, ObjectIdentifier.of("cat1", "db1", "tbl1"));
        assertThat(actualTable.getResolvedSchema()).isEqualTo(ResolvedSchema.of());
        assertThat(actualTable.getOptions())
                .isEqualTo(Collections.singletonMap("connector", "values"));

        ResolvedCatalogView actualView =
                (ResolvedCatalogView)
                        service.getTable(sessionHandle, ObjectIdentifier.of("cat1", "db1", "tbl3"));
        assertThat(actualView.getOriginalQuery()).isEqualTo("SELECT 1");
    }

    // --------------------------------------------------------------------------------------------
    // Concurrent tests
    // --------------------------------------------------------------------------------------------

    @Test
    void testCancelOperationAndFetchResultInParallel() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        CountDownLatch latch = new CountDownLatch(1);
        // Make sure cancel the Operation before finish.
        OperationHandle operationHandle = submitDefaultOperation(sessionHandle, latch::await);
        runCancelOrCloseOperationWhenFetchResults(
                sessionHandle,
                operationHandle,
                () -> service.cancelOperation(sessionHandle, operationHandle),
                new Condition<>(
                        msg ->
                                msg.contains(
                                        String.format(
                                                "Can not fetch results from the %s in %s status.",
                                                operationHandle, OperationStatus.CANCELED)),
                        "Fetch results with expected error message."));
        latch.countDown();
    }

    @Test
    void testCloseOperationAndFetchResultInParallel() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        OperationHandle operationHandle =
                submitDefaultOperation(
                        sessionHandle,
                        () -> {
                            // allow close before execution finish.
                            Thread.sleep(1);
                        });
        runCancelOrCloseOperationWhenFetchResults(
                sessionHandle,
                operationHandle,
                () -> service.closeOperation(sessionHandle, operationHandle),
                // It's possible the fetcher fetch the result from a closed operation or fetcher
                // can't find the operation.
                new Condition<>(
                        msg ->
                                msg.contains(
                                                String.format(
                                                        "Can not find the submitted operation in the OperationManager with the %s.",
                                                        operationHandle))
                                        || msg.contains(
                                                String.format(
                                                        "Can not fetch results from the %s in %s status.",
                                                        operationHandle, OperationStatus.CLOSED)),
                        "Fetch results with expected error message."));
    }

    @Test
    void testCancelAndCloseOperationInParallel() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        int operationNum = 200;
        List<OperationManager.Operation> operations = new ArrayList<>(operationNum);
        for (int i = 0; i < operationNum; i++) {
            boolean throwError = i % 2 == 0;
            OperationHandle operationHandle =
                    submitDefaultOperation(
                            sessionHandle,
                            () -> {
                                // allow cancel/close before execution finish.
                                Thread.sleep(100);
                                if (throwError) {
                                    throw new SqlGatewayException("Artificial Exception.");
                                }
                            });

            operations.add(
                    service.getSession(sessionHandle)
                            .getOperationManager()
                            .getOperation(operationHandle));

            ExecutorService executor = EXECUTOR_EXTENSION.getExecutor();
            executor.submit(() -> service.cancelOperation(sessionHandle, operationHandle));
            executor.submit(() -> service.closeOperation(sessionHandle, operationHandle));
        }

        CommonTestUtils.waitUtil(
                () ->
                        service.getSession(sessionHandle).getOperationManager().getOperationCount()
                                == 0,
                Duration.ofSeconds(10),
                "All operations should be closed.");

        for (OperationManager.Operation op : operations) {
            assertThat(op.getOperationInfo().getStatus()).isEqualTo(OperationStatus.CLOSED);
        }
    }

    @Test
    void testSubmitOperationAndCloseOperationManagerInParallel1() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        OperationManager manager = service.getSession(sessionHandle).getOperationManager();
        int submitThreadsNum = 100;
        CountDownLatch latch = new CountDownLatch(submitThreadsNum);
        for (int i = 0; i < submitThreadsNum; i++) {
            EXECUTOR_EXTENSION
                    .getExecutor()
                    .submit(
                            () -> {
                                try {
                                    submitDefaultOperation(sessionHandle, () -> {});
                                } finally {
                                    latch.countDown();
                                }
                            });
        }
        manager.close();
        latch.await();
        assertThat(manager.getOperationCount()).isEqualTo(0);
    }

    @Test
    void testSubmitOperationAndCloseOperationManagerInParallel2() throws Exception {
        int count = 3;
        CountDownLatch startRunning = new CountDownLatch(1);
        CountDownLatch terminateRunning = new CountDownLatch(1);
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        for (int i = 0; i < count; i++) {
            EXECUTOR_EXTENSION
                    .getExecutor()
                    .submit(
                            () ->
                                    service.submitOperation(
                                            sessionHandle,
                                            () -> {
                                                startRunning.countDown();
                                                terminateRunning.await();
                                                return getDefaultResultSet();
                                            }));
        }
        startRunning.await();
        service.getSession(sessionHandle).getOperationManager().close();
        terminateRunning.countDown();
    }

    @Test
    void testExecuteOperationInSequence() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        AtomicReference<Integer> v = new AtomicReference<>(0);

        int threadNum = 100;
        List<OperationHandle> handles = new ArrayList<>();

        for (int i = 0; i < threadNum; i++) {
            handles.add(
                    service.submitOperation(
                            sessionHandle,
                            () -> {
                                // If execute in parallel, the value of v may be overridden by
                                // another thread
                                int origin = v.get();
                                v.set(origin + 1);
                                return getDefaultResultSet();
                            }));
        }
        for (OperationHandle handle : handles) {
            awaitOperationTermination(service, sessionHandle, handle);
        }

        assertThat(v.get()).isEqualTo(threadNum);
    }

    @Test
    void testReleaseLockWhenFailedToSubmitOperation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        int maximumThreads = 500;
        List<SessionHandle> sessions = new ArrayList<>();
        List<OperationHandle> operations = new ArrayList<>();
        for (int i = 0; i < maximumThreads; i++) {
            SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
            sessions.add(sessionHandle);
            operations.add(
                    service.submitOperation(
                            sessionHandle,
                            () -> {
                                latch.await();
                                return getDefaultResultSet();
                            }));
        }
        // The queue is full and should reject
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        assertThatThrownBy(
                        () ->
                                service.submitOperation(
                                        sessionHandle,
                                        () -> {
                                            latch.await();
                                            return getDefaultResultSet();
                                        }))
                .satisfies(anyCauseMatches(RejectedExecutionException.class));
        latch.countDown();
        // Wait the first operation finishes
        awaitOperationTermination(service, sessions.get(0), operations.get(0));
        // Service is able to submit operation
        CountDownLatch success = new CountDownLatch(1);
        service.submitOperation(
                sessionHandle,
                () -> {
                    success.countDown();
                    return getDefaultResultSet();
                });
        CommonTestUtils.waitUtil(
                () -> success.getCount() == 0, Duration.ofSeconds(10), "Should come to end.");
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    void testConfigureSessionWithIllegalStatement() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        assertThatThrownBy(() -> service.configureSession(sessionHandle, "SELECT 1;", 0))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Unsupported statement for configuring session:SELECT 1;\n"
                                        + "The configureSession API only supports to execute statement of type "
                                        + "CREATE TABLE, DROP TABLE, ALTER TABLE, "
                                        + "CREATE DATABASE, DROP DATABASE, ALTER DATABASE, "
                                        + "CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, "
                                        + "CREATE CATALOG, DROP CATALOG, USE CATALOG, USE [CATALOG.]DATABASE, "
                                        + "CREATE VIEW, DROP VIEW, LOAD MODULE, UNLOAD MODULE, USE MODULE, ADD JAR."));
    }

    @Test
    void testFetchResultsFromCanceledOperation() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch latch = new CountDownLatch(1);

        OperationHandle operationHandle = submitDefaultOperation(sessionHandle, latch::await);
        service.cancelOperation(sessionHandle, operationHandle);
        assertThatThrownBy(() -> fetchResults(service, sessionHandle, operationHandle))
                .satisfies(
                        anyCauseMatches(
                                String.format(
                                        "Can not fetch results from the %s in %s status.",
                                        operationHandle, OperationStatus.CANCELED)));
        latch.countDown();
    }

    @Test
    void testRequestNonExistOperation() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        OperationHandle operationHandle = OperationHandle.create();
        List<RunnableWithException> requests =
                Arrays.asList(
                        () -> service.cancelOperation(sessionHandle, operationHandle),
                        () -> service.getOperationInfo(sessionHandle, operationHandle),
                        () -> fetchResults(service, sessionHandle, operationHandle));

        for (RunnableWithException request : requests) {
            assertThatThrownBy(request::run)
                    .satisfies(
                            anyCauseMatches(
                                    String.format(
                                            "Can not find the submitted operation in the OperationManager with the %s.",
                                            operationHandle)));
        }
    }

    @Test
    void testGetOperationSchemaWhenOperationGetError() throws Exception {
        String msg = "Artificial Exception.";
        runGetOperationSchemaUntilOperationIsReadyOrError(
                () -> {
                    throw new SqlGatewayException(msg);
                },
                task ->
                        assertThatThrownBy(task::get)
                                .satisfies(anyCauseMatches(SqlGatewayException.class, msg)));
    }

    // --------------------------------------------------------------------------------------------

    private OperationHandle submitDefaultOperation(
            SessionHandle sessionHandle, RunnableWithException executor) {
        return service.submitOperation(
                sessionHandle,
                () -> {
                    executor.run();
                    return getDefaultResultSet();
                });
    }

    private ResultSet getDefaultResultSet() {
        List<RowData> data =
                Arrays.asList(
                        GenericRowData.ofKind(INSERT, 1L, StringData.fromString("Flink CDC"), 3),
                        GenericRowData.ofKind(INSERT, 2L, StringData.fromString("MySql"), null),
                        GenericRowData.ofKind(DELETE, 1, null, null),
                        GenericRowData.ofKind(UPDATE_AFTER, 2, null, 101));
        return new ResultSetImpl(
                PAYLOAD,
                null,
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT())),
                data,
                SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                false,
                null,
                SUCCESS_WITH_CONTENT);
    }

    private void runGetOperationSchemaUntilOperationIsReadyOrError(
            Callable<ResultSet> executor,
            ThrowingConsumer<FutureTask<ResolvedSchema>, Exception> validator)
            throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        CountDownLatch operationIsRunning = new CountDownLatch(1);
        CountDownLatch schemaFetcherIsRunning = new CountDownLatch(1);
        OperationHandle operationHandle =
                service.submitOperation(
                        sessionHandle,
                        () -> {
                            operationIsRunning.await();
                            return executor.call();
                        });
        FutureTask<ResolvedSchema> task =
                new FutureTask<>(
                        () -> {
                            schemaFetcherIsRunning.countDown();
                            return service.getOperationResultSchema(sessionHandle, operationHandle);
                        });

        EXECUTOR_EXTENSION.getExecutor().submit(task);

        schemaFetcherIsRunning.await();
        operationIsRunning.countDown();
        validator.accept(task);
    }

    private void runCancelOrCloseOperationWhenFetchResults(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            RunnableWithException cancelOrClose,
            Condition<String> condition) {

        List<RowData> actual = new ArrayList<>();

        EXECUTOR_EXTENSION
                .getExecutor()
                .submit(
                        () -> {
                            try {
                                cancelOrClose.run();
                            } catch (Exception e) {
                                // ignore
                            }
                        });

        assertThatThrownBy(
                        () -> {
                            Long token = 0L;
                            while (token != null) {
                                ResultSet resultSet =
                                        service.fetchResults(
                                                sessionHandle,
                                                operationHandle,
                                                token,
                                                Integer.MAX_VALUE);
                                // Keep fetching from the Operation until meet exceptions.
                                if (resultSet.getNextToken() != null) {
                                    token = resultSet.getNextToken();
                                }
                                if (resultSet.getResultType() == PAYLOAD) {
                                    actual.addAll(resultSet.getData());
                                }
                            }
                        })
                .satisfies(
                        t ->
                                assertThatChainOfCauses(t)
                                        .anySatisfy(t1 -> condition.matches(t1.getMessage())));

        assertThat(getDefaultResultSet().getData()).containsAll(actual);
    }

    private void validateStatementResult(
            SessionHandle sessionHandle, String statement, List<RowData> expected) {
        TableEnvironmentInternal tableEnv =
                service.getSession(sessionHandle).createExecutor().getTableEnvironment();
        assertThat(
                        CollectionUtil.iteratorToList(
                                ((TableResultInternal) tableEnv.executeSql(statement))
                                        .collectInternal()))
                .isEqualTo(expected);
    }

    private void validateCompletionHints(
            SessionHandle sessionHandle,
            String incompleteSql,
            List<String> expectedCompletionHints) {
        assertThat(service.completeStatement(sessionHandle, incompleteSql, incompleteSql.length()))
                .isEqualTo(expectedCompletionHints);
    }

    private List<RowData> fetchAllResults(
            SessionHandle sessionHandle, OperationHandle operationHandle) {
        Long token = 0L;
        List<RowData> results = new ArrayList<>();
        while (token != null) {
            ResultSet result =
                    service.fetchResults(sessionHandle, operationHandle, token, Integer.MAX_VALUE);
            results.addAll(result.getData());
            token = result.getNextToken();
        }
        return results;
    }
}
