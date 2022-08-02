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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.planner.runtime.batch.sql.TestModule;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.PAYLOAD;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** ITCase for {@link SqlGatewayServiceImpl}. */
public class SqlGatewayServiceITCase extends AbstractTestBase {

    @RegisterExtension
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension();

    private static SessionManager sessionManager;
    private static SqlGatewayServiceImpl service;

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();
    private final ThreadFactory threadFactory =
            new ExecutorThreadFactory(
                    "SqlGatewayService Test Pool", IgnoreExceptionHandler.INSTANCE);

    @BeforeAll
    public static void setUp() {
        sessionManager = SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
        service = (SqlGatewayServiceImpl) SQL_GATEWAY_SERVICE_EXTENSION.getService();
    }

    @Test
    public void testOpenSessionWithConfig() {
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
    public void testOpenSessionWithEnvironment() throws Exception {
        String catalogName = "default";
        String databaseName = "testDb";
        String moduleName = "testModule";
        GenericInMemoryCatalog defaultCatalog = new GenericInMemoryCatalog(catalogName);
        defaultCatalog.createDatabase(
                databaseName, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        SessionEnvironment environment =
                SessionEnvironment.newBuilder()
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .registerCatalog(catalogName, defaultCatalog)
                        .registerModuleAtHead(moduleName, new TestModule())
                        .setDefaultCatalog(catalogName)
                        .setDefaultDatabase(databaseName)
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
    public void testFetchResultsInRunning() throws Exception {
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
        assertThat(service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE))
                .isEqualTo(ResultSet.NOT_READY_RESULTS);
        endRunningLatch.countDown();
    }

    @Test
    public void testGetOperationFinishedAndFetchResults() throws Exception {
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
                .isEqualTo(new OperationInfo(OperationStatus.RUNNING, OperationType.UNKNOWN));

        endRunningLatch.countDown();
        OperationInfo expectedInfo =
                new OperationInfo(OperationStatus.FINISHED, OperationType.UNKNOWN);

        CommonTestUtils.waitUtil(
                () -> service.getOperationInfo(sessionHandle, operationHandle).equals(expectedInfo),
                Duration.ofSeconds(10),
                "Failed to wait operation finish.");

        Long token = 0L;
        List<RowData> expectedData = getDefaultResultSet().getData();
        List<RowData> actualData = new ArrayList<>();
        while (token != null) {
            ResultSet currentResult =
                    service.fetchResults(sessionHandle, operationHandle, token, 1);
            actualData.addAll(checkNotNull(currentResult.getData()));
            token = currentResult.getNextToken();
        }
        assertThat(actualData).isEqualTo(expectedData);

        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    public void testCancelOperation() throws Exception {
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
                .isEqualTo(new OperationInfo(OperationStatus.RUNNING, OperationType.UNKNOWN));

        service.cancelOperation(sessionHandle, operationHandle);

        assertThat(service.getOperationInfo(sessionHandle, operationHandle))
                .isEqualTo(new OperationInfo(OperationStatus.CANCELED, OperationType.UNKNOWN));
        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    public void testOperationGetErrorAndFetchError() throws Exception {
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

        assertThatThrownBy(
                        () ->
                                service.fetchResults(
                                        sessionHandle, operationHandle, 0, Integer.MAX_VALUE))
                .satisfies(anyCauseMatches(SqlExecutionException.class, msg));

        service.closeOperation(sessionHandle, operationHandle);
        assertThat(sessionManager.getOperationCount(sessionHandle)).isEqualTo(0);
    }

    @Test
    public void testExecuteSqlWithConfig() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        String key = "username";
        String value = "Flink";
        OperationHandle operationHandle =
                service.executeStatement(
                        sessionHandle,
                        "SET",
                        -1,
                        Configuration.fromMap(Collections.singletonMap(key, value)));

        Long token = 0L;
        List<RowData> settings = new ArrayList<>();
        while (token != null) {
            ResultSet result =
                    service.fetchResults(sessionHandle, operationHandle, token, Integer.MAX_VALUE);
            settings.addAll(result.getData());
            token = result.getNextToken();
        }

        assertThat(settings)
                .contains(
                        GenericRowData.of(
                                StringData.fromString(key), StringData.fromString(value)));
    }

    @Test
    public void testGetOperationSchemaUntilOperationIsReady() throws Exception {
        runGetOperationSchemaUntilOperationIsReadyOrError(
                this::getDefaultResultSet,
                task -> assertThat(task.get()).isEqualTo(getDefaultResultSet().getResultSchema()));
    }

    // --------------------------------------------------------------------------------------------
    // Concurrent tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testCancelOperationAndFetchResultInParallel() {
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
    public void testCloseOperationAndFetchResultInParallel() {
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
    public void testCancelAndCloseOperationInParallel() throws Exception {
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
            threadFactory
                    .newThread(() -> service.cancelOperation(sessionHandle, operationHandle))
                    .start();
            threadFactory
                    .newThread(() -> service.closeOperation(sessionHandle, operationHandle))
                    .start();
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
    public void testSubmitOperationAndCloseOperationManagerInParallel1() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        OperationManager manager = service.getSession(sessionHandle).getOperationManager();
        int submitThreadsNum = 100;
        CountDownLatch latch = new CountDownLatch(submitThreadsNum);
        for (int i = 0; i < submitThreadsNum; i++) {
            threadFactory
                    .newThread(
                            () -> {
                                try {
                                    submitDefaultOperation(sessionHandle, () -> {});
                                } finally {
                                    latch.countDown();
                                }
                            })
                    .start();
        }
        manager.close();
        latch.await();
        assertThat(manager.getOperationCount()).isEqualTo(0);
    }

    @Test
    public void testSubmitOperationAndCloseOperationManagerInParallel2() throws Exception {
        int count = 3;
        CountDownLatch startRunning = new CountDownLatch(1);
        CountDownLatch terminateRunning = new CountDownLatch(1);
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        for (int i = 0; i < count; i++) {
            threadFactory
                    .newThread(
                            () ->
                                    service.submitOperation(
                                            sessionHandle,
                                            OperationType.UNKNOWN,
                                            () -> {
                                                startRunning.countDown();
                                                terminateRunning.await();
                                                return getDefaultResultSet();
                                            }))
                    .start();
        }
        startRunning.await();
        service.getSession(sessionHandle).getOperationManager().close();
        terminateRunning.countDown();
    }

    @Test
    public void testExecuteOperationInSequence() throws Exception {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);
        AtomicReference<Integer> v = new AtomicReference<>(0);

        int threadNum = 100;
        List<OperationHandle> handles = new ArrayList<>();

        for (int i = 0; i < threadNum; i++) {
            handles.add(
                    service.submitOperation(
                            sessionHandle,
                            OperationType.UNKNOWN,
                            () -> {
                                // If execute in parallel, the value of v may be overridden by
                                // another thread
                                int origin = v.get();
                                v.set(origin + 1);
                                return getDefaultResultSet();
                            }));
        }
        for (OperationHandle handle : handles) {
            CommonTestUtils.waitUtil(
                    () ->
                            service.getOperationInfo(sessionHandle, handle)
                                    .getStatus()
                                    .isTerminalStatus(),
                    Duration.ofSeconds(10),
                    "Failed to wait operation terminate");
        }

        assertThat(v.get()).isEqualTo(threadNum);
    }

    @Test
    public void testReleaseLockWhenFailedToSubmitOperation() throws Exception {
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
                            OperationType.UNKNOWN,
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
                                        OperationType.UNKNOWN,
                                        () -> {
                                            latch.await();
                                            return getDefaultResultSet();
                                        }))
                .satisfies(anyCauseMatches(RejectedExecutionException.class));
        latch.countDown();
        // Wait the first operation finishes
        CommonTestUtils.waitUtil(
                () ->
                        service.getOperationInfo(sessions.get(0), operations.get(0))
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(10),
                "Should come to end soon.");
        // Service is able to submit operation
        CountDownLatch success = new CountDownLatch(1);
        service.submitOperation(
                sessionHandle,
                OperationType.UNKNOWN,
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
    public void testFetchResultsFromCanceledOperation() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        CountDownLatch latch = new CountDownLatch(1);

        OperationHandle operationHandle = submitDefaultOperation(sessionHandle, latch::await);
        service.cancelOperation(sessionHandle, operationHandle);
        assertThatThrownBy(
                        () ->
                                service.fetchResults(
                                        sessionHandle, operationHandle, 0, Integer.MAX_VALUE))
                .satisfies(
                        anyCauseMatches(
                                String.format(
                                        "Can not fetch results from the %s in %s status.",
                                        operationHandle, OperationStatus.CANCELED)));
        latch.countDown();
    }

    @Test
    public void testRequestNonExistOperation() {
        SessionHandle sessionHandle = service.openSession(defaultSessionEnvironment);

        OperationHandle operationHandle = OperationHandle.create();
        List<RunnableWithException> requests =
                Arrays.asList(
                        () -> service.cancelOperation(sessionHandle, operationHandle),
                        () -> service.getOperationInfo(sessionHandle, operationHandle),
                        () ->
                                service.fetchResults(
                                        sessionHandle, operationHandle, 0, Integer.MAX_VALUE));

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
    public void testGetOperationSchemaWhenOperationGetError() throws Exception {
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
                OperationType.UNKNOWN,
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
        return new ResultSet(
                PAYLOAD,
                null,
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT())),
                data);
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
                        OperationType.UNKNOWN,
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
        threadFactory.newThread(task).start();

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
        threadFactory
                .newThread(
                        () -> {
                            try {
                                cancelOrClose.run();
                            } catch (Exception e) {
                                // ignore
                            }
                        })
                .start();

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
}
