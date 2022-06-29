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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
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
import org.apache.flink.table.gateway.service.operation.Operation;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.RunnableWithException;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        options.forEach(
                (k, v) ->
                        assertThat(
                                String.format(
                                        "Should contains (%s, %s) in the actual config.", k, v),
                                actualConfig,
                                Matchers.hasEntry(k, v)));
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
        assertEquals(
                ResultSet.NOT_READY_RESULTS,
                service.fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE));
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
        assertEquals(
                new OperationInfo(OperationStatus.RUNNING, OperationType.UNKNOWN, true),
                service.getOperationInfo(sessionHandle, operationHandle));

        endRunningLatch.countDown();
        OperationInfo expectedInfo =
                new OperationInfo(OperationStatus.FINISHED, OperationType.UNKNOWN, true);

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
        assertEquals(expectedData, actualData);

        service.closeOperation(sessionHandle, operationHandle);
        assertEquals(0, sessionManager.getOperationCount(sessionHandle));
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
        assertEquals(
                new OperationInfo(OperationStatus.RUNNING, OperationType.UNKNOWN, true),
                service.getOperationInfo(sessionHandle, operationHandle));

        service.cancelOperation(sessionHandle, operationHandle);

        assertEquals(
                new OperationInfo(OperationStatus.CANCELED, OperationType.UNKNOWN, true),
                service.getOperationInfo(sessionHandle, operationHandle));
        service.closeOperation(sessionHandle, operationHandle);
        assertEquals(0, sessionManager.getOperationCount(sessionHandle));
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

        Assertions.assertThatThrownBy(
                        () ->
                                service.fetchResults(
                                        sessionHandle, operationHandle, 0, Integer.MAX_VALUE))
                .satisfies(anyCauseMatches(SqlExecutionException.class, msg));

        service.closeOperation(sessionHandle, operationHandle);
        assertEquals(0, sessionManager.getOperationCount(sessionHandle));
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
        List<Operation> operations = new ArrayList<>(operationNum);
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
                "All operation should be closed.");

        for (Operation op : operations) {
            assertEquals(OperationStatus.CLOSED, op.getOperationInfo().getStatus());
        }
    }

    @Test
    public void testSubmitOperationAndCloseOperationManagerInParallel() throws Exception {
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
        assertEquals(0, manager.getOperationCount());
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testFetchResultsFromCanceledOperation() throws Exception {
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
                ResultSet.ResultType.PAYLOAD,
                null,
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT())),
                data);
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
                                if (resultSet.getResultType() == ResultSet.ResultType.PAYLOAD) {
                                    actual.addAll(resultSet.getData());
                                }
                            }
                        })
                .satisfies(
                        t ->
                                assertThatChainOfCauses(t)
                                        .anySatisfy(t1 -> condition.matches(t1.getMessage())));

        assertTrue(new HashSet<>(getDefaultResultSet().getData()).containsAll(actual));
    }
}
