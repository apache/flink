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

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlCancelException;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.PAYLOAD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link OperationManager}. */
class OperationManagerTest {

    private static OperationManager operationManager;
    private static ResultSet defaultResultSet;

    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "SqlGatewayService Test Pool",
                                            IgnoreExceptionHandler.INSTANCE)));

    @BeforeEach
    void setUp() {
        operationManager = new OperationManager(EXECUTOR_EXTENSION.getExecutor());
        defaultResultSet =
                new ResultSetImpl(
                        PAYLOAD,
                        1L,
                        ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())),
                        Collections.singletonList(GenericRowData.of(1L)),
                        SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                        false,
                        null,
                        ResultKind.SUCCESS_WITH_CONTENT);
    }

    @AfterEach
    void cleanEach() {
        operationManager.close();
    }

    @Test
    void testRunOperationAsynchronously() throws Exception {
        OperationHandle operationHandle = operationManager.submitOperation(() -> defaultResultSet);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isNotEqualTo(OperationStatus.ERROR);

        assertThat(operationManager.getOperationResultSchema(operationHandle))
                .isEqualTo(ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.FINISHED);
    }

    @Test
    void testRunOperationSynchronously() throws Exception {
        OperationHandle operationHandle = operationManager.submitOperation(() -> defaultResultSet);
        operationManager.awaitOperationTermination(operationHandle);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.FINISHED);

        assertThat(operationManager.fetchResults(operationHandle, 0, Integer.MAX_VALUE))
                .isEqualTo(defaultResultSet);
    }

    @Test
    void testCancelOperation() throws Exception {
        CountDownLatch endRunningLatch = new CountDownLatch(1);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            endRunningLatch.await();
                            return defaultResultSet;
                        });

        EXECUTOR_EXTENSION
                .getExecutor()
                .submit(() -> operationManager.cancelOperation(operationHandle));
        operationManager.awaitOperationTermination(operationHandle);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.CANCELED);
    }

    @Test
    void testCancelUninterruptedOperation() throws Exception {
        AtomicReference<Boolean> isRunning = new AtomicReference<>(false);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            // mock cpu busy task that doesn't interrupt system call
                            while (true) {
                                isRunning.compareAndSet(false, true);
                            }
                        });
        CommonTestUtils.waitUtil(
                isRunning::get, Duration.ofSeconds(10), "Failed to start up the task.");
        assertThatThrownBy(() -> operationManager.cancelOperation(operationHandle))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlCancelException.class,
                                String.format(
                                        "Operation '%s' did not react to \"Future.cancel(true)\" and "
                                                + "is stuck for %s seconds in method.\n",
                                        operationHandle, 5)));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.CANCELED);
    }

    @Test
    void testCloseUninterruptedOperation() throws Exception {
        AtomicReference<Boolean> isRunning = new AtomicReference<>(false);
        for (int i = 0; i < 10; i++) {
            EXECUTOR_EXTENSION
                    .getExecutor()
                    .submit(
                            () -> {
                                operationManager.submitOperation(
                                        () -> {
                                            // mock cpu busy task that doesn't interrupt system call
                                            while (true) {
                                                isRunning.compareAndSet(false, true);
                                            }
                                        });
                            });
        }
        CommonTestUtils.waitUtil(
                isRunning::get, Duration.ofSeconds(10), "Failed to start up the task.");

        assertThatThrownBy(() -> operationManager.close())
                .satisfies(FlinkAssertions.anyCauseMatches(SqlCancelException.class));
        assertThat(operationManager.getOperationCount()).isEqualTo(0);
    }

    @Test
    void testCloseOperation() {
        CountDownLatch endRunningLatch = new CountDownLatch(1);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            endRunningLatch.await();
                            return defaultResultSet;
                        });

        EXECUTOR_EXTENSION
                .getExecutor()
                .submit(() -> operationManager.closeOperation(operationHandle));

        assertThatThrownBy(
                        () -> {
                            operationManager.awaitOperationTermination(operationHandle);
                            operationManager.getOperation(operationHandle);
                        })
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlGatewayException.class,
                                String.format(
                                        "Can not find the submitted operation in the OperationManager with the %s.",
                                        operationHandle)));
    }

    @Test
    void testRunOperationSynchronouslyWithError() {
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            throw new SqlExecutionException("Execution error.");
                        });

        assertThatThrownBy(() -> operationManager.awaitOperationTermination(operationHandle))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlExecutionException.class, "Execution error."));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.ERROR);
    }
}
