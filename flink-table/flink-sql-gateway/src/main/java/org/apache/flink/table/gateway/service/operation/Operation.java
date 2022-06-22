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

import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.gateway.api.results.ResultSet.NOT_READY_RESULTS;

/** Operation to manage the execution, results and so on. */
public class Operation {

    private static final Logger LOG = LoggerFactory.getLogger(Operation.class);

    private final OperationHandle operationHandle;

    private final OperationType operationType;
    private final boolean hasResults;
    private final AtomicReference<OperationStatus> status;

    private final Callable<ResultFetcher> resultSupplier;

    private Future<?> invocation;
    private volatile ResultFetcher resultFetcher;
    private volatile SqlExecutionException operationError;

    public Operation(
            OperationHandle operationHandle,
            OperationType operationType,
            Callable<ResultFetcher> resultSupplier) {
        this.operationHandle = operationHandle;
        this.status = new AtomicReference<>(OperationStatus.INITIALIZED);
        this.operationType = operationType;
        this.hasResults = true;
        this.resultSupplier = resultSupplier;
    }

    void runBefore() {
        updateState(OperationStatus.RUNNING);
    }

    void runAfter() {
        updateState(OperationStatus.FINISHED);
    }

    public void run(ExecutorService service) {
        updateState(OperationStatus.PENDING);
        invocation =
                service.submit(
                        () -> {
                            try {
                                runBefore();
                                resultFetcher = resultSupplier.call();
                                runAfter();
                            } catch (Exception e) {
                                String msg =
                                        String.format(
                                                "Failed to execute the operation %s.",
                                                operationHandle);
                                LOG.error(msg, e);
                                operationError = new SqlExecutionException(msg, e);
                                updateState(OperationStatus.ERROR);
                            }
                        });
    }

    public void cancel() {
        updateState(OperationStatus.CANCELED);
        closeResources();
    }

    public void close() {
        updateState(OperationStatus.CLOSED);
        closeResources();
    }

    public ResultSet fetchResults(long token, int maxRows) {
        OperationStatus currentStatus = status.get();

        if (currentStatus == OperationStatus.ERROR) {
            throw operationError;
        } else if (currentStatus == OperationStatus.FINISHED) {
            return resultFetcher.fetchResults(token, maxRows);
        } else if (currentStatus == OperationStatus.RUNNING
                || currentStatus == OperationStatus.PENDING
                || currentStatus == OperationStatus.INITIALIZED) {
            return NOT_READY_RESULTS;
        } else {
            throw new SqlGatewayException(
                    String.format(
                            "Can not fetch results from the %s in %s status.",
                            operationHandle, currentStatus));
        }
    }

    public OperationInfo getOperationInfo() {
        return new OperationInfo(status.get(), operationType, hasResults);
    }

    private void updateState(OperationStatus toStatus) {
        OperationStatus currentStatus;
        do {
            currentStatus = status.get();
            boolean isValid = OperationStatus.isValidStatusTransition(currentStatus, toStatus);
            if (!isValid) {
                String message =
                        String.format(
                                "Failed to convert the Operation Status from %s to %s for %s.",
                                currentStatus, toStatus, operationHandle);
                LOG.error(message);
                throw new SqlGatewayException(message);
            }
        } while (!status.compareAndSet(currentStatus, toStatus));

        LOG.debug(
                String.format(
                        "Convert operation %s from %s to %s.",
                        operationHandle, currentStatus, toStatus));
    }

    private void closeResources() {
        if (invocation != null && !invocation.isDone()) {
            invocation.cancel(true);
        }

        if (resultFetcher != null) {
            resultFetcher.close();
        }
    }
}
