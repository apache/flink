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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/** Manager for the {@link Operation}. */
public class OperationManager {

    private static final Logger LOG = LoggerFactory.getLogger(OperationManager.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<OperationHandle, Operation> submittedOperations;
    private final ExecutorService service;

    private boolean isRunning;

    public OperationManager(ExecutorService service) {
        this.service = service;
        this.submittedOperations = new HashMap<>();
        this.isRunning = true;
    }

    /**
     * Submit the operation to the {@link OperationManager}. The {@link OperationManager} manges the
     * lifecycle of the {@link Operation}, including register resources, fire the execution and so
     * on.
     *
     * @param operationType The type of the submitted operation.
     * @param executor Worker to execute.
     * @return OperationHandle to fetch the results or check the status.
     */
    public OperationHandle submitOperation(
            OperationType operationType, Callable<ResultSet> executor) {
        OperationHandle handle = OperationHandle.create();
        Operation operation =
                new Operation(
                        handle,
                        operationType,
                        () -> {
                            ResultSet resultSet = executor.call();
                            List<RowData> rows = resultSet.getData();
                            return new ResultFetcher(
                                    handle,
                                    resultSet.getResultSchema(),
                                    CloseableIterator.adapterForIterator(rows.iterator()),
                                    rows.size());
                        });

        writeLock(
                () -> {
                    submittedOperations.put(handle, operation);
                    operation.run(service);
                });
        return handle;
    }

    /**
     * Cancel the execution of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public void cancelOperation(OperationHandle operationHandle) {
        getOperation(operationHandle).cancel();
    }

    /**
     * Close the operation and release all resources used by the {@link Operation}.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public void closeOperation(OperationHandle operationHandle) {
        writeLock(
                () -> {
                    Operation opToRemove = submittedOperations.remove(operationHandle);
                    if (opToRemove != null) {
                        opToRemove.close();
                    }
                });
    }

    /**
     * Get the {@link OperationInfo} of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     */
    public OperationInfo getOperationInfo(OperationHandle operationHandle) {
        return getOperation(operationHandle).getOperationInfo();
    }

    /**
     * Get the results of the operation.
     *
     * @param operationHandle identifies the {@link Operation}.
     * @param token identifies which batch of data to fetch.
     * @param maxRows the maximum number of rows to fetch.
     * @return ResultSet contains the results.
     */
    public ResultSet fetchResults(OperationHandle operationHandle, long token, int maxRows) {
        return getOperation(operationHandle).fetchResults(token, maxRows);
    }

    /** Closes the {@link OperationManager} and all operations. */
    public void close() {
        lock.writeLock().lock();
        try {
            isRunning = false;
            for (Operation operation : submittedOperations.values()) {
                operation.close();
            }
            submittedOperations.clear();
        } finally {
            lock.writeLock().unlock();
        }
        LOG.debug("Closes the Operation Manager.");
    }

    // -------------------------------------------------------------------------------------------

    @VisibleForTesting
    public int getOperationCount() {
        return submittedOperations.size();
    }

    @VisibleForTesting
    public Operation getOperation(OperationHandle operationHandle) {
        return readLock(
                () -> {
                    Operation operation = submittedOperations.get(operationHandle);
                    if (operation == null) {
                        throw new SqlGatewayException(
                                String.format(
                                        "Can not find the submitted operation in the OperationManager with the %s.",
                                        operationHandle));
                    }
                    return operation;
                });
    }

    private void writeLock(Runnable runner) {
        lock.writeLock().lock();
        try {
            if (!isRunning) {
                throw new SqlGatewayException("The OperationManager is closed.");
            }
            runner.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private <T> T readLock(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            if (!isRunning) {
                throw new SqlGatewayException("The OperationManager is closed.");
            }
            return supplier.get();
        } finally {
            lock.readLock().unlock();
        }
    }
}
