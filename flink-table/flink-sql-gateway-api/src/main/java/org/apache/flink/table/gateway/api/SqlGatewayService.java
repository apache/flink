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

package org.apache.flink.table.gateway.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationType;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;

import java.util.Map;
import java.util.concurrent.Callable;

/** A service of SQL gateway is responsible for handling requests from the endpoints. */
@PublicEvolving
public interface SqlGatewayService {

    // -------------------------------------------------------------------------------------------
    // Session Management
    // -------------------------------------------------------------------------------------------

    /**
     * Open the {@code Session}.
     *
     * @param environment Environment to initialize the Session.
     * @return Returns a handle that used to identify the Session.
     */
    SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException;

    /**
     * Close the {@code Session}.
     *
     * @param sessionHandle handle to identify the Session needs to be closed.
     */
    void closeSession(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Get the current configuration of the {@code Session}.
     *
     * @param sessionHandle handle to identify the session.
     * @return Returns configuration of the session.
     */
    Map<String, String> getSessionConfig(SessionHandle sessionHandle) throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Operation Management
    // -------------------------------------------------------------------------------------------

    /**
     * Submit an operation and execute. The {@link SqlGatewayService} will take care of the
     * execution and assign the {@link OperationHandle} for later to retrieve the results.
     *
     * @param sessionHandle handle to identify the session.
     * @param type describe the operation type.
     * @param executor the main logic to get the execution results.
     * @return Returns the handle for later retrieve results.
     */
    OperationHandle submitOperation(
            SessionHandle sessionHandle, OperationType type, Callable<ResultSet> executor)
            throws SqlGatewayException;

    /**
     * Cancel the operation when it is not in terminal status.
     *
     * <p>It can't cancel an Operation if it is terminated.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.JarURLConnection
     */
    void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    /**
     * Close the operation and release all used resource by the operation.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.
     */
    void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    /**
     * Get the {@link OperationInfo} of the operation.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.
     */
    OperationInfo getOperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Statements
    // -------------------------------------------------------------------------------------------

    /**
     * Execute the submitted statement.
     *
     * @param sessionHandle handle to identify the session.
     * @param statement the SQL to execute.
     * @param executionTimeoutMs the execution timeout. Please use non-positive value to forbid the
     *     timeout mechanism.
     * @param executionConfig execution config for the statement.
     * @return handle to identify the operation.
     */
    OperationHandle executeStatement(
            SessionHandle sessionHandle,
            String statement,
            long executionTimeoutMs,
            Configuration executionConfig)
            throws SqlGatewayException;

    /**
     * Fetch the results from the operation. When maxRows is Integer.MAX_VALUE, it means to fetch
     * all available data.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.
     * @param token token to identify results.
     * @param maxRows max number of rows to fetch.
     * @return Returns the results.
     */
    ResultSet fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, long token, int maxRows)
            throws SqlGatewayException;
}
