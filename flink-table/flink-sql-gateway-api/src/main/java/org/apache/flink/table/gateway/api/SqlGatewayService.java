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
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.FetchOrientation;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.GatewayInfo;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
     * Using the statement to initialize the Session. It's only allowed to execute
     * SET/RESET/CREATE/DROP/USE/ALTER/LOAD MODULE/UNLOAD MODULE/ADD JAR.
     *
     * <p>It returns until the execution finishes.
     *
     * @param sessionHandle handle to identify the session.
     * @param statement the statement used to configure the session.
     * @param executionTimeoutMs the execution timeout. Please use non-positive value to forbid the
     *     timeout mechanism.
     */
    void configureSession(SessionHandle sessionHandle, String statement, long executionTimeoutMs)
            throws SqlGatewayException;

    /**
     * Get the current configuration of the {@code Session}.
     *
     * @param sessionHandle handle to identify the session.
     * @return Returns configuration of the session.
     */
    Map<String, String> getSessionConfig(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Get endpoint version that is negotiated in the openSession.
     *
     * @param sessionHandle handle to identify the session.
     * @return Returns the version.
     */
    EndpointVersion getSessionEndpointVersion(SessionHandle sessionHandle)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Operation Management
    // -------------------------------------------------------------------------------------------

    /**
     * Submit an operation and execute. The {@link SqlGatewayService} will take care of the
     * execution and assign the {@link OperationHandle} for later to retrieve the results.
     *
     * @param sessionHandle handle to identify the session.
     * @param executor the main logic to get the execution results.
     * @return Returns the handle for later retrieve results.
     */
    OperationHandle submitOperation(SessionHandle sessionHandle, Callable<ResultSet> executor)
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

    /**
     * Get the result schema for the specified Operation.
     *
     * <p>Note: The result schema is available when the Operation is in the {@link
     * OperationStatus#FINISHED}.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.
     */
    ResolvedSchema getOperationResultSchema(
            SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Statements API
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

    /**
     * Fetch the results from the operation. When maxRows is Integer.MAX_VALUE, it means to fetch
     * all available data. It promises to return at least one rows if the results is not
     * end-of-stream.
     *
     * @param sessionHandle handle to identify the session.
     * @param operationHandle handle to identify the operation.
     * @param orientation orientation to fetch the results.
     * @param maxRows max number of rows to fetch.
     * @return Returns the results.
     */
    ResultSet fetchResults(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            FetchOrientation orientation,
            int maxRows)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Catalog API
    // -------------------------------------------------------------------------------------------

    /**
     * Return current catalog name.
     *
     * @param sessionHandle handle to identify the session.
     * @return name of the current catalog.
     */
    String getCurrentCatalog(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Return all available catalogs in the current session.
     *
     * @param sessionHandle handle to identify the session.
     * @return names of the registered catalogs.
     */
    Set<String> listCatalogs(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Return all available schemas in the given catalog.
     *
     * @param sessionHandle handle to identify the session.
     * @param catalogName name string of the given catalog.
     * @return names of the registered schemas.
     */
    Set<String> listDatabases(SessionHandle sessionHandle, String catalogName)
            throws SqlGatewayException;

    /**
     * Return all available tables/views in the given catalog and database.
     *
     * @param sessionHandle handle to identify the session.
     * @param catalogName name of the given catalog.
     * @param databaseName name of the given database.
     * @param tableKinds used to specify the type of return values.
     * @return table info of the registered tables/views.
     */
    Set<TableInfo> listTables(
            SessionHandle sessionHandle,
            String catalogName,
            String databaseName,
            Set<TableKind> tableKinds)
            throws SqlGatewayException;

    /**
     * Return table of the given fully qualified name.
     *
     * @param sessionHandle handle to identify the session.
     * @param tableIdentifier fully qualified name of the table.
     * @return information of the table.
     */
    ResolvedCatalogBaseTable<?> getTable(
            SessionHandle sessionHandle, ObjectIdentifier tableIdentifier)
            throws SqlGatewayException;

    /**
     * List all user defined functions.
     *
     * @param sessionHandle handle to identify the session.
     * @param catalogName name string of the given catalog.
     * @param databaseName name string of the given database.
     * @return user defined functions info.
     */
    Set<FunctionInfo> listUserDefinedFunctions(
            SessionHandle sessionHandle, String catalogName, String databaseName)
            throws SqlGatewayException;

    /**
     * List all available system functions.
     *
     * @param sessionHandle handle to identify the session.
     * @return system functions info.
     */
    Set<FunctionInfo> listSystemFunctions(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Get the specific definition of the function. If the input identifier only contains the
     * function name, it is resolved with the order of the temporary system function, system
     * function, temporary function and catalog function.
     *
     * @param sessionHandle handle to identify the session.
     * @param functionIdentifier identifier of the function.
     * @return the definition of the function.
     */
    FunctionDefinition getFunctionDefinition(
            SessionHandle sessionHandle, UnresolvedIdentifier functionIdentifier)
            throws SqlGatewayException;

    // -------------------------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------------------------

    /**
     * Get the info about the {@link SqlGatewayService}.
     *
     * @return Returns gateway info.
     */
    GatewayInfo getGatewayInfo();

    /**
     * Returns a list of completion hints for the given statement at the given position.
     *
     * @param sessionHandle handle to identify the session.
     * @param statement sql statement to be completed.
     * @param position position of where need completion hints.
     * @return completion hints.
     */
    List<String> completeStatement(SessionHandle sessionHandle, String statement, int position)
            throws SqlGatewayException;
}
