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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.FetchOrientation;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.GatewayInfo;
import org.apache.flink.table.gateway.api.results.OperationInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.session.Session;
import org.apache.flink.table.gateway.service.session.SessionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/** The implementation of the {@link SqlGatewayService} interface. */
public class SqlGatewayServiceImpl implements SqlGatewayService {

    private static final Logger LOG = LoggerFactory.getLogger(SqlGatewayServiceImpl.class);

    private final SessionManager sessionManager;

    public SqlGatewayServiceImpl(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException {
        try {
            return sessionManager.openSession(environment).getSessionHandle();
        } catch (Throwable e) {
            LOG.error("Failed to openSession.", e);
            throw new SqlGatewayException("Failed to openSession.", e);
        }
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
        try {
            sessionManager.closeSession(sessionHandle);
        } catch (Throwable e) {
            LOG.error("Failed to closeSession.", e);
            throw new SqlGatewayException("Failed to closeSession.", e);
        }
    }

    @Override
    public void configureSession(
            SessionHandle sessionHandle, String statement, long executionTimeoutMs)
            throws SqlGatewayException {
        try {
            if (executionTimeoutMs > 0) {
                // TODO: support the feature in FLINK-27838
                throw new UnsupportedOperationException(
                        "SqlGatewayService doesn't support timeout mechanism now.");
            }

            OperationManager operationManager = getSession(sessionHandle).getOperationManager();
            OperationHandle operationHandle =
                    operationManager.submitOperation(
                            handle ->
                                    getSession(sessionHandle)
                                            .createExecutor()
                                            .configureSession(handle, statement));
            operationManager.awaitOperationTermination(operationHandle);
            operationManager.closeOperation(operationHandle);
        } catch (Throwable t) {
            LOG.error("Failed to configure session.", t);
            throw new SqlGatewayException("Failed to configure session.", t);
        }
    }

    @Override
    public Map<String, String> getSessionConfig(SessionHandle sessionHandle)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle).getSessionConfig();
        } catch (Throwable e) {
            LOG.error("Failed to getSessionConfig.", e);
            throw new SqlGatewayException("Failed to getSessionConfig.", e);
        }
    }

    @Override
    public EndpointVersion getSessionEndpointVersion(SessionHandle sessionHandle)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle).getEndpointVersion();
        } catch (Throwable e) {
            LOG.error("Failed to getSessionConfig.", e);
            throw new SqlGatewayException("Failed to getSessionEndpointVersion.", e);
        }
    }

    @Override
    public OperationHandle submitOperation(
            SessionHandle sessionHandle, Callable<ResultSet> executor) throws SqlGatewayException {
        try {
            return getSession(sessionHandle).getOperationManager().submitOperation(executor);
        } catch (Throwable e) {
            LOG.error("Failed to submitOperation.", e);
            throw new SqlGatewayException("Failed to submitOperation.", e);
        }
    }

    @Override
    public void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle) {
        try {
            getSession(sessionHandle).getOperationManager().cancelOperation(operationHandle);
        } catch (Throwable t) {
            LOG.error("Failed to cancelOperation.", t);
            throw new SqlGatewayException("Failed to cancelOperation.", t);
        }
    }

    @Override
    public void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle) {
        try {
            getSession(sessionHandle).getOperationManager().closeOperation(operationHandle);
        } catch (Throwable t) {
            LOG.error("Failed to closeOperation.", t);
            throw new SqlGatewayException("Failed to closeOperation.", t);
        }
    }

    @Override
    public OperationInfo getOperationInfo(
            SessionHandle sessionHandle, OperationHandle operationHandle) {
        try {
            return getSession(sessionHandle)
                    .getOperationManager()
                    .getOperationInfo(operationHandle);
        } catch (Throwable t) {
            LOG.error("Failed to getOperationInfo.", t);
            throw new SqlGatewayException("Failed to getOperationInfo.", t);
        }
    }

    @Override
    public ResolvedSchema getOperationResultSchema(
            SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle)
                    .getOperationManager()
                    .getOperationResultSchema(operationHandle);
        } catch (Throwable t) {
            LOG.error("Failed to getOperationResultSchema.", t);
            throw new SqlGatewayException("Failed to getOperationResultSchema.", t);
        }
    }

    @Override
    public OperationHandle executeStatement(
            SessionHandle sessionHandle,
            String statement,
            long executionTimeoutMs,
            Configuration executionConfig)
            throws SqlGatewayException {
        try {
            if (executionTimeoutMs > 0) {
                // TODO: support the feature in FLINK-27838
                throw new UnsupportedOperationException(
                        "SqlGatewayService doesn't support timeout mechanism now.");
            }

            return getSession(sessionHandle)
                    .getOperationManager()
                    .submitOperation(
                            handle ->
                                    getSession(sessionHandle)
                                            .createExecutor(executionConfig)
                                            .executeStatement(handle, statement));
        } catch (Throwable t) {
            LOG.error("Failed to execute statement.", t);
            throw new SqlGatewayException("Failed to execute statement.", t);
        }
    }

    @Override
    public ResultSet fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, long token, int maxRows)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle)
                    .getOperationManager()
                    .fetchResults(operationHandle, token, maxRows);
        } catch (Throwable t) {
            LOG.error("Failed to fetchResults.", t);
            throw new SqlGatewayException("Failed to fetchResults.", t);
        }
    }

    @Override
    public ResultSet fetchResults(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            FetchOrientation orientation,
            int maxRows) {
        try {
            return getSession(sessionHandle)
                    .getOperationManager()
                    .fetchResults(operationHandle, orientation, maxRows);
        } catch (Throwable t) {
            LOG.error("Failed to fetchResults.", t);
            throw new SqlGatewayException("Failed to fetchResults.", t);
        }
    }

    @Override
    public String getCurrentCatalog(SessionHandle sessionHandle) {
        return getSession(sessionHandle).createExecutor().getCurrentCatalog();
    }

    @Override
    public Set<String> listCatalogs(SessionHandle sessionHandle) throws SqlGatewayException {
        try {
            return getSession(sessionHandle).createExecutor().listCatalogs();
        } catch (Throwable t) {
            LOG.error("Failed to listCatalogs.", t);
            throw new SqlGatewayException("Failed to listCatalogs.", t);
        }
    }

    @Override
    public Set<String> listDatabases(SessionHandle sessionHandle, String catalogName) {
        try {
            return getSession(sessionHandle).createExecutor().listDatabases(catalogName);
        } catch (Throwable t) {
            LOG.error("Failed to listDatabases.", t);
            throw new SqlGatewayException("Failed to listDatabases.", t);
        }
    }

    @Override
    public Set<TableInfo> listTables(
            SessionHandle sessionHandle,
            String catalogName,
            String databaseName,
            Set<TableKind> tableKinds) {
        try {
            return getSession(sessionHandle)
                    .createExecutor()
                    .listTables(catalogName, databaseName, tableKinds);
        } catch (Throwable t) {
            LOG.error("Failed to listTables.", t);
            throw new SqlGatewayException("Failed to listTables.", t);
        }
    }

    @Override
    public ResolvedCatalogBaseTable<?> getTable(
            SessionHandle sessionHandle, ObjectIdentifier tableIdentifier)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle).createExecutor().getTable(tableIdentifier);
        } catch (Throwable t) {
            LOG.error("Failed to getTable.", t);
            throw new SqlGatewayException("Failed to getTable.", t);
        }
    }

    @Override
    public Set<FunctionInfo> listUserDefinedFunctions(
            SessionHandle sessionHandle, String catalogName, String databaseName)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle)
                    .createExecutor()
                    .listUserDefinedFunctions(catalogName, databaseName);
        } catch (Throwable t) {
            LOG.error("Failed to listUserDefinedFunctions.", t);
            throw new SqlGatewayException("Failed to listUserDefinedFunctions.", t);
        }
    }

    public Set<FunctionInfo> listSystemFunctions(SessionHandle sessionHandle) {
        try {
            return getSession(sessionHandle).createExecutor().listSystemFunctions();
        } catch (Throwable t) {
            LOG.error("Failed to listSystemFunctions.", t);
            throw new SqlGatewayException("Failed to listSystemFunctions.", t);
        }
    }

    @Override
    public FunctionDefinition getFunctionDefinition(
            SessionHandle sessionHandle, UnresolvedIdentifier functionIdentifier)
            throws SqlGatewayException {
        try {
            return getSession(sessionHandle)
                    .createExecutor()
                    .getFunctionDefinition(functionIdentifier);
        } catch (Throwable t) {
            LOG.error("Failed to getFunctionDefinition.", t);
            throw new SqlGatewayException("Failed to getFunctionDefinition.", t);
        }
    }

    @Override
    public GatewayInfo getGatewayInfo() {
        return GatewayInfo.INSTANCE;
    }

    @Override
    public List<String> completeStatement(
            SessionHandle sessionHandle, String statement, int position)
            throws SqlGatewayException {
        try {
            OperationManager operationManager = getSession(sessionHandle).getOperationManager();
            OperationHandle operationHandle =
                    operationManager.submitOperation(
                            handle ->
                                    getSession(sessionHandle)
                                            .createExecutor()
                                            .getCompletionHints(handle, statement, position));
            operationManager.awaitOperationTermination(operationHandle);

            ResultSet resultSet =
                    fetchResults(sessionHandle, operationHandle, 0, Integer.MAX_VALUE);
            return resultSet.getData().stream()
                    .map(data -> data.getString(0))
                    .map(StringData::toString)
                    .collect(Collectors.toList());
        } catch (Throwable t) {
            LOG.error("Failed to get statement completion candidates.", t);
            throw new SqlGatewayException("Failed to get statement completion candidates.", t);
        }
    }

    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public Session getSession(SessionHandle sessionHandle) {
        return sessionManager.getSession(sessionHandle);
    }
}
