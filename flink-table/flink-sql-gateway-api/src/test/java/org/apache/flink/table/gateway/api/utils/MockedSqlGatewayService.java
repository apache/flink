/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.gateway.api.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/** Mocked {@link SqlGatewayService}. */
public class MockedSqlGatewayService implements SqlGatewayService {

    @Override
    public SessionHandle openSession(SessionEnvironment environment) throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    public void configureSession(
            SessionHandle sessionHandle, String statement, long executionTimeoutMs)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getSessionConfig(SessionHandle sessionHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public EndpointVersion getSessionEndpointVersion(SessionHandle sessionHandle)
            throws SqlGatewayException {
        return MockedEndpointVersion.V1;
    }

    @Override
    public OperationHandle submitOperation(
            SessionHandle sessionHandle, Callable<ResultSet> executor) throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeOperation(SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, long token, int maxRows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet fetchResults(
            SessionHandle sessionHandle,
            OperationHandle operationHandle,
            FetchOrientation orientation,
            int maxRows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationInfo getOperationInfo(
            SessionHandle sessionHandle, OperationHandle operationHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationHandle executeStatement(
            SessionHandle sessionHandle,
            String statement,
            long executionTimeoutMs,
            Configuration executionConfig)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedSchema getOperationResultSchema(
            SessionHandle sessionHandle, OperationHandle operationHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentCatalog(SessionHandle sessionHandle) throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listCatalogs(SessionHandle sessionHandle) throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listDatabases(SessionHandle sessionHandle, String catalogName)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TableInfo> listTables(
            SessionHandle sessionHandle,
            String catalogName,
            String databaseName,
            Set<TableKind> tableKinds)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<FunctionInfo> listUserDefinedFunctions(
            SessionHandle sessionHandle, String catalogName, String databaseName)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<FunctionInfo> listSystemFunctions(SessionHandle sessionHandle)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionDefinition getFunctionDefinition(
            SessionHandle sessionHandle, UnresolvedIdentifier functionIdentifier)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GatewayInfo getGatewayInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> completeStatement(
            SessionHandle sessionHandle, String statement, int position)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResolvedCatalogBaseTable<?> getTable(
            SessionHandle sessionHandle, ObjectIdentifier tableIdentifier)
            throws SqlGatewayException {
        throw new UnsupportedOperationException();
    }
}
