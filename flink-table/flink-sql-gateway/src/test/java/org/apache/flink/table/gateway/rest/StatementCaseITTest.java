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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCase;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in statement
 * related cases.
 */
class SqlGatewayRestEndpointStatementITCase extends AbstractSqlGatewayStatementITCase {

    @RegisterExtension
    @Order(3)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static final RestClient restClient = getTestRestClient();
    private static final ExecuteStatementHeaders executeStatementHeaders =
            ExecuteStatementHeaders.getInstance();
    private static SessionMessageParameters sessionMessageParameters;
    private static final FetchResultsHeaders fetchResultsHeaders =
            FetchResultsHeaders.getInstance();
    private static final int OPERATION_WAIT_SECONDS = 100;

    private static final String PATTERN1 = "Caused by: ";
    private static final String PATTERN2 = "\tat ";

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();

    private SessionHandle sessionHandle;

    @BeforeEach
    @Override
    public void before(@TempDir Path temporaryFolder) throws Exception {
        super.before(temporaryFolder);
        sessionHandle = service.openSession(defaultSessionEnvironment);
        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
    }

    @Override
    protected String runSingleStatement(String statement) throws Exception {
        ExecuteStatementRequestBody executeStatementRequestBody =
                new ExecuteStatementRequestBody(statement, 0L, new HashMap<>());
        CompletableFuture<ExecuteStatementResponseBody> response =
                restClient.sendRequest(
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                        executeStatementHeaders,
                        sessionMessageParameters,
                        executeStatementRequestBody);
        ExecuteStatementResponseBody executeStatementResponseBody = response.get();
        String operationHandleString = executeStatementResponseBody.getOperationHandle();
        assertNotNull(operationHandleString);
        OperationHandle operationHandle =
                new OperationHandle(UUID.fromString(operationHandleString));
        assertDoesNotThrow(
                () ->
                        SQL_GATEWAY_SERVICE_EXTENSION
                                .getSessionManager()
                                .getSession(sessionHandle)
                                .getOperationManager()
                                .getOperation(operationHandle));

        CommonTestUtils.waitUtil(
                () ->
                        SQL_GATEWAY_SERVICE_EXTENSION
                                .getService()
                                .getOperationInfo(sessionHandle, operationHandle)
                                .getStatus()
                                .isTerminalStatus(),
                Duration.ofSeconds(OPERATION_WAIT_SECONDS),
                "Failed to wait operation finish.");

        FetchResultsResponseBody fetchResultsResponseBody =
                fetchResults(sessionHandle, operationHandle, 0L);

        ResultSet resultSet = fetchResultsResponseBody.getResults();
        String resultType = fetchResultsResponseBody.getResultType();
        assertThat(resultSet).isNotNull();
        assertThat(
                        Arrays.asList(
                                ResultSet.ResultType.PAYLOAD.name(),
                                ResultSet.ResultType.EOS.name()))
                .contains(resultType);

        return toString(
                StatementType.match(statement),
                resultSet.getResultSchema(),
                new RowDataToStringConverterImpl(
                        resultSet.getResultSchema().toPhysicalRowDataType(),
                        DateTimeUtils.UTC_ZONE.toZoneId(),
                        SqlGatewayRestEndpointStatementITCase.class.getClassLoader(),
                        false),
                new RowDataIterator(sessionHandle, operationHandle));
    }

    FetchResultsResponseBody fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, Long token)
            throws Exception {
        FetchResultsTokenParameters fetchResultsTokenParameters =
                new FetchResultsTokenParameters(sessionHandle, operationHandle, token);
        CompletableFuture<FetchResultsResponseBody> response =
                restClient.sendRequest(
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                        fetchResultsHeaders,
                        fetchResultsTokenParameters,
                        EmptyRequestBody.getInstance());
        return response.get();
    }

    @Override
    protected String stringifyException(Throwable t) {
        String message = t.getMessage();
        String[] splitExceptions = message.split(PATTERN1);
        return splitExceptions[splitExceptions.length - 1].split(PATTERN2)[0];
    }

    @Override
    protected boolean isStreaming() {
        return Configuration.fromMap(service.getSessionConfig(sessionHandle))
                .get(ExecutionOptions.RUNTIME_MODE)
                .equals(RuntimeExecutionMode.STREAMING);
    }

    private static RestClient getTestRestClient() {
        try {
            return new RestClient(
                    new Configuration(),
                    Executors.newFixedThreadPool(
                            1, new ExecutorThreadFactory("rest-client-thread-pool")));
        } catch (ConfigurationException e) {
            throw new SqlGatewayException("Cannot get rest client.", e);
        }
    }

    private class RowDataIterator implements Iterator<RowData> {

        private final SessionHandle sessionHandle;
        private final OperationHandle operationHandle;

        private Long token = 0L;
        private Iterator<RowData> fetchedRows = Collections.emptyIterator();

        public RowDataIterator(SessionHandle sessionHandle, OperationHandle operationHandle)
                throws Exception {
            this.sessionHandle = sessionHandle;
            this.operationHandle = operationHandle;
            fetch();
        }

        @Override
        public boolean hasNext() {
            while (token != null && !fetchedRows.hasNext()) {
                try {
                    fetch();
                } catch (Exception ignored) {
                }
            }

            return token != null;
        }

        @Override
        public RowData next() {
            return fetchedRows.next();
        }

        private void fetch() throws Exception {
            FetchResultsResponseBody fetchResultsResponseBody =
                    fetchResults(sessionHandle, operationHandle, token);
            String nextResultUri = fetchResultsResponseBody.getNextResultUri();
            ResultSet resultSet = fetchResultsResponseBody.getResults();
            token = parseTokenFromUri(nextResultUri);
            fetchedRows = resultSet.getData().iterator();
        }
    }

    private static Long parseTokenFromUri(String uri) {
        if (uri == null || uri.length() == 0) {
            return null;
        }
        String[] split = uri.split("/");
        return Long.valueOf(split[split.length - 1]);
    }
}
