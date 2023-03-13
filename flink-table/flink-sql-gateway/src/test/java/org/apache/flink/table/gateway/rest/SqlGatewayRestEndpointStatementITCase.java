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
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.AbstractSqlGatewayStatementITCase;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointUtils;
import org.apache.flink.table.gateway.rest.util.TestingRestClient;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.gateway.rest.util.TestingRestClient.getTestingRestClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in statement
 * related cases.
 */
public class SqlGatewayRestEndpointStatementITCase extends AbstractSqlGatewayStatementITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(SqlGatewayRestEndpointStatementITCase.class);

    @RegisterExtension
    @Order(3)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static TestingRestClient restClient;
    private static final ExecuteStatementHeaders executeStatementHeaders =
            ExecuteStatementHeaders.getInstance();
    private static SessionMessageParameters sessionMessageParameters;
    private static final FetchResultsHeaders fetchResultsHeaders =
            FetchResultsHeaders.getDefaultInstance();
    private static final int OPERATION_WAIT_SECONDS = 100;

    private static final String PATTERN1 = "Caused by: ";
    private static final String PATTERN2 = "\tat ";

    private final SessionEnvironment defaultSessionEnvironment =
            SessionEnvironment.newBuilder()
                    .setSessionEndpointVersion(MockedEndpointVersion.V1)
                    .build();

    private SessionHandle sessionHandle;

    @BeforeAll
    static void setup() throws Exception {
        restClient = getTestingRestClient();
    }

    @AfterAll
    static void cleanUp() throws Exception {
        restClient.shutdown();
    }

    @Parameters(name = "parameters={0}")
    public static List<TestParameters> parameters() throws Exception {
        return listFlinkSqlTests().stream()
                .flatMap(
                        path ->
                                Stream.of(
                                        new RestTestParameters(path, RowFormat.JSON),
                                        new RestTestParameters(path, RowFormat.PLAIN_TEXT)))
                .collect(Collectors.toList());
    }

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

        ResultInfo resultInfo = fetchResultsResponseBody.getResults();
        assertThat(resultInfo).isNotNull();

        ResultSet.ResultType resultType = fetchResultsResponseBody.getResultType();
        assertThat(Arrays.asList(ResultSet.ResultType.PAYLOAD, ResultSet.ResultType.EOS))
                .contains(resultType);

        ResolvedSchema resultSchema = resultInfo.getResultSchema();

        return toString(
                StatementType.match(statement),
                resultSchema,
                ((RestTestParameters) parameters).getRowFormat() == RowFormat.JSON
                        ? new RowDataToStringConverterImpl(
                                resultSchema.toPhysicalRowDataType(),
                                DateTimeUtils.UTC_ZONE.toZoneId(),
                                SqlGatewayRestEndpointStatementITCase.class.getClassLoader(),
                                false)
                        : SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                new RowDataIterator(sessionHandle, operationHandle));
    }

    FetchResultsResponseBody fetchResults(
            SessionHandle sessionHandle, OperationHandle operationHandle, Long token)
            throws Exception {
        FetchResultsMessageParameters fetchResultsMessageParameters =
                new FetchResultsMessageParameters(
                        sessionHandle,
                        operationHandle,
                        token,
                        ((RestTestParameters) parameters).getRowFormat());
        CompletableFuture<FetchResultsResponseBody> response =
                restClient.sendRequest(
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort(),
                        fetchResultsHeaders,
                        fetchResultsMessageParameters,
                        EmptyRequestBody.getInstance());
        return response.get();
    }

    @Override
    protected String stringifyException(Throwable t) {
        if (StringUtils.isNullOrWhitespaceOnly(t.getMessage())) {
            return t.getClass().getCanonicalName();
        }
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

    private static class RestTestParameters extends TestParameters {

        private final RowFormat rowFormat;

        public RestTestParameters(String sqlPath, RowFormat rowFormat) {
            super(sqlPath);
            this.rowFormat = rowFormat;
        }

        public RowFormat getRowFormat() {
            return rowFormat;
        }

        @Override
        public String toString() {
            return "RestTestParameters{"
                    + "sqlPath='"
                    + sqlPath
                    + '\''
                    + ", rowFormat="
                    + rowFormat
                    + '}';
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
                    LOG.error("Failed to fetch results.", ignored);
                }
            }

            return fetchedRows.hasNext();
        }

        @Override
        public RowData next() {
            return fetchedRows.next();
        }

        private void fetch() throws Exception {
            FetchResultsResponseBody fetchResultsResponseBody =
                    fetchResults(sessionHandle, operationHandle, token);

            token =
                    SqlGatewayRestEndpointUtils.parseToken(
                            fetchResultsResponseBody.getNextResultUri());
            fetchedRows = fetchResultsResponseBody.getResults().getData().iterator();
        }
    }
}
