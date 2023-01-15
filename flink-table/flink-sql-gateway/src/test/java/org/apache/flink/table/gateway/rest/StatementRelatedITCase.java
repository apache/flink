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

import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.session.ConfigureSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.statement.CompleteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.session.ConfigureSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.util.RowFormat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.NOT_READY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test basic logic of handlers inherited from {@link AbstractSqlGatewayRestHandler} in statement
 * related cases.
 */
public class StatementRelatedITCase extends RestAPIITCaseBase {

    private SessionHandle sessionHandle;
    private SessionMessageParameters sessionMessageParameters;
    private @TempDir Path tempDir;

    @BeforeEach
    public void setUp() throws Exception {
        CompletableFuture<OpenSessionResponseBody> response =
                sendRequest(
                        OpenSessionHeaders.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        new OpenSessionRequestBody(null, null));

        sessionHandle = new SessionHandle(UUID.fromString(response.get().getSessionHandle()));

        sessionMessageParameters = new SessionMessageParameters(sessionHandle);
    }

    @Test
    public void testCompleteStatement() throws Exception {
        CompletableFuture<CompleteStatementResponseBody> completeStatementResponse =
                sendRequest(
                        CompleteStatementHeaders.getINSTANCE(),
                        sessionMessageParameters,
                        new CompleteStatementRequestBody("CREATE TA", 9));

        assertThat(completeStatementResponse.get().getCandidates())
                .isEqualTo(Collections.singletonList("TABLE"));
    }

    @Test
    public void testFetchPlainTextResult() throws Exception {
        // SET 'table.dml-sync' = 'true';
        sendRequest(
                ConfigureSessionHeaders.getINSTANCE(),
                sessionMessageParameters,
                new ConfigureSessionRequestBody("SET 'table.dml-sync' = 'true';", null));

        // create table
        sendRequest(
                        ConfigureSessionHeaders.getINSTANCE(),
                        sessionMessageParameters,
                        new ConfigureSessionRequestBody(
                                "CREATE TABLE default_catalog.default_database.tbl (\n"
                                        + "  `k` STRING,\n"
                                        + "  `v` INT\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'filesystem',\n"
                                        + String.format(
                                                "  'path' = '%s',\n",
                                                tempDir.toFile().getAbsolutePath())
                                        + "  'format' = 'csv'\n"
                                        + ");",
                                -1L))
                .get();

        // insert value
        ExecuteStatementResponseBody executeStatementResponseBody =
                sendRequest(
                                ExecuteStatementHeaders.getInstance(),
                                sessionMessageParameters,
                                new ExecuteStatementRequestBody(
                                        "INSERT INTO tbl VALUES ('I', 1), ('Like', 4), ('Flink', 5), (CAST (NULL AS STRING), 0);",
                                        null,
                                        null))
                        .get();

        OperationHandle operationHandle =
                new OperationHandle(
                        UUID.fromString(executeStatementResponseBody.getOperationHandle()));

        FetchResultsResponseBody fetchResultsResponseBody =
                fetchFirst(sessionHandle, operationHandle, RowFormat.PLAIN_TEXT);

        assertThat(fetchResultsResponseBody.getJobID()).isNotNull();
        assertThat(fetchResultsResponseBody.getResultKind())
                .isEqualTo(ResultKind.SUCCESS_WITH_CONTENT.name());
        assertThat(fetchResultsResponseBody.isQueryResult()).isFalse();

        // do query
        executeStatementResponseBody =
                sendRequest(
                                ExecuteStatementHeaders.getInstance(),
                                sessionMessageParameters,
                                new ExecuteStatementRequestBody("SELECT * FROM tbl;", null, null))
                        .get();

        operationHandle =
                new OperationHandle(
                        UUID.fromString(executeStatementResponseBody.getOperationHandle()));

        fetchResultsResponseBody = fetchFirst(sessionHandle, operationHandle, RowFormat.PLAIN_TEXT);

        assertThat(fetchResultsResponseBody.getJobID()).isNotNull();
        assertThat(fetchResultsResponseBody.getResultKind())
                .isEqualTo(ResultKind.SUCCESS_WITH_CONTENT.name());
        assertThat(fetchResultsResponseBody.isQueryResult()).isTrue();

        List<RowData> data = new ArrayList<>(fetchResultsResponseBody.getResults().getData());

        Long nextToken = fetchResultsResponseBody.parseToken();
        while (nextToken != null) {
            fetchResultsResponseBody =
                    sendRequest(
                                    FetchResultsHeaders.getInstance(),
                                    new FetchResultsMessageParameters(
                                            sessionHandle,
                                            operationHandle,
                                            nextToken,
                                            RowFormat.PLAIN_TEXT),
                                    EmptyRequestBody.getInstance())
                            .get();

            data.addAll(fetchResultsResponseBody.getResults().getData());
            nextToken = fetchResultsResponseBody.parseToken();
        }

        // validate
        assertData(
                data,
                Arrays.asList(
                        new String[] {"I", "1"},
                        new String[] {"Like", "4"},
                        new String[] {"Flink", "5"},
                        new String[] {"", "0"}));
    }

    private FetchResultsResponseBody fetchFirst(
            SessionHandle sessionHandle, OperationHandle operationHandle, RowFormat rowFormat)
            throws Exception {
        FetchResultsResponseBody response;
        do {
            response =
                    sendRequest(
                                    FetchResultsHeaders.getInstance(),
                                    new FetchResultsMessageParameters(
                                            sessionHandle, operationHandle, 0L, rowFormat),
                                    EmptyRequestBody.getInstance())
                            .get();
        } while (response.getResultType().equals(NOT_READY.name()));

        return response;
    }

    private void assertData(List<RowData> actual, List<String[]> expected) {
        List<String[]> plainStrings =
                actual.stream()
                        .map(
                                rowData ->
                                        IntStream.range(0, rowData.getArity())
                                                .mapToObj(i -> rowData.getString(i).toString())
                                                .toArray(String[]::new))
                        .collect(Collectors.toList());

        int size = actual.size();
        assertThat(size).isEqualTo(expected.size());

        for (int i = 0; i < size; i++) {
            assertThat(plainStrings.get(i)).isEqualTo(expected.get(i));
        }
    }
}
