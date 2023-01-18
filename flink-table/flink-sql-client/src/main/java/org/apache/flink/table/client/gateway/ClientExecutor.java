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

package org.apache.flink.table.client.gateway;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.ConfigureSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.statement.CompleteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.ConfigureSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointUtils;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler.CLOSE_MESSAGE;

/** Client executor to connect to {@link SqlGateway} and execute statements. */
public class ClientExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(ClientExecutor.class);

    private final DefaultContext defaultContext;
    private final ExecutorService service;

    private RestClient restClient;
    private SessionHandle sessionHandle;
    private SessionMessageParameters sessionMessageParametersInstance;

    public ClientExecutor(DefaultContext defaultContext) {
        this.defaultContext = defaultContext;
        this.service = Executors.newFixedThreadPool(2);
    }

    public void openSession(@Nullable String sessionId) {
        try {
            restClient = new RestClient(defaultContext.getFlinkConfig(), service);
        } catch (Exception e) {
            throw new SqlExecutionException("Can not create the Rest Client.", e);
        }

        LOG.info("Open session to {}.", defaultContext.getGatewayAddress());
        // Open session to address:port and get the session handle ID
        OpenSessionRequestBody request =
                new OpenSessionRequestBody(sessionId, defaultContext.getFlinkConfig().toMap());
        try {
            OpenSessionResponseBody response =
                    sendRequest(
                                    OpenSessionHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    request)
                            .get();
            sessionHandle = new SessionHandle(UUID.fromString(response.getSessionHandle()));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to open session to %s", defaultContext.getGatewayAddress()),
                    e);
        }
        sessionMessageParametersInstance = new SessionMessageParameters(sessionHandle);
    }

    public void closeSession() throws SqlExecutionException {
        // close session
        try {
            CompletableFuture<CloseSessionResponseBody> response =
                    sendRequest(
                            CloseSessionHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            EmptyRequestBody.getInstance());

            if (!response.get().getStatus().equals(CLOSE_MESSAGE)) {
                LOG.warn("The status of close session response isn't {}.", CLOSE_MESSAGE);
            }
        } catch (Throwable t) {
            LOG.warn(
                    String.format(
                            "Unexpected error occurs when closing session %s.", sessionHandle),
                    t);
            // ignore any throwable to keep the cleanup running
        }
        service.shutdownNow();
    }

    public void configureSession(String statement) {
        try {
            sendRequest(
                            ConfigureSessionHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            new ConfigureSessionRequestBody(statement))
                    .get();
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to open session to %s", defaultContext.getGatewayAddress()),
                    e);
        }
    }

    public ReadableConfig getSessionConfig() {
        GetSessionConfigResponseBody response =
                getResponse(
                        sendRequest(
                                GetSessionConfigHeaders.getInstance(),
                                sessionMessageParametersInstance,
                                EmptyRequestBody.getInstance()));
        return Configuration.fromMap(response.getProperties());
    }

    public ClientResult executeStatement(String statement) {
        ExecuteStatementRequestBody request = new ExecuteStatementRequestBody(statement);
        CompletableFuture<ExecuteStatementResponseBody> executeStatementResponse =
                sendRequest(
                        ExecuteStatementHeaders.getInstance(),
                        sessionMessageParametersInstance,
                        request);
        OperationHandle operationHandle =
                new OperationHandle(
                        UUID.fromString(
                                getResponse(executeStatementResponse).getOperationHandle()));
        FetchResultsResponseBody fetchResultsResponse = fetchUtilResultsReady(operationHandle);
        ResultInfo firstResult = fetchResultsResponse.getResults();

        return new ClientResult(
                fetchResultsResponse.getResults().getResultSchema(),
                new RowDataInfoIterator(
                        operationHandle,
                        firstResult,
                        SqlGatewayRestEndpointUtils.parseToken(
                                fetchResultsResponse.getNextResultUri())),
                fetchResultsResponse.isQueryResult(),
                fetchResultsResponse.getResultKind(),
                fetchResultsResponse.getJobID());
    }

    /** Returns a list of completion hints for the given statement at the given position. */
    public List<String> completeStatement(String statement, int position) {
        return getResponse(
                        sendRequest(
                                CompleteStatementHeaders.getInstance(),
                                sessionMessageParametersInstance,
                                new CompleteStatementRequestBody(statement, position)))
                .getCandidates();
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request) {
        try {
            return restClient.sendRequest(
                    defaultContext.getGatewayAddress().getHostName(),
                    defaultContext.getGatewayAddress().getPort(),
                    messageHeaders,
                    messageParameters,
                    request);
        } catch (IOException ioException) {
            throw new SqlExecutionException("Failed to connect to the Sql Gateway.", ioException);
        }
    }

    private class RowDataInfoIterator implements CloseableIterator<RowData> {

        private final OperationHandle operationHandle;
        private Iterator<RowData> current;
        @Nullable private Long nextToken;

        public RowDataInfoIterator(
                OperationHandle operationHandle, ResultInfo resultInfo, @Nullable Long nextToken) {
            this.operationHandle = operationHandle;
            this.current = resultInfo.getData().iterator();
            this.nextToken = nextToken;
        }

        @Override
        public void close() throws Exception {
            sendRequest(
                    CloseOperationHeaders.getInstance(),
                    new OperationMessageParameters(sessionHandle, operationHandle),
                    EmptyRequestBody.getInstance());
        }

        @Override
        public boolean hasNext() {
            if (!current.hasNext()) {
                while (nextToken != null && !current.hasNext()) {
                    FetchResultsResponseBody fetchResultsResponseBody =
                            fetchResults(operationHandle, nextToken);
                    nextToken =
                            SqlGatewayRestEndpointUtils.parseToken(
                                    fetchResultsResponseBody.getNextResultUri());
                    current = fetchResultsResponseBody.getResults().getData().iterator();
                }
            }
            return current.hasNext();
        }

        @Override
        public RowData next() {
            return current.next();
        }
    }

    private FetchResultsResponseBody fetchResults(OperationHandle operationHandle) {
        return fetchResults(operationHandle, 0L);
    }

    public FetchResultsResponseBody fetchResults(OperationHandle operationHandle, long token) {
        FetchResultsMessageParameters fetchResultsParameters =
                new FetchResultsMessageParameters(
                        sessionHandle, operationHandle, token, RowFormat.PLAIN_TEXT);
        return getResponse(
                sendRequest(
                        FetchResultsHeaders.getDefaultInstance(),
                        fetchResultsParameters,
                        EmptyRequestBody.getInstance()));
    }

    @SuppressWarnings("BusyWait")
    private FetchResultsResponseBody fetchUtilResultsReady(OperationHandle operationHandle) {
        FetchResultsResponseBody response = fetchResults(operationHandle);
        while (response.getResultType().equals(ResultSet.ResultType.NOT_READY)) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new SqlExecutionException("The execution is interrupted.", e);
            }
            response = fetchResults(operationHandle);
        }
        return response;
    }

    private <T> T getResponse(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof RestClientException
                    && cause.getMessage().contains("Encountered \"<EOF>\"")) {
                throw new SqlParserEOFException(cause.getMessage(), cause);
            } else {
                throw new SqlExecutionException("Failed to get response.", cause);
            }
        } catch (InterruptedException e) {
            throw new SqlExecutionException("Interrupted.", e);
        }
    }
}
