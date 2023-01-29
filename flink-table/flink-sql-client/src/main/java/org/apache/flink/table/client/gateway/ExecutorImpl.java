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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.operation.CancelOperationHeaders;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.ConfigureSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.TriggerSessionHeartbeatHeaders;
import org.apache.flink.table.gateway.rest.header.statement.CompleteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.header.util.GetApiVersionHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.operation.OperationStatusResponseBody;
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
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointUtils;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler.CLOSE_MESSAGE;

/** Executor to connect to {@link SqlGateway} and execute statements. */
public class ExecutorImpl implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorImpl.class);
    private static final long HEARTBEAT_INTERVAL_MILLISECONDS = 60_000L;

    private final DefaultContext defaultContext;
    private final InetSocketAddress gatewayAddress;
    private final long heartbeatInterval;
    private final ExecutorService service;
    private final ScheduledExecutorService heartbeatScheduler;
    private final RestClient restClient;

    private SessionHandle sessionHandle;
    private SqlGatewayRestAPIVersion connectionVersion;

    @VisibleForTesting
    public ExecutorImpl(
            DefaultContext defaultContext,
            InetSocketAddress gatewayAddress,
            long heartbeatInterval) {
        this.defaultContext = defaultContext;
        this.gatewayAddress = gatewayAddress;
        this.heartbeatInterval = heartbeatInterval;
        this.service = Executors.newCachedThreadPool();
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            this.restClient = new RestClient(defaultContext.getFlinkConfig(), service);
        } catch (Exception e) {
            throw new SqlClientException("Can not create the Rest Client.", e);
        }
    }

    public ExecutorImpl(DefaultContext defaultContext, InetSocketAddress gatewayAddress) {
        this(defaultContext, gatewayAddress, HEARTBEAT_INTERVAL_MILLISECONDS);
    }

    public void openSession(@Nullable String sessionId) {
        try {
            // determine gateway rest api version
            connectionVersion = negotiateVersion();
            // open session to address:port
            LOG.info("Open session to {}.", gatewayAddress);
            OpenSessionResponseBody response =
                    sendRequest(
                                    OpenSessionHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    new OpenSessionRequestBody(
                                            sessionId, defaultContext.getFlinkConfig().toMap()))
                            .get();
            sessionHandle = new SessionHandle(UUID.fromString(response.getSessionHandle()));
            // register heartbeat service
            heartbeatScheduler.scheduleAtFixedRate(
                    () ->
                            getResponse(
                                    sendRequest(
                                            TriggerSessionHeartbeatHeaders.getInstance(),
                                            new SessionMessageParameters(sessionHandle),
                                            EmptyRequestBody.getInstance())),
                    heartbeatInterval,
                    heartbeatInterval,
                    TimeUnit.MILLISECONDS);
            // register dependencies
            defaultContext
                    .getDependencies()
                    .forEach(jar -> configureSession(String.format("ADD JAR '%s'", jar)));
        } catch (Exception e) {
            throw new SqlClientException(
                    String.format("Failed to open session to %s", gatewayAddress), e);
        }
    }

    public void closeSession() throws SqlExecutionException {
        if (sessionHandle == null) {
            return;
        }
        try {
            CompletableFuture<CloseSessionResponseBody> response =
                    sendRequest(
                            CloseSessionHeaders.getInstance(),
                            new SessionMessageParameters(sessionHandle),
                            EmptyRequestBody.getInstance());

            if (!response.get().getStatus().equals(CLOSE_MESSAGE)) {
                LOG.warn("The status of close session response isn't {}.", CLOSE_MESSAGE);
            }
        } catch (Exception e) {
            LOG.warn(
                    String.format(
                            "Unexpected error occurs when closing session %s.", sessionHandle),
                    e);
            // ignore any throwable to keep the cleanup running
        } finally {
            sessionHandle = null;
        }
    }

    public void configureSession(String statement) {
        try {
            sendRequest(
                            ConfigureSessionHeaders.getInstance(),
                            new SessionMessageParameters(sessionHandle),
                            new ConfigureSessionRequestBody(statement))
                    .get();
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format("Failed to open session to %s", gatewayAddress), e);
        }
    }

    public ReadableConfig getSessionConfig() {
        try {
            GetSessionConfigResponseBody response =
                    getResponse(
                            sendRequest(
                                    GetSessionConfigHeaders.getInstance(),
                                    new SessionMessageParameters(sessionHandle),
                                    EmptyRequestBody.getInstance()));
            return Configuration.fromMap(response.getProperties());
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to get the get session config.", e);
        }
    }

    public ClientResult executeStatement(String statement) {
        ExecuteStatementRequestBody request = new ExecuteStatementRequestBody(statement);
        CompletableFuture<ExecuteStatementResponseBody> executeStatementResponse =
                sendRequest(
                        ExecuteStatementHeaders.getInstance(),
                        new SessionMessageParameters(sessionHandle),
                        request);
        // It's possible that the execution is canceled during the submission.
        // Close the Operation in background to make sure the execution can continue.
        getResponse(
                executeStatementResponse,
                e -> {
                    service.submit(
                            () -> {
                                try {
                                    ExecuteStatementResponseBody executeStatementResponseBody =
                                            executeStatementResponse.get();
                                    // close operation in background to make sure users can not
                                    // interrupt the execution.
                                    closeOperationAsync(
                                            getOperationHandle(
                                                    executeStatementResponseBody
                                                            ::getOperationHandle));
                                } catch (Exception newException) {
                                    // ignore
                                }
                            });
                    return new SqlExecutionException("Interrupted to get response.", e);
                });

        OperationHandle operationHandle =
                getOperationHandle(
                        () -> getResponse(executeStatementResponse).getOperationHandle());
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

    public List<String> completeStatement(String statement, int position) {
        return getResponse(
                        sendRequest(
                                CompleteStatementHeaders.getInstance(),
                                new SessionMessageParameters(sessionHandle),
                                new CompleteStatementRequestBody(statement, position)))
                .getCandidates();
    }

    @Override
    public void close() {
        closeSession();
        service.shutdownNow();
        heartbeatScheduler.shutdownNow();
    }

    @VisibleForTesting
    public SessionHandle getSessionHandle() {
        return sessionHandle;
    }

    // --------------------------------------------------------------------------------------------

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
            getResponse(closeOperationAsync(operationHandle));
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

        private FetchResultsResponseBody fetchResults(OperationHandle operationHandle, long token) {
            return getFetchResultResponse(
                    operationHandle,
                    token,
                    true,
                    e -> {
                        sendRequest(
                                CancelOperationHeaders.getInstance(),
                                new OperationMessageParameters(sessionHandle, operationHandle),
                                EmptyRequestBody.getInstance());
                        return new SqlExecutionException("Interrupted to fetch results.", e);
                    });
        }
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request) {
        Preconditions.checkNotNull(connectionVersion, "The connection version should not be null.");
        return sendRequest(messageHeaders, messageParameters, request, connectionVersion);
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    SqlGatewayRestAPIVersion connectionVersion) {
        try {
            return restClient.sendRequest(
                    gatewayAddress.getHostName(),
                    gatewayAddress.getPort(),
                    messageHeaders,
                    messageParameters,
                    request,
                    Collections.emptyList(),
                    connectionVersion);
        } catch (IOException ioException) {
            throw new SqlExecutionException("Failed to connect to the SQL Gateway.", ioException);
        }
    }

    private FetchResultsResponseBody fetchUtilResultsReady(OperationHandle operationHandle) {
        FetchResultsResponseBody response;
        do {
            response =
                    getFetchResultResponse(
                            operationHandle,
                            0L,
                            false,
                            e -> {
                                // CliClient will not close the results. Try best to close it.
                                closeOperationAsync(operationHandle);
                                return new SqlExecutionException(
                                        "Interrupted to fetch results.", e);
                            });
        } while (response.getResultType().equals(ResultSet.ResultType.NOT_READY));
        return response;
    }

    private FetchResultsResponseBody getFetchResultResponse(
            OperationHandle operationHandle,
            long token,
            boolean fetchResultWithInterval,
            Function<InterruptedException, SqlExecutionException> interruptedExceptionHandler) {
        try {
            if (!fetchResultWithInterval) {
                Thread.sleep(100);
            }
            return sendRequest(
                            FetchResultsHeaders.getDefaultInstance(),
                            new FetchResultsMessageParameters(
                                    sessionHandle, operationHandle, token, RowFormat.PLAIN_TEXT),
                            EmptyRequestBody.getInstance())
                    .get();
        } catch (InterruptedException e) {
            throw interruptedExceptionHandler.apply(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RestClientException
                    && cause.getMessage().contains("Encountered \"<EOF>\"")) {
                throw new SqlParserEOFException(cause.getMessage(), cause);
            } else {
                throw new SqlExecutionException(
                        String.format(
                                "Failed to get response for the operation %s.", operationHandle),
                        cause);
            }
        }
    }

    private <T> T getResponse(CompletableFuture<T> future) {
        return getResponse(
                future, e -> new SqlExecutionException("Interrupted to get response.", e));
    }

    private <T> T getResponse(
            CompletableFuture<T> future,
            Function<InterruptedException, SqlExecutionException> interruptedExceptionHandler) {
        try {
            return future.get();
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            throw new SqlExecutionException("Failed to get response.", cause);
        } catch (InterruptedException e) {
            throw interruptedExceptionHandler.apply(e);
        }
    }

    private CompletableFuture<OperationStatusResponseBody> closeOperationAsync(
            OperationHandle operationHandle) {
        return sendRequest(
                CloseOperationHeaders.getInstance(),
                new OperationMessageParameters(sessionHandle, operationHandle),
                EmptyRequestBody.getInstance());
    }

    private OperationHandle getOperationHandle(Supplier<String> handleSupplier) {
        return new OperationHandle(UUID.fromString(handleSupplier.get()));
    }

    private SqlGatewayRestAPIVersion negotiateVersion() {
        List<SqlGatewayRestAPIVersion> gatewayVersions =
                getResponse(
                                sendRequest(
                                        GetApiVersionHeaders.getInstance(),
                                        EmptyMessageParameters.getInstance(),
                                        EmptyRequestBody.getInstance(),
                                        // Currently, RestClient always uses the latest REST API
                                        // version to build the targetUrl. However, it's possible
                                        // that the client REST API version is higher than the
                                        // server REST API version. In this case, the gateway will
                                        // report Not Found Error to notify the client.
                                        //
                                        // So, here use the lowest REST API version to get the
                                        // remote gateway version list and then determine the
                                        // connection version.
                                        // TODO: Remove this after the REST Client should allow
                                        // to build the target URL without API version.
                                        Collections.min(
                                                SqlGatewayRestAPIVersion.getStableVersions())))
                        .getVersions()
                        .stream()
                        .map(SqlGatewayRestAPIVersion::valueOf)
                        .collect(Collectors.toList());
        SqlGatewayRestAPIVersion clientVersion = SqlGatewayRestAPIVersion.getDefaultVersion();

        if (gatewayVersions.contains(clientVersion)) {
            return clientVersion;
        } else {
            SqlGatewayRestAPIVersion latestVersion =
                    RestAPIVersion.getLatestVersion(gatewayVersions);
            if (latestVersion.equals(SqlGatewayRestAPIVersion.V1)) {
                throw new SqlExecutionException(
                        "Currently, SQL Client only supports to connect to the REST endpoint with API version larger than V1.");
            }
            return latestVersion;
        }
    }
}
