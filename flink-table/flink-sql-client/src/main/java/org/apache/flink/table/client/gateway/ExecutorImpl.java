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
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.rest.UrlPrefixDecorator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.CustomHeadersDecorator;
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
import org.apache.flink.table.gateway.rest.message.util.GetApiVersionResponseBody;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointUtils;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;

/** Executor to connect to {@link SqlGateway} and execute statements. */
public class ExecutorImpl implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorImpl.class);
    private static final long HEARTBEAT_INTERVAL_MILLISECONDS = 60_000L;

    private final AutoCloseableRegistry registry;
    private final URL gatewayUrl;

    private final ExecutorService executorService;
    private final RestClient restClient;

    private final SqlGatewayRestAPIVersion connectionVersion;
    private final SessionHandle sessionHandle;
    private final RowFormat rowFormat;
    private final Collection<HttpHeader> customHttpHeaders;

    public ExecutorImpl(
            DefaultContext defaultContext, InetSocketAddress gatewayAddress, String sessionId) {
        this(
                defaultContext,
                NetUtils.socketToUrl(gatewayAddress),
                sessionId,
                HEARTBEAT_INTERVAL_MILLISECONDS,
                RowFormat.PLAIN_TEXT);
    }

    public ExecutorImpl(
            DefaultContext defaultContext,
            InetSocketAddress gatewayAddress,
            String sessionId,
            RowFormat rowFormat) {
        this(
                defaultContext,
                NetUtils.socketToUrl(gatewayAddress),
                sessionId,
                HEARTBEAT_INTERVAL_MILLISECONDS,
                rowFormat);
    }

    public ExecutorImpl(DefaultContext defaultContext, URL gatewayUrl, String sessionId) {
        this(
                defaultContext,
                gatewayUrl,
                sessionId,
                HEARTBEAT_INTERVAL_MILLISECONDS,
                RowFormat.PLAIN_TEXT);
    }

    @VisibleForTesting
    ExecutorImpl(
            DefaultContext defaultContext,
            InetSocketAddress gatewayAddress,
            String sessionId,
            long heartbeatInterval) {
        this(
                defaultContext,
                NetUtils.socketToUrl(gatewayAddress),
                sessionId,
                heartbeatInterval,
                RowFormat.PLAIN_TEXT);
    }

    @VisibleForTesting
    ExecutorImpl(
            DefaultContext defaultContext,
            URL gatewayUrl,
            String sessionId,
            long heartbeatInterval,
            RowFormat rowFormat) {
        this.registry = new AutoCloseableRegistry();
        this.gatewayUrl = gatewayUrl;
        this.rowFormat = rowFormat;
        this.customHttpHeaders =
                ClientUtils.readHeadersFromEnvironmentVariable(
                        ConfigConstants.FLINK_REST_CLIENT_HEADERS);
        try {
            // register required resource
            this.executorService = Executors.newCachedThreadPool();
            registry.registerCloseable(executorService::shutdownNow);
            Configuration flinkConfig = defaultContext.getFlinkConfig();

            this.restClient = RestClient.forUrl(flinkConfig, executorService, gatewayUrl);
            registry.registerCloseable(restClient);

            // determine gateway rest api version
            this.connectionVersion = negotiateVersion();

            // register session
            LOG.info(
                    "Open session to {} with connection version: {}.",
                    gatewayUrl,
                    connectionVersion);
            OpenSessionResponseBody response =
                    sendRequest(
                                    OpenSessionHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    new OpenSessionRequestBody(sessionId, flinkConfig.toMap()))
                            .get();
            this.sessionHandle = new SessionHandle(UUID.fromString(response.getSessionHandle()));
            registry.registerCloseable(this::closeSession);

            // heartbeat
            ScheduledExecutorService heartbeatScheduler =
                    Executors.newSingleThreadScheduledExecutor();
            registry.registerCloseable(heartbeatScheduler::shutdownNow);
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
        } catch (Exception e) {
            try {
                registry.close();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }

            throw new SqlClientException("Failed to create the executor.", e);
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
                    String.format("Failed to open session to %s", gatewayUrl), e);
        }
    }

    public ReadableConfig getSessionConfig() {
        try {
            return Configuration.fromMap(getSessionConfigMap());
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to get the get session config.", e);
        }
    }

    public Map<String, String> getSessionConfigMap() {
        try {
            GetSessionConfigResponseBody response =
                    getResponse(
                            sendRequest(
                                    GetSessionConfigHeaders.getInstance(),
                                    new SessionMessageParameters(sessionHandle),
                                    EmptyRequestBody.getInstance()));
            return response.getProperties();
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to get the get session config.", e);
        }
    }

    public StatementResult executeStatement(String statement) {
        ExecuteStatementRequestBody request = new ExecuteStatementRequestBody(statement);
        CompletableFuture<ExecuteStatementResponseBody> executeStatementResponse =
                sendRequest(
                        ExecuteStatementHeaders.getInstance(),
                        new SessionMessageParameters(sessionHandle),
                        request);

        // It's possible that the execution is canceled during the submission.
        // Close the Operation in background to make sure the execution can continue.
        OperationHandle operationHandle =
                getOperationHandle(
                        () ->
                                getResponse(
                                                executeStatementResponse,
                                                e -> {
                                                    executorService.submit(
                                                            () -> {
                                                                try {
                                                                    ExecuteStatementResponseBody
                                                                            executeStatementResponseBody =
                                                                                    executeStatementResponse
                                                                                            .get();
                                                                    // close operation in background
                                                                    // to make sure users can not
                                                                    // interrupt the execution.
                                                                    closeOperationAsync(
                                                                            getOperationHandle(
                                                                                    executeStatementResponseBody
                                                                                            ::getOperationHandle));
                                                                } catch (Exception newException) {
                                                                    e.addSuppressed(newException);
                                                                    LOG.error(
                                                                            "Failed to cancel the interrupted exception.",
                                                                            e);
                                                                }
                                                            });
                                                    return new SqlExecutionException(
                                                            "Interrupted to get response.", e);
                                                })
                                        .getOperationHandle());
        FetchResultsResponseBody fetchResultsResponse = fetchUtilResultsReady(operationHandle);
        ResultInfo firstResult = fetchResultsResponse.getResults();

        return new StatementResult(
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
        if (!registry.isClosed()) {
            try {
                registry.close();
            } catch (Throwable t) {
                Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
                LOG.error("Exception happens when closing the Executor.", firstOrSuppressed(e, t));
            }
        }
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
                    false,
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
        CustomHeadersDecorator<R, P, U> headers =
                new CustomHeadersDecorator<>(
                        new UrlPrefixDecorator<>(messageHeaders, gatewayUrl.getPath()));
        headers.setCustomHeaders(customHttpHeaders);

        return sendRequest(headers, messageParameters, request, connectionVersion);
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
                    gatewayUrl.getHost(),
                    gatewayUrl.getPort(),
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
                            true,
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
            if (fetchResultWithInterval) {
                Thread.sleep(100);
            }
            return sendRequest(
                            FetchResultsHeaders.getDefaultInstance(),
                            new FetchResultsMessageParameters(
                                    sessionHandle, operationHandle, token, rowFormat),
                            EmptyRequestBody.getInstance())
                    .get();
        } catch (InterruptedException e) {
            throw interruptedExceptionHandler.apply(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RestClientException
                    && cause.getMessage().contains("Encountered \"<EOF>\"")) {
                throw new SqlExecutionException(
                        "The SQL statement is incomplete.",
                        new SqlParserEOFException(cause.getMessage(), cause));
            } else {
                throw new SqlExecutionException(
                        String.format(
                                "Failed to get response for the operation %s.", operationHandle),
                        cause);
            }
        }
    }

    private static <T> T getResponse(CompletableFuture<T> future) {
        return getResponse(
                future, e -> new SqlExecutionException("Interrupted to get response.", e));
    }

    private static <T> T getResponse(
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

    private SqlGatewayRestAPIVersion negotiateVersion() throws Exception {

        CustomHeadersDecorator<EmptyRequestBody, GetApiVersionResponseBody, EmptyMessageParameters>
                headers =
                        new CustomHeadersDecorator<>(
                                new UrlPrefixDecorator<>(
                                        GetApiVersionHeaders.getInstance(), gatewayUrl.getPath()));
        headers.setCustomHeaders(customHttpHeaders);

        List<SqlGatewayRestAPIVersion> gatewayVersions =
                getResponse(
                                restClient.sendRequest(
                                        gatewayUrl.getHost(),
                                        gatewayUrl.getPort(),
                                        headers,
                                        EmptyMessageParameters.getInstance(),
                                        EmptyRequestBody.getInstance(),
                                        Collections.emptyList(),
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
                        .getVersions().stream()
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

    private void closeSession() throws SqlExecutionException {
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
        }
    }

    @VisibleForTesting
    Collection<HttpHeader> getCustomHttpHeaders() {
        return customHttpHeaders;
    }
}
