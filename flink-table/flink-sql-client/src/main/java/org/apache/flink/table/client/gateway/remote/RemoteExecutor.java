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

package org.apache.flink.table.client.gateway.remote;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.ResultStore;
import org.apache.flink.table.client.gateway.local.result.ChangelogResult;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;
import org.apache.flink.table.client.gateway.remote.result.TableResultWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.TriggerSessionHeartbeatHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenParameters;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler.CLOSE_MESSAGE;

/**
 * Executor that performs the Flink communication remotely. Connection to SQL and query execution
 * are managed by the RestClient.
 */
public class RemoteExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutor.class);

    private final RestClient restClient;
    private final ResultStore resultStore;
    private final KeepingAliveThread keepingAliveThread;

    private String sessionHandleId;
    private SessionHandle sessionHandle;
    private SessionMessageParameters sessionMessageParametersInstance;

    private final String address;
    private final int port;

    /** Creates a remote executor for submitting table programs and retrieving results. */
    public RemoteExecutor(Configuration flinkConfig, String address, int port)
            throws SqlClientException {
        this.address = address;
        this.port = port;
        // init rest client
        try {
            restClient = new RestClient(flinkConfig, Executors.directExecutor());
        } catch (ConfigurationException e) {
            LOG.error("Cannot get rest client.", e);
            throw new SqlClientException("Cannot get rest client.", e);
        }
        this.resultStore = new ResultStore();
        this.keepingAliveThread = new KeepingAliveThread(10_000L);
    }

    public void start() throws SqlClientException {
        // Open session to address:port and get the session handle ID
        OpenSessionRequestBody request = new OpenSessionRequestBody(null, null);
        try {
            CompletableFuture<OpenSessionResponseBody> response =
                    sendRequest(
                            OpenSessionHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            request);
            sessionHandleId = response.get().getSessionHandle();
            LOG.info("Open session '{}' to {}:{}.", sessionHandleId, address, port);
        } catch (Exception e) {
            LOG.error(String.format("Failed to open session to %s:%s", address, port), e);
            throw new SqlClientException(
                    String.format("Failed to open session to %s:%s", address, port), e);
        }
        sessionHandle = new SessionHandle(UUID.fromString(sessionHandleId));
        sessionMessageParametersInstance = new SessionMessageParameters(sessionHandle);
        keepingAliveThread.start();
    }

    public void close() throws SqlClientException {
        resultStore
                .getResults()
                .forEach(
                        (resultId) -> {
                            try {
                                cancelQuery(resultId);
                            } catch (Throwable t) {
                                LOG.warn(
                                        String.format(
                                                "Unexpected error occurs when canceling query. Result ID: %s.",
                                                resultId),
                                        t);
                                // ignore any throwable to keep the cleanup running
                            }
                        });

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
                            "Unexpected error occurs when closing session %s.", sessionHandleId),
                    t);
            // ignore any throwable to keep the cleanup running
        }

        keepingAliveThread.interrupt();
    }

    public Map<String, String> getSessionConfig() throws SqlClientException {
        try {
            CompletableFuture<GetSessionConfigResponseBody> response =
                    sendRequest(
                            GetSessionConfigHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            EmptyRequestBody.getInstance());
            return response.get().getProperties();
        } catch (Exception e) {
            LOG.error("Failed to get session config.", e);
            throw new SqlClientException("Failed to get session config.", e);
        }
    }

    public List<String> completeStatement(String statement, int position) {
        return new ArrayList<>();
    }

    public TableResultWrapper executeStatement(
            String statement, long executionTimeOutMs, @Nullable Configuration executionConfig)
            throws SqlClientException {
        if (executionTimeOutMs <= 0) {
            LOG.error("The timeout must be positive.");
            throw new SqlClientException("The timeout must be positive.");
        }

        statement = statement.trim();
        LOG.info("Executing SQL statement: '{}'...", statement);
        Map<String, String> config =
                executionConfig == null ? new HashMap<>() : executionConfig.toMap();
        ExecuteStatementRequestBody request =
                new ExecuteStatementRequestBody(statement, 0L, config);
        try {
            CompletableFuture<ExecuteStatementResponseBody> executeStatementResponse =
                    sendRequest(
                            ExecuteStatementHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            request);

            String operationHandleId = executeStatementResponse.get().getOperationHandle();
            OperationHandle operationHandle =
                    new OperationHandle(UUID.fromString(operationHandleId));

            LOG.info("Fetching the first result...");
            FetchResultsResponseBody fetchResultsResponse =
                    fetchWhenResultsReady(operationHandle, Duration.ofMillis(executionTimeOutMs));
            ResultSet firstResult = fetchResultsResponse.getResults();
            Long nextToken = parseTokenFromUri(fetchResultsResponse.getNextResultUri());

            TableResultWrapper result =
                    new TableResultWrapper(this, operationHandle, firstResult, nextToken);

            if (isQuery(statement)) {
                storeResult(result, executionConfig);
            }
            return result;
        } catch (Exception e) {
            LOG.error("Unexpected error occurs when executing SQL statement.", e);
            throw new SqlClientException(
                    "Unexpected error occurs when executing SQL statement.", e);
        }
    }

    public TypedResult<List<RowData>> retrieveResultChanges(String resultId) {
        DynamicResult result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException(
                    String.format(
                            "Could not find a result with result identifier '%s'.", resultId));
        }
        if (result.isMaterialized()) {
            throw new SqlClientException("Invalid result retrieval mode.");
        }
        return ((ChangelogResult) result).retrieveChanges();
    }

    public TypedResult<Integer> snapshotResult(String resultId, int pageSize) {
        DynamicResult result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException(
                    String.format(
                            "Could not find a result with result identifier '%s'.", resultId));
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult) result).snapshot(pageSize);
    }

    public List<RowData> retrieveResultPage(String resultId, int page) {
        final DynamicResult result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException(
                    String.format(
                            "Could not find a result with result identifier '%s'.", resultId));
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult) result).retrievePage(page);
    }

    public void cancelQuery(String resultId) throws SqlExecutionException {
        DynamicResult result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException(
                    String.format(
                            "Could not find a result with result identifier '%s'.", resultId));
        }

        // stop retrieval and remove the result
        LOG.info("Cancelling job {} and result retrieval.", resultId);
        try {
            // this operator will also stop flink job
            result.close();
        } catch (Throwable t) {
            throw new SqlExecutionException("Could not cancel the query execution", t);
        }
        resultStore.removeResult(resultId);
    }

    // ---------------------------------------------------------------------------------------------
    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request)
                    throws IOException {
        return restClient.sendRequest(address, port, messageHeaders, messageParameters, request);
    }

    private FetchResultsResponseBody fetchResults(OperationHandle operationHandle) {
        return fetchResults(operationHandle, 0L);
    }

    public FetchResultsResponseBody fetchResults(OperationHandle operationHandle, long token)
            throws SqlClientException {
        FetchResultsTokenParameters fetchResultsTokenParameters =
                new FetchResultsTokenParameters(sessionHandle, operationHandle, token);
        try {
            return sendRequest(
                            FetchResultsHeaders.getInstance(),
                            fetchResultsTokenParameters,
                            EmptyRequestBody.getInstance())
                    .get();
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Unexpected error occurs when fetching results. OperationHandle ID: %s.",
                            operationHandle),
                    e);
            throw new SqlClientException(
                    String.format(
                            "Unexpected error occurs when fetching results. OperationHandle ID: %s.",
                            operationHandle),
                    e);
        }
    }

    @SuppressWarnings("BusyWait")
    private FetchResultsResponseBody fetchWhenResultsReady(
            OperationHandle operationHandle, Duration timeout) throws SqlClientException {
        long timeoutMs = timeout.toMillis();
        long startingTime = System.currentTimeMillis();

        Function<FetchResultsResponseBody, Boolean> wait =
                response -> response.getResultType().equals(ResultSet.ResultType.NOT_READY.name());
        FetchResultsResponseBody response = fetchResults(operationHandle);

        while (wait.apply(response) && System.currentTimeMillis() - startingTime < timeoutMs) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new SqlClientException(e);
            }
            response = fetchResults(operationHandle);
        }

        if (wait.apply(response)) {
            LOG.error(
                    "Failed to fetch results within timeout. OperationHandle ID: {}.",
                    operationHandle);
            throw new SqlClientException(
                    String.format(
                            "Failed to fetch results within timeout. OperationHandle ID: %s.",
                            operationHandle));
        }

        return response;
    }

    public Long parseTokenFromUri(String uri) {
        if (uri == null || uri.length() == 0) {
            return null;
        }
        String[] split = uri.split("/");
        return Long.valueOf(split[split.length - 1]);
    }

    private boolean isQuery(String sql) {
        sql = sql.toUpperCase();
        return sql.startsWith("SELECT ");
    }

    private void storeResult(
            TableResultWrapper resultWrapper, @Nullable Configuration executionConfig) {
        // TODO: get config cost too much time. optimize later
        Configuration configuration = Configuration.fromMap(getSessionConfig());
        configuration.addAll(executionConfig == null ? new Configuration() : executionConfig);
        DynamicResult result = resultStore.createResult(configuration, resultWrapper);

        String resultId = new AbstractID().toHexString();
        resultWrapper.setResultId(resultId);
        resultWrapper.setMaterialized(result.isMaterialized());
        resultWrapper.setConfig(configuration);

        resultStore.storeResult(resultId, result);
    }

    @VisibleForTesting
    public SessionHandle getSessionHandle() {
        return sessionHandle;
    }

    /** Thread to keep connection to SQL gateway alive. */
    private class KeepingAliveThread extends Thread {
        private final long interval;

        private KeepingAliveThread(long interval) {
            this.interval = interval;
        }

        @Override
        @SuppressWarnings("BusyWait")
        public void run() {
            while (true) {
                try {
                    Thread.sleep(interval);
                    sendRequest(
                            TriggerSessionHeartbeatHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            EmptyRequestBody.getInstance());
                } catch (Exception e) {
                    LOG.warn("Unexpected error occurs when sending heart beat message.", e);
                    // ignore
                }
            }
        }
    }
}
