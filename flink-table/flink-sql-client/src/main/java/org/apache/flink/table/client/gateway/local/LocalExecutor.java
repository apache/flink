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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.ExecutionContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SQL_EXECUTION_ERROR;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);

    // result maintenance
    private final DefaultContext defaultContext;
    private SessionContext sessionContext;

    private final ClusterClientServiceLoader clusterClientServiceLoader;

    /** Creates a local executor for submitting table programs and retrieving results. */
    public LocalExecutor(DefaultContext defaultContext) {
        this.defaultContext = defaultContext;
        this.clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
    }

    @Override
    public void start() {
        // nothing to do yet
    }

    @Override
    public void openSession(@Nullable String sessionId) throws SqlExecutionException {
        // do nothing
        sessionContext = LocalContextUtils.buildSessionContext(sessionId, defaultContext);
    }

    @Override
    public void closeSession() throws SqlExecutionException {
        // Remove the session's ExecutionContext from contextMap and close it.
        if (sessionContext != null) {
            sessionContext.close();
        }
    }

    /**
     * Get the existed {@link ExecutionContext} from contextMap, or thrown exception if does not
     * exist.
     */
    @VisibleForTesting
    protected ExecutionContext getExecutionContext() throws SqlExecutionException {
        return sessionContext.getExecutionContext();
    }

    @Override
    public Map<String, String> getSessionConfigMap() throws SqlExecutionException {
        return sessionContext.getConfigMap();
    }

    @Override
    public ReadableConfig getSessionConfig() throws SqlExecutionException {
        return sessionContext.getReadableConfig();
    }

    @Override
    public void resetSessionProperties() throws SqlExecutionException {
        sessionContext.reset();
    }

    @Override
    public void resetSessionProperty(String key) throws SqlExecutionException {
        sessionContext.reset(key);
    }

    @Override
    public void setSessionProperty(String key, String value) throws SqlExecutionException {
        sessionContext.set(key, value);
    }

    @Override
    public Operation parseStatement(String statement) throws SqlExecutionException {
        final ExecutionContext context = getExecutionContext();
        final TableEnvironment tableEnv = context.getTableEnvironment();
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();

        List<Operation> operations;
        try {
            operations = parser.parse(statement);
        } catch (Throwable t) {
            throw new SqlExecutionException("Failed to parse statement: " + statement, t);
        }
        if (operations.isEmpty()) {
            throw new SqlExecutionException("Failed to parse statement: " + statement);
        }
        return operations.get(0);
    }

    @Override
    public List<String> completeStatement(String statement, int position) {
        final ExecutionContext context = getExecutionContext();
        final TableEnvironmentInternal tableEnv =
                (TableEnvironmentInternal) context.getTableEnvironment();

        try {
            return Arrays.asList(tableEnv.getParser().getCompletionHints(statement, position));
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not complete statement at " + position + ":" + statement, t);
            }
            return Collections.emptyList();
        }
    }

    @Override
    public TableResultInternal executeOperation(Operation operation) throws SqlExecutionException {
        final ExecutionContext context = getExecutionContext();
        final TableEnvironmentInternal tEnv =
                (TableEnvironmentInternal) context.getTableEnvironment();
        try {
            return tEnv.executeInternal(operation);
        } catch (Throwable t) {
            throw new SqlExecutionException(MESSAGE_SQL_EXECUTION_ERROR, t);
        }
    }

    @Override
    public TableResultInternal executeModifyOperations(List<ModifyOperation> operations)
            throws SqlExecutionException {
        final ExecutionContext context = getExecutionContext();
        final TableEnvironmentInternal tEnv =
                (TableEnvironmentInternal) context.getTableEnvironment();
        try {
            return tEnv.executeInternal(operations);
        } catch (Throwable t) {
            throw new SqlExecutionException(MESSAGE_SQL_EXECUTION_ERROR, t);
        }
    }

    @Override
    public ResultDescriptor executeQuery(QueryOperation query) throws SqlExecutionException {
        return new ResultDescriptor(executeOperation(query), sessionContext.getReadableConfig());
    }

    @Override
    public void removeJar(String jarUrl) {
        sessionContext.removeJar(jarUrl);
    }

    @Override
    public Optional<String> stopJob(String jobId, boolean isWithSavepoint, boolean isWithDrain)
            throws SqlExecutionException {
        Duration clientTimeout = getSessionConfig().get(ClientOptions.CLIENT_TIMEOUT);
        try {
            return runClusterAction(
                    clusterClient -> {
                        if (isWithSavepoint) {
                            // blocking get savepoint path
                            try {
                                String savepoint =
                                        clusterClient
                                                .stopWithSavepoint(
                                                        JobID.fromHexString(jobId),
                                                        isWithDrain,
                                                        null,
                                                        SavepointFormatType.DEFAULT)
                                                .get(
                                                        clientTimeout.toMillis(),
                                                        TimeUnit.MILLISECONDS);
                                return Optional.of(savepoint);
                            } catch (Exception e) {
                                throw new FlinkException(
                                        "Could not stop job "
                                                + jobId
                                                + " in session "
                                                + sessionContext.getSessionId()
                                                + ".",
                                        e);
                            }
                        } else {
                            clusterClient.cancel(JobID.fromHexString(jobId));
                            return Optional.empty();
                        }
                    });
        } catch (Exception e) {
            throw new SqlExecutionException(
                    "Could not stop job "
                            + jobId
                            + " in session "
                            + sessionContext.getSessionId()
                            + ".",
                    e);
        }
    }

    /**
     * Retrieves the {@link ClusterClient} from the session and runs the given {@link ClusterAction}
     * against it.
     *
     * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
     * @param <ClusterID> type of the cluster id
     * @param <Result>> type of the result
     * @throws FlinkException if something goes wrong
     */
    private <ClusterID, Result> Result runClusterAction(
            ClusterAction<ClusterID, Result> clusterAction) throws FlinkException {
        final Configuration configuration = (Configuration) sessionContext.getReadableConfig();
        final ClusterClientFactory<ClusterID> clusterClientFactory =
                sessionContext
                        .getExecutionContext()
                        .wrapClassLoader(
                                () ->
                                        clusterClientServiceLoader.getClusterClientFactory(
                                                configuration));

        final ClusterID clusterId = clusterClientFactory.getClusterId(configuration);
        Preconditions.checkNotNull(
                clusterId, "No cluster ID found for session " + sessionContext.getSessionId());

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                        clusterClientFactory.createClusterDescriptor(configuration);
                final ClusterClient<ClusterID> clusterClient =
                        clusterDescriptor.retrieve(clusterId).getClusterClient()) {
            return clusterAction.runAction(clusterClient);
        }
    }

    /**
     * Internal interface to encapsulate cluster actions which are executed via the {@link
     * ClusterClient}.
     *
     * @param <ClusterID> type of the cluster id
     * @param <Result>> type of the result
     */
    @FunctionalInterface
    private interface ClusterAction<ClusterID, Result> {

        /**
         * Run the cluster action with the given {@link ClusterClient}.
         *
         * @param clusterClient to run the cluster action against
         * @throws FlinkException if something goes wrong
         */
        Result runAction(ClusterClient<ClusterID> clusterClient) throws FlinkException;
    }
}
