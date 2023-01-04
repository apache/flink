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

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.StopJobOperation;
import org.apache.flink.table.operations.ddl.AlterOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.DropOperation;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.service.utils.Constants.COMPLETION_CANDIDATES;
import static org.apache.flink.table.gateway.service.utils.Constants.JOB_ID;
import static org.apache.flink.table.gateway.service.utils.Constants.SAVEPOINT_PATH;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_KEY;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_VALUE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** An executor to execute the {@link Operation}. */
public class OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(OperationExecutor.class);

    private final SessionContext sessionContext;
    private final Configuration executionConfig;

    private final ClusterClientServiceLoader clusterClientServiceLoader;

    @VisibleForTesting
    public OperationExecutor(SessionContext context, Configuration executionConfig) {
        this.sessionContext = context;
        this.executionConfig = executionConfig;
        this.clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
    }

    public ResultFetcher configureSession(OperationHandle handle, String statement) {
        TableEnvironmentInternal tableEnv = getTableEnvironment();
        List<Operation> parsedOperations = tableEnv.getParser().parse(statement);
        if (parsedOperations.size() > 1) {
            throw new UnsupportedOperationException(
                    "Unsupported SQL statement! Configure session only accepts a single SQL statement.");
        }
        Operation op = parsedOperations.get(0);

        if (!(op instanceof SetOperation)
                && !(op instanceof ResetOperation)
                && !(op instanceof CreateOperation)
                && !(op instanceof DropOperation)
                && !(op instanceof UseOperation)
                && !(op instanceof AlterOperation)
                && !(op instanceof LoadModuleOperation)
                && !(op instanceof UnloadModuleOperation)
                && !(op instanceof AddJarOperation)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported statement for configuring session:%s\n"
                                    + "The configureSession API only supports to execute statement of type "
                                    + "CREATE TABLE, DROP TABLE, ALTER TABLE, "
                                    + "CREATE DATABASE, DROP DATABASE, ALTER DATABASE, "
                                    + "CREATE FUNCTION, DROP FUNCTION, ALTER FUNCTION, "
                                    + "CREATE CATALOG, DROP CATALOG, "
                                    + "USE CATALOG, USE [CATALOG.]DATABASE, "
                                    + "CREATE VIEW, DROP VIEW, "
                                    + "LOAD MODULE, UNLOAD MODULE, USE MODULE, "
                                    + "ADD JAR.",
                            statement));
        }

        if (op instanceof SetOperation) {
            return callSetOperation(tableEnv, handle, (SetOperation) op);
        } else if (op instanceof ResetOperation) {
            return callResetOperation(handle, (ResetOperation) op);
        } else {
            return callOperation(tableEnv, handle, op);
        }
    }

    public ResultFetcher executeStatement(OperationHandle handle, String statement) {
        // Instantiate the TableEnvironment lazily
        TableEnvironmentInternal tableEnv = getTableEnvironment();
        List<Operation> parsedOperations = tableEnv.getParser().parse(statement);
        if (parsedOperations.size() > 1) {
            throw new UnsupportedOperationException(
                    "Unsupported SQL statement! Execute statement only accepts a single SQL statement or "
                            + "multiple 'INSERT INTO' statements wrapped in a 'STATEMENT SET' block.");
        }
        Operation op = parsedOperations.get(0);
        return sessionContext.isStatementSetState()
                ? executeOperationInStatementSetState(tableEnv, handle, op)
                : executeOperation(tableEnv, handle, op);
    }

    public String getCurrentCatalog() {
        return sessionContext.getSessionState().catalogManager.getCurrentCatalog();
    }

    public Set<String> listCatalogs() {
        return sessionContext.getSessionState().catalogManager.listCatalogs();
    }

    public Set<String> listDatabases(String catalogName) {
        return new HashSet<>(
                sessionContext
                        .getSessionState()
                        .catalogManager
                        .getCatalog(catalogName)
                        .orElseThrow(
                                () ->
                                        new CatalogNotExistException(
                                                String.format(
                                                        "Catalog '%s' does not exist.",
                                                        catalogName)))
                        .listDatabases());
    }

    public Set<TableInfo> listTables(
            String catalogName, String databaseName, Set<TableKind> tableKinds) {
        checkArgument(
                Arrays.asList(TableKind.TABLE, TableKind.VIEW).containsAll(tableKinds),
                "Currently only support to list TABLE, VIEW or TABLE AND VIEW.");
        if (tableKinds.contains(TableKind.TABLE) && tableKinds.contains(TableKind.VIEW)) {
            return listTables(catalogName, databaseName, true);
        } else if (tableKinds.contains(TableKind.TABLE)) {
            return listTables(catalogName, databaseName, false);
        } else {
            return listViews(catalogName, databaseName);
        }
    }

    public ResolvedCatalogBaseTable<?> getTable(ObjectIdentifier tableIdentifier) {
        return getTableEnvironment()
                .getCatalogManager()
                .getTableOrError(tableIdentifier)
                .getResolvedTable();
    }

    public Set<FunctionInfo> listUserDefinedFunctions(String catalogName, String databaseName) {
        return sessionContext.getSessionState().functionCatalog
                .getUserDefinedFunctions(catalogName, databaseName).stream()
                // Load the CatalogFunction from the remote catalog is time wasted. Set the
                // FunctionKind null.
                .map(FunctionInfo::new)
                .collect(Collectors.toSet());
    }

    public Set<FunctionInfo> listSystemFunctions() {
        Set<FunctionInfo> info = new HashSet<>();
        for (String functionName : sessionContext.getSessionState().moduleManager.listFunctions()) {
            try {
                info.add(
                        sessionContext
                                .getSessionState()
                                .moduleManager
                                .getFunctionDefinition(functionName)
                                .map(
                                        definition ->
                                                new FunctionInfo(
                                                        FunctionIdentifier.of(functionName),
                                                        definition.getKind()))
                                .orElse(new FunctionInfo(FunctionIdentifier.of(functionName))));
            } catch (Throwable t) {
                // Failed to load the function. Ignore.
                LOG.error(
                        String.format("Failed to load the system function `%s`.", functionName), t);
            }
        }
        return info;
    }

    public FunctionDefinition getFunctionDefinition(UnresolvedIdentifier identifier) {
        return sessionContext
                .getSessionState()
                .functionCatalog
                .lookupFunction(identifier)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Can not find the definition: %s.",
                                                identifier.asSummaryString())))
                .getDefinition();
    }

    public ResultSet getCompletionHints(String statement, int position) {
        return new ResultSet(
                ResultSet.ResultType.EOS,
                null,
                ResolvedSchema.of(Column.physical(COMPLETION_CANDIDATES, DataTypes.STRING())),
                Arrays.stream(
                                getTableEnvironment()
                                        .getParser()
                                        .getCompletionHints(statement, position))
                        .map(hint -> GenericRowData.of(StringData.fromString(hint)))
                        .collect(Collectors.toList()));
    }

    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public TableEnvironmentInternal getTableEnvironment() {
        TableEnvironmentInternal tableEnv = sessionContext.createTableEnvironment();
        tableEnv.getConfig().getConfiguration().addAll(executionConfig);
        return tableEnv;
    }

    private ResultFetcher executeOperationInStatementSetState(
            TableEnvironmentInternal tableEnv, OperationHandle handle, Operation operation) {
        if (operation instanceof EndStatementSetOperation) {
            return callEndStatementSetOperation(tableEnv, handle);
        } else if (operation instanceof ModifyOperation) {
            sessionContext.addStatementSetOperation((ModifyOperation) operation);
            return buildOkResultFetcher(handle);
        } else {
            throw new SqlExecutionException(
                    "Only 'INSERT/CREATE TABLE AS' statement is allowed in Statement Set or use 'END' statement to submit Statement Set.");
        }
    }

    private ResultFetcher executeOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, Operation op) {
        if (op instanceof SetOperation) {
            return callSetOperation(tableEnv, handle, (SetOperation) op);
        } else if (op instanceof ResetOperation) {
            return callResetOperation(handle, (ResetOperation) op);
        } else if (op instanceof BeginStatementSetOperation) {
            return callBeginStatementSetOperation(handle);
        } else if (op instanceof EndStatementSetOperation) {
            throw new SqlExecutionException(
                    "No Statement Set to submit. 'END' statement should be used after 'BEGIN STATEMENT SET'.");
        } else if (op instanceof ModifyOperation) {
            return callModifyOperations(
                    tableEnv, handle, Collections.singletonList((ModifyOperation) op));
        } else if (op instanceof StatementSetOperation) {
            return callModifyOperations(
                    tableEnv, handle, ((StatementSetOperation) op).getOperations());
        } else if (op instanceof QueryOperation) {
            TableResultInternal result = tableEnv.executeInternal(op);
            return new ResultFetcher(handle, result.getResolvedSchema(), result.collectInternal());
        } else if (op instanceof StopJobOperation) {
            return callStopJobOperation(handle, (StopJobOperation) op);
        } else {
            return callOperation(tableEnv, handle, op);
        }
    }

    private ResultFetcher callSetOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, SetOperation setOp) {
        if (setOp.getKey().isPresent() && setOp.getValue().isPresent()) {
            // set a property
            sessionContext.set(setOp.getKey().get().trim(), setOp.getValue().get().trim());
            return buildOkResultFetcher(handle);
        } else if (!setOp.getKey().isPresent() && !setOp.getValue().isPresent()) {
            // show all properties
            Map<String, String> configMap = tableEnv.getConfig().getConfiguration().toMap();
            return new ResultFetcher(
                    handle,
                    ResolvedSchema.of(
                            Column.physical(SET_KEY, DataTypes.STRING()),
                            Column.physical(SET_VALUE, DataTypes.STRING())),
                    CollectionUtil.iteratorToList(
                            configMap.entrySet().stream()
                                    .map(
                                            entry ->
                                                    GenericRowData.of(
                                                            StringData.fromString(entry.getKey()),
                                                            StringData.fromString(
                                                                    entry.getValue())))
                                    .map(RowData.class::cast)
                                    .iterator()));
        } else {
            // impossible
            throw new SqlExecutionException("Illegal SetOperation: " + setOp.asSummaryString());
        }
    }

    private ResultFetcher callResetOperation(OperationHandle handle, ResetOperation resetOp) {
        if (resetOp.getKey().isPresent()) {
            // reset a property
            sessionContext.reset(resetOp.getKey().get().trim());
        } else {
            // reset all properties
            sessionContext.reset();
        }
        return buildOkResultFetcher(handle);
    }

    private ResultFetcher callBeginStatementSetOperation(OperationHandle handle) {
        sessionContext.enableStatementSet();
        return buildOkResultFetcher(handle);
    }

    private ResultFetcher callEndStatementSetOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle) {
        // reset the state regardless of whether error occurs while executing the set
        List<ModifyOperation> statementSetOperations = sessionContext.getStatementSetOperations();
        sessionContext.disableStatementSet();

        if (statementSetOperations.isEmpty()) {
            // there's no statement in the statement set, skip submitting
            return buildOkResultFetcher(handle);
        } else {
            return callModifyOperations(tableEnv, handle, statementSetOperations);
        }
    }

    private ResultFetcher callModifyOperations(
            TableEnvironmentInternal tableEnv,
            OperationHandle handle,
            List<ModifyOperation> modifyOperations) {
        TableResultInternal result = tableEnv.executeInternal(modifyOperations);
        return new ResultFetcher(
                handle,
                ResolvedSchema.of(Column.physical(JOB_ID, DataTypes.STRING())),
                Collections.singletonList(
                        GenericRowData.of(
                                StringData.fromString(
                                        result.getJobClient()
                                                .orElseThrow(
                                                        () ->
                                                                new SqlExecutionException(
                                                                        String.format(
                                                                                "Can't get job client for the operation %s.",
                                                                                handle)))
                                                .getJobID()
                                                .toString()))));
    }

    private ResultFetcher callOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, Operation op) {
        TableResultInternal result = tableEnv.executeInternal(op);
        return new ResultFetcher(
                handle,
                result.getResolvedSchema(),
                CollectionUtil.iteratorToList(result.collectInternal()));
    }

    private Set<TableInfo> listTables(
            String catalogName, String databaseName, boolean includeViews) {
        CatalogManager catalogManager = sessionContext.getSessionState().catalogManager;
        Map<String, TableInfo> views = new HashMap<>();
        catalogManager
                .listViews(catalogName, databaseName)
                .forEach(
                        name ->
                                views.put(
                                        name,
                                        new TableInfo(
                                                ObjectIdentifier.of(
                                                        catalogName, databaseName, name),
                                                TableKind.VIEW)));

        Map<String, TableInfo> ans = new HashMap<>();
        if (includeViews) {
            ans.putAll(views);
        }
        catalogManager.listTables(catalogName, databaseName).stream()
                .filter(name -> !views.containsKey(name))
                .forEach(
                        name ->
                                ans.put(
                                        name,
                                        new TableInfo(
                                                ObjectIdentifier.of(
                                                        catalogName, databaseName, name),
                                                TableKind.TABLE)));
        return Collections.unmodifiableSet(new HashSet<>(ans.values()));
    }

    private Set<TableInfo> listViews(String catalogName, String databaseName) {
        return Collections.unmodifiableSet(
                sessionContext.getSessionState().catalogManager.listViews(catalogName, databaseName)
                        .stream()
                        .map(
                                name ->
                                        new TableInfo(
                                                ObjectIdentifier.of(
                                                        catalogName, databaseName, name),
                                                TableKind.VIEW))
                        .collect(Collectors.toSet()));
    }

    public ResultFetcher callStopJobOperation(
            OperationHandle handle, StopJobOperation stopJobOperation)
            throws SqlExecutionException {
        String jobId = stopJobOperation.getJobId();
        boolean isWithSavepoint = stopJobOperation.isWithSavepoint();
        boolean isWithDrain = stopJobOperation.isWithDrain();
        Duration clientTimeout =
                Configuration.fromMap(sessionContext.getConfigMap())
                        .get(ClientOptions.CLIENT_TIMEOUT);
        Optional<String> savepoint;
        try {
            savepoint =
                    runClusterAction(
                            handle,
                            clusterClient -> {
                                if (isWithSavepoint) {
                                    // blocking get savepoint path
                                    try {
                                        return Optional.of(
                                                clusterClient
                                                        .stopWithSavepoint(
                                                                JobID.fromHexString(jobId),
                                                                isWithDrain,
                                                                executionConfig.get(
                                                                        CheckpointingOptions
                                                                                .SAVEPOINT_DIRECTORY),
                                                                SavepointFormatType.DEFAULT)
                                                        .get(
                                                                clientTimeout.toMillis(),
                                                                TimeUnit.MILLISECONDS));
                                    } catch (Exception e) {
                                        throw new FlinkException(
                                                "Could not stop job "
                                                        + stopJobOperation.getJobId()
                                                        + " in session "
                                                        + handle.getIdentifier()
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
                    "Could not stop job " + jobId + " for operation " + handle + ".", e);
        }
        if (isWithSavepoint) {
            return new ResultFetcher(
                    handle,
                    ResolvedSchema.of(Column.physical(SAVEPOINT_PATH, DataTypes.STRING())),
                    Collections.singletonList(
                            GenericRowData.of(StringData.fromString(savepoint.orElse("")))));
        } else {
            return buildOkResultFetcher(handle);
        }
    }

    private ResultFetcher buildOkResultFetcher(OperationHandle handle) {
        return new ResultFetcher(
                handle,
                TableResultInternal.TABLE_RESULT_OK.getResolvedSchema(),
                CollectionUtil.iteratorToList(
                        TableResultInternal.TABLE_RESULT_OK.collectInternal()));
    }

    /**
     * Retrieves the {@link ClusterClient} from the session and runs the given {@link ClusterAction}
     * against it.
     *
     * @param handle the specified operation handle
     * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
     * @param <ClusterID> type of the cluster id
     * @param <Result>> type of the result
     * @throws FlinkException if something goes wrong
     */
    private <ClusterID, Result> Result runClusterAction(
            OperationHandle handle, ClusterAction<ClusterID, Result> clusterAction)
            throws FlinkException {
        final Configuration configuration = Configuration.fromMap(sessionContext.getConfigMap());
        final ClusterClientFactory<ClusterID> clusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(configuration);

        final ClusterID clusterId = clusterClientFactory.getClusterId(configuration);
        Preconditions.checkNotNull(clusterId, "No cluster ID found for operation " + handle);

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
