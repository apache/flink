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
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.environment.SqlGatewayStreamExecutionEnvironment;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.CallProcedureOperation;
import org.apache.flink.table.operations.CompileAndExecutePlanOperation;
import org.apache.flink.table.operations.DeleteFromFilterOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.LoadModuleOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.UnloadModuleOperation;
import org.apache.flink.table.operations.UseOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ExecutePlanOperation;
import org.apache.flink.table.operations.command.RemoveJarOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.command.ShowJobsOperation;
import org.apache.flink.table.operations.command.StopJobOperation;
import org.apache.flink.table.operations.ddl.AlterOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.DropOperation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;
import static org.apache.flink.table.gateway.service.utils.Constants.COMPLETION_CANDIDATES;
import static org.apache.flink.table.gateway.service.utils.Constants.JOB_ID;
import static org.apache.flink.table.gateway.service.utils.Constants.JOB_NAME;
import static org.apache.flink.table.gateway.service.utils.Constants.SAVEPOINT_PATH;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_KEY;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_VALUE;
import static org.apache.flink.table.gateway.service.utils.Constants.START_TIME;
import static org.apache.flink.table.gateway.service.utils.Constants.STATUS;
import static org.apache.flink.util.Preconditions.checkArgument;

/** An executor to execute the {@link Operation}. */
public class OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(OperationExecutor.class);

    protected final SessionContext sessionContext;

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
        if (op instanceof CallProcedureOperation) {
            // if the operation is CallProcedureOperation, we need to set the stream environment
            // context to it since the procedure will use the stream environment
            try {
                SqlGatewayStreamExecutionEnvironment.setAsContext(
                        sessionContext.getUserClassloader());
                return executeOperation(tableEnv, handle, op);
            } finally {
                SqlGatewayStreamExecutionEnvironment.unsetAsContext();
            }
        } else {
            return sessionContext.isStatementSetState()
                    ? executeOperationInStatementSetState(tableEnv, handle, op)
                    : executeOperation(tableEnv, handle, op);
        }
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

    public ResultFetcher getCompletionHints(
            OperationHandle operationHandle, String statement, int position) {
        return ResultFetcher.fromResults(
                operationHandle,
                ResolvedSchema.of(Column.physical(COMPLETION_CANDIDATES, DataTypes.STRING())),
                Arrays.stream(
                                getTableEnvironment()
                                        .getParser()
                                        .getCompletionHints(statement, position))
                        .map(hint -> GenericRowData.of(StringData.fromString(hint)))
                        .collect(Collectors.toList()));
    }

    // --------------------------------------------------------------------------------------------

    public TableEnvironmentInternal getTableEnvironment() {
        // checks the value of RUNTIME_MODE
        Configuration operationConfig = sessionContext.getSessionConf().clone();
        operationConfig.addAll(executionConfig);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(operationConfig).build();

        // We need not different StreamExecutionEnvironments to build and submit flink job,
        // instead we just use StreamExecutionEnvironment#executeAsync(StreamGraph) method
        // to execute existing StreamGraph.
        // This requires StreamExecutionEnvironment to have a full flink configuration.
        StreamExecutionEnvironment streamExecEnv =
                new StreamExecutionEnvironment(
                        operationConfig, sessionContext.getUserClassloader());

        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(sessionContext.getDefaultContext().getFlinkConfig());
        tableConfig.addConfiguration(operationConfig);

        final Executor executor =
                lookupExecutor(streamExecEnv, sessionContext.getUserClassloader());
        return createStreamTableEnvironment(
                streamExecEnv,
                settings,
                tableConfig,
                executor,
                sessionContext.getSessionState().catalogManager,
                sessionContext.getSessionState().moduleManager,
                sessionContext.getSessionState().resourceManager,
                sessionContext.getSessionState().functionCatalog);
    }

    private static Executor lookupExecutor(
            StreamExecutionEnvironment executionEnvironment, ClassLoader userClassLoader) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            userClassLoader,
                            ExecutorFactory.class,
                            ExecutorFactory.DEFAULT_IDENTIFIER);
            final Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private TableEnvironmentInternal createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig tableConfig,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog) {

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        resourceManager.getUserClassLoader(),
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                tableConfig,
                env,
                planner,
                executor,
                settings.isStreamingMode());
    }

    private ResultFetcher executeOperationInStatementSetState(
            TableEnvironmentInternal tableEnv, OperationHandle handle, Operation operation) {
        if (operation instanceof EndStatementSetOperation) {
            return callEndStatementSetOperation(tableEnv, handle);
        } else if (operation instanceof ModifyOperation) {
            sessionContext.addStatementSetOperation((ModifyOperation) operation);
            return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
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
        } else if (op instanceof CompileAndExecutePlanOperation
                || op instanceof ExecutePlanOperation) {
            return callExecuteOperation(tableEnv, handle, op);
        } else if (op instanceof StatementSetOperation) {
            return callModifyOperations(
                    tableEnv, handle, ((StatementSetOperation) op).getOperations());
        } else if (op instanceof QueryOperation) {
            TableResultInternal result = tableEnv.executeInternal(op);
            return ResultFetcher.fromTableResult(handle, result, true);
        } else if (op instanceof StopJobOperation) {
            return callStopJobOperation(tableEnv, handle, (StopJobOperation) op);
        } else if (op instanceof ShowJobsOperation) {
            return callShowJobsOperation(tableEnv, handle, (ShowJobsOperation) op);
        } else if (op instanceof RemoveJarOperation) {
            return callRemoveJar(handle, ((RemoveJarOperation) op).getPath());
        } else {
            return callOperation(tableEnv, handle, op);
        }
    }

    private ResultFetcher callSetOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, SetOperation setOp) {
        if (setOp.getKey().isPresent() && setOp.getValue().isPresent()) {
            // set a property
            sessionContext.set(setOp.getKey().get().trim(), setOp.getValue().get().trim());
            return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
        } else if (!setOp.getKey().isPresent() && !setOp.getValue().isPresent()) {
            // show all properties
            Map<String, String> configMap = tableEnv.getConfig().getConfiguration().toMap();
            return ResultFetcher.fromResults(
                    handle,
                    ResolvedSchema.of(
                            Column.physical(SET_KEY, DataTypes.STRING()),
                            Column.physical(SET_VALUE, DataTypes.STRING())),
                    CollectionUtil.iteratorToList(
                            configMap.keySet().stream()
                                    .sorted()
                                    .map(
                                            key ->
                                                    GenericRowData.of(
                                                            StringData.fromString(key),
                                                            StringData.fromString(
                                                                    configMap.get(key))))
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
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private ResultFetcher callBeginStatementSetOperation(OperationHandle handle) {
        sessionContext.enableStatementSet();
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private ResultFetcher callEndStatementSetOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle) {
        // reset the state regardless of whether error occurs while executing the set
        List<ModifyOperation> statementSetOperations = sessionContext.getStatementSetOperations();
        sessionContext.disableStatementSet();

        if (statementSetOperations.isEmpty()) {
            // there's no statement in the statement set, skip submitting
            return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
        } else {
            return callModifyOperations(tableEnv, handle, statementSetOperations);
        }
    }

    private ResultFetcher callModifyOperations(
            TableEnvironmentInternal tableEnv,
            OperationHandle handle,
            List<ModifyOperation> modifyOperations) {
        TableResultInternal result = tableEnv.executeInternal(modifyOperations);
        // DeleteFromFilterOperation doesn't have a JobClient
        if (modifyOperations.size() == 1
                && modifyOperations.get(0) instanceof DeleteFromFilterOperation) {
            return ResultFetcher.fromTableResult(handle, result, false);
        }

        return fetchJobId(result, handle);
    }

    private ResultFetcher callExecuteOperation(
            TableEnvironmentInternal tableEnv,
            OperationHandle handle,
            Operation executePlanOperation) {
        return fetchJobId(tableEnv.executeInternal(executePlanOperation), handle);
    }

    private ResultFetcher fetchJobId(TableResultInternal result, OperationHandle handle) {
        JobID jobID =
                result.getJobClient()
                        .orElseThrow(
                                () ->
                                        new SqlExecutionException(
                                                String.format(
                                                        "Can't get job client for the operation %s.",
                                                        handle)))
                        .getJobID();
        return ResultFetcher.fromResults(
                handle,
                ResolvedSchema.of(Column.physical(JOB_ID, DataTypes.STRING())),
                Collections.singletonList(
                        GenericRowData.of(StringData.fromString(jobID.toString()))),
                jobID,
                result.getResultKind());
    }

    protected ResultFetcher callRemoveJar(OperationHandle operationHandle, String jarPath) {
        throw new UnsupportedOperationException(
                "SQL Gateway doesn't support REMOVE JAR syntax now.");
    }

    private ResultFetcher callOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, Operation op) {
        TableResultInternal result = tableEnv.executeInternal(op);
        return ResultFetcher.fromTableResult(handle, result, false);
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
            TableEnvironmentInternal tableEnv,
            OperationHandle handle,
            StopJobOperation stopJobOperation)
            throws SqlExecutionException {
        String jobId = stopJobOperation.getJobId();
        boolean isWithSavepoint = stopJobOperation.isWithSavepoint();
        boolean isWithDrain = stopJobOperation.isWithDrain();
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        Duration clientTimeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
        Optional<String> savepoint =
                runClusterAction(
                        configuration,
                        handle,
                        clusterClient -> {
                            try {
                                if (isWithSavepoint) {
                                    // blocking get savepoint path
                                    return Optional.of(
                                            clusterClient
                                                    .stopWithSavepoint(
                                                            JobID.fromHexString(jobId),
                                                            isWithDrain,
                                                            configuration.get(
                                                                    CheckpointingOptions
                                                                            .SAVEPOINT_DIRECTORY),
                                                            SavepointFormatType.DEFAULT)
                                                    .get(
                                                            clientTimeout.toMillis(),
                                                            TimeUnit.MILLISECONDS));
                                } else {
                                    clusterClient
                                            .cancel(JobID.fromHexString(jobId))
                                            .get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
                                    return Optional.empty();
                                }
                            } catch (Exception e) {
                                throw new SqlExecutionException(
                                        String.format(
                                                "Could not stop job %s %s for operation %s.",
                                                jobId,
                                                isWithSavepoint ? "with savepoint" : "",
                                                handle.getIdentifier()),
                                        e);
                            }
                        });
        if (isWithSavepoint) {
            return ResultFetcher.fromResults(
                    handle,
                    ResolvedSchema.of(Column.physical(SAVEPOINT_PATH, DataTypes.STRING())),
                    Collections.singletonList(
                            GenericRowData.of(StringData.fromString(savepoint.orElse("")))));
        } else {
            return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
        }
    }

    public ResultFetcher callShowJobsOperation(
            TableEnvironmentInternal tableEnv,
            OperationHandle operationHandle,
            ShowJobsOperation showJobsOperation)
            throws SqlExecutionException {
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        Duration clientTimeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
        Collection<JobStatusMessage> jobs =
                runClusterAction(
                        configuration,
                        operationHandle,
                        clusterClient -> {
                            try {
                                return clusterClient
                                        .listJobs()
                                        .get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                throw new SqlExecutionException(
                                        "Failed to list jobs in the cluster.", e);
                            }
                        });
        List<RowData> resultRows =
                jobs.stream()
                        .map(
                                job ->
                                        GenericRowData.of(
                                                StringData.fromString(job.getJobId().toString()),
                                                StringData.fromString(job.getJobName()),
                                                StringData.fromString(job.getJobState().toString()),
                                                DateTimeUtils.toTimestampData(
                                                        job.getStartTime(), 3)))
                        .collect(Collectors.toList());
        return ResultFetcher.fromResults(
                operationHandle,
                ResolvedSchema.of(
                        Column.physical(JOB_ID, DataTypes.STRING()),
                        Column.physical(JOB_NAME, DataTypes.STRING()),
                        Column.physical(STATUS, DataTypes.STRING()),
                        Column.physical(START_TIME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())),
                resultRows);
    }

    /**
     * Retrieves the {@link ClusterClient} from the session and runs the given {@link ClusterAction}
     * against it.
     *
     * @param configuration the combined configuration of {@code sessionConf} and {@code
     *     executionConfig}.
     * @param handle the specified operation handle
     * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
     * @param <ClusterID> type of the cluster id
     * @param <Result>> type of the result
     * @throws SqlExecutionException if something goes wrong
     */
    private <ClusterID, Result> Result runClusterAction(
            Configuration configuration,
            OperationHandle handle,
            ClusterAction<ClusterID, Result> clusterAction)
            throws SqlExecutionException {
        final ClusterClientFactory<ClusterID> clusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(configuration);

        final ClusterID clusterId = clusterClientFactory.getClusterId(configuration);
        Preconditions.checkNotNull(clusterId, "No cluster ID found for operation " + handle);

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                        clusterClientFactory.createClusterDescriptor(configuration);
                final ClusterClient<ClusterID> clusterClient =
                        clusterDescriptor.retrieve(clusterId).getClusterClient()) {
            return clusterAction.runAction(clusterClient);
        } catch (FlinkException e) {
            throw new SqlExecutionException("Failed to run cluster action.", e);
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
