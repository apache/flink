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
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.service.utils.Constants.JOB_ID;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_KEY;
import static org.apache.flink.table.gateway.service.utils.Constants.SET_VALUE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** An executor to execute the {@link Operation}. */
public class OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(OperationExecutor.class);

    private final SessionContext sessionContext;
    private final Configuration executionConfig;

    @VisibleForTesting
    public OperationExecutor(SessionContext context, Configuration executionConfig) {
        this.sessionContext = context;
        this.executionConfig = executionConfig;
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
        if (op instanceof SetOperation) {
            return callSetOperation(tableEnv, handle, (SetOperation) op);
        } else if (op instanceof ResetOperation) {
            return callResetOperation(handle, (ResetOperation) op);
        } else if (op instanceof BeginStatementSetOperation) {
            // TODO: support statement set in the FLINK-27837
            throw new UnsupportedOperationException();
        } else if (op instanceof EndStatementSetOperation) {
            // TODO: support statement set in the FLINK-27837
            throw new UnsupportedOperationException();
        } else if (op instanceof ModifyOperation) {
            return callModifyOperations(
                    tableEnv, handle, Collections.singletonList((ModifyOperation) op));
        } else if (op instanceof StatementSetOperation) {
            return callModifyOperations(
                    tableEnv, handle, ((StatementSetOperation) op).getOperations());
        } else if (op instanceof QueryOperation) {
            TableResultInternal result = tableEnv.executeInternal(op);
            return new ResultFetcher(handle, result.getResolvedSchema(), result.collectInternal());
        } else {
            TableResultInternal result = tableEnv.executeInternal(op);
            return new ResultFetcher(
                    handle, result.getResolvedSchema(), collect(result.collectInternal()));
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

    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public TableEnvironmentInternal getTableEnvironment() {
        TableEnvironmentInternal tableEnv = sessionContext.createTableEnvironment();
        tableEnv.getConfig().getConfiguration().addAll(executionConfig);
        return tableEnv;
    }

    private ResultFetcher callSetOperation(
            TableEnvironmentInternal tableEnv, OperationHandle handle, SetOperation setOp) {
        if (setOp.getKey().isPresent() && setOp.getValue().isPresent()) {
            // set a property
            sessionContext.set(setOp.getKey().get().trim(), setOp.getValue().get().trim());
            return new ResultFetcher(
                    handle,
                    TableResultInternal.TABLE_RESULT_OK.getResolvedSchema(),
                    collect(TableResultInternal.TABLE_RESULT_OK.collectInternal()));
        } else if (!setOp.getKey().isPresent() && !setOp.getValue().isPresent()) {
            // show all properties
            Map<String, String> configMap = tableEnv.getConfig().getConfiguration().toMap();
            return new ResultFetcher(
                    handle,
                    ResolvedSchema.of(
                            Column.physical(SET_KEY, DataTypes.STRING()),
                            Column.physical(SET_VALUE, DataTypes.STRING())),
                    collect(
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
        return new ResultFetcher(
                handle,
                TableResultInternal.TABLE_RESULT_OK.getResolvedSchema(),
                collect(TableResultInternal.TABLE_RESULT_OK.collectInternal()));
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

    private List<RowData> collect(Iterator<RowData> tableResult) {
        List<RowData> rows = new ArrayList<>();
        tableResult.forEachRemaining(rows::add);
        return rows;
    }
}
