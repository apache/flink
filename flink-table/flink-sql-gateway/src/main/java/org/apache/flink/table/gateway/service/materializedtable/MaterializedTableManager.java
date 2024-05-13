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

package org.apache.flink.table.gateway.service.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.operations.command.DescribeJobOperation;
import org.apache.flink.table.operations.command.StopJobOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableResumeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.DropMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperation;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

/** Manager is responsible for execute the {@link MaterializedTableOperation}. */
@Internal
public class MaterializedTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedTableManager.class);

    public static ResultFetcher callMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            MaterializedTableOperation op,
            String statement) {
        if (op instanceof CreateMaterializedTableOperation) {
            return callCreateMaterializedTableOperation(
                    operationExecutor, handle, (CreateMaterializedTableOperation) op);
        } else if (op instanceof AlterMaterializedTableRefreshOperation) {
            return callAlterMaterializedTableRefreshOperation(
                    operationExecutor, handle, (AlterMaterializedTableRefreshOperation) op);
        } else if (op instanceof AlterMaterializedTableSuspendOperation) {
            return callAlterMaterializedTableSuspend(
                    operationExecutor, handle, (AlterMaterializedTableSuspendOperation) op);
        } else if (op instanceof AlterMaterializedTableResumeOperation) {
            return callAlterMaterializedTableResume(
                    operationExecutor, handle, (AlterMaterializedTableResumeOperation) op);
        } else if (op instanceof DropMaterializedTableOperation) {
            return callDropMaterializedTableOperation(
                    operationExecutor, handle, (DropMaterializedTableOperation) op);
        }

        throw new SqlExecutionException(
                String.format(
                        "Unsupported Operation %s for materialized table.", op.asSummaryString()));
    }

    private static ResultFetcher callCreateMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        CatalogMaterializedTable materializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            createMaterializedInContinuousMode(
                    operationExecutor, handle, createMaterializedTableOperation);
        } else {
            throw new SqlExecutionException(
                    "Only support create materialized table in continuous refresh mode currently.");
        }
        // Just return ok for unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private static void createMaterializedInContinuousMode(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        // create materialized table first
        operationExecutor.callExecutableOperation(handle, createMaterializedTableOperation);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        try {
            executeContinuousRefreshJob(
                    operationExecutor,
                    handle,
                    catalogMaterializedTable,
                    materializedTableIdentifier,
                    Collections.emptyMap(),
                    Optional.empty());
        } catch (Exception e) {
            // drop materialized table while submit flink streaming job occur exception. Thus, weak
            // atomicity is guaranteed
            LOG.warn(
                    "Submit continuous refresh job occur exception, drop materialized table {}.",
                    materializedTableIdentifier,
                    e);
            operationExecutor.callExecutableOperation(
                    handle, new DropMaterializedTableOperation(materializedTableIdentifier, true));
            throw e;
        }
    }

    private static ResultFetcher callAlterMaterializedTableSuspend(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableSuspendOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        CatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(operationExecutor, tableIdentifier);

        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS != materializedTable.getRefreshMode()) {
            throw new SqlExecutionException(
                    "Only support suspend continuous refresh job currently.");
        }

        ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(
                        materializedTable.getSerializedRefreshHandler(),
                        operationExecutor.getSessionContext().getUserClassloader());

        String savepointPath =
                stopJobWithSavepoint(operationExecutor, handle, refreshHandler.getJobId());

        ContinuousRefreshHandler updateRefreshHandler =
                new ContinuousRefreshHandler(
                        refreshHandler.getExecutionTarget(),
                        refreshHandler.getJobId(),
                        savepointPath);

        CatalogMaterializedTable updatedMaterializedTable =
                materializedTable.copy(
                        CatalogMaterializedTable.RefreshStatus.SUSPENDED,
                        materializedTable.getRefreshHandlerDescription().orElse(null),
                        serializeContinuousHandler(updateRefreshHandler));
        List<TableChange> tableChanges = new ArrayList<>();
        tableChanges.add(
                TableChange.modifyRefreshStatus(CatalogMaterializedTable.RefreshStatus.ACTIVATED));
        AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        tableIdentifier, tableChanges, updatedMaterializedTable);

        operationExecutor.callExecutableOperation(handle, alterMaterializedTableChangeOperation);

        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private static ResultFetcher callAlterMaterializedTableResume(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableResumeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                getCatalogMaterializedTable(operationExecutor, tableIdentifier);

        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS
                != catalogMaterializedTable.getRefreshMode()) {
            throw new SqlExecutionException(
                    "Only support resume continuous refresh job currently.");
        }

        ContinuousRefreshHandler continuousRefreshHandler =
                deserializeContinuousHandler(
                        catalogMaterializedTable.getSerializedRefreshHandler(),
                        operationExecutor.getSessionContext().getUserClassloader());
        Optional<String> restorePath = continuousRefreshHandler.getRestorePath();
        executeContinuousRefreshJob(
                operationExecutor,
                handle,
                catalogMaterializedTable,
                tableIdentifier,
                op.getDynamicOptions(),
                restorePath);

        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private static void executeContinuousRefreshJob(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CatalogMaterializedTable catalogMaterializedTable,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> dynamicOptions,
            Optional<String> restorePath) {
        // Set job name, runtime mode, checkpoint interval
        // TODO: Set minibatch related optimization options.
        Configuration customConfig = new Configuration();
        String jobName =
                String.format(
                        "Materialized_table_%s_continuous_refresh_job",
                        materializedTableIdentifier.asSerializableString());
        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, STREAMING);
        customConfig.set(CHECKPOINTING_INTERVAL, catalogMaterializedTable.getFreshness());
        restorePath.ifPresent(s -> customConfig.set(SAVEPOINT_PATH, s));

        String insertStatement =
                getInsertStatement(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getDefinitionQuery(),
                        dynamicOptions);
        try {
            // submit flink streaming job
            ResultFetcher resultFetcher =
                    operationExecutor.executeStatement(handle, customConfig, insertStatement);

            // get execution.target and jobId, currently doesn't support yarn and k8s, so doesn't
            // get clusterId
            List<RowData> results = fetchAllResults(resultFetcher);
            String jobId = results.get(0).getString(0).toString();
            String executeTarget =
                    operationExecutor.getSessionContext().getSessionConf().get(TARGET);
            ContinuousRefreshHandler continuousRefreshHandler =
                    new ContinuousRefreshHandler(executeTarget, jobId);
            byte[] serializedBytes = serializeContinuousHandler(continuousRefreshHandler);

            // update RefreshHandler to Catalog
            CatalogMaterializedTable updatedMaterializedTable =
                    catalogMaterializedTable.copy(
                            CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                            continuousRefreshHandler.asSummaryString(),
                            serializedBytes);
            List<TableChange> tableChanges = new ArrayList<>();
            tableChanges.add(
                    TableChange.modifyRefreshStatus(
                            CatalogMaterializedTable.RefreshStatus.ACTIVATED));
            tableChanges.add(
                    TableChange.modifyRefreshHandler(
                            continuousRefreshHandler.asSummaryString(), serializedBytes));

            AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                    new AlterMaterializedTableChangeOperation(
                            materializedTableIdentifier, tableChanges, updatedMaterializedTable);
            operationExecutor.callExecutableOperation(
                    handle, alterMaterializedTableChangeOperation);
        } catch (Exception e) {
            // drop materialized table while submit flink streaming job occur exception. Thus, weak
            // atomicity is guaranteed
            operationExecutor.callExecutableOperation(
                    handle, new DropMaterializedTableOperation(materializedTableIdentifier, true));
            // log and throw exception
            LOG.error(
                    "Submit continuous refresh job for materialized table {} occur exception.",
                    materializedTableIdentifier,
                    e);
            throw new SqlExecutionException(
                    String.format(
                            "Submit continuous refresh job for materialized table %s occur exception.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private static ResultFetcher callAlterMaterializedTableRefreshOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableRefreshOperation alterMaterializedTableRefreshOperation) {
        ObjectIdentifier materializedTableIdentifier =
                alterMaterializedTableRefreshOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(operationExecutor, materializedTableIdentifier);

        Map<String, String> partitionSpec =
                alterMaterializedTableRefreshOperation.getPartitionSpec();

        validatePartitionSpec(partitionSpec, materializedTable);

        // Set job name, runtime mode
        Configuration customConfig = new Configuration();
        String jobName =
                String.format(
                        "Materialized_table_%s_one_time_refresh_job",
                        materializedTableIdentifier.asSerializableString());
        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, BATCH);

        String insertStatement =
                getManuallyRefreshStatement(
                        materializedTableIdentifier.toString(),
                        materializedTable.getDefinitionQuery(),
                        partitionSpec);

        try {
            LOG.debug(
                    "Begin to manually refreshing the materialization table {}, statement: {}",
                    materializedTableIdentifier,
                    insertStatement);
            return operationExecutor.executeStatement(
                    handle, customConfig, insertStatement.toString());
        } catch (Exception e) {
            // log and throw exception
            LOG.error(
                    "Manually refreshing the materialization table {} occur exception.",
                    materializedTableIdentifier,
                    e);
            throw new SqlExecutionException(
                    String.format(
                            "Manually refreshing the materialization table %s occur exception.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private static void validatePartitionSpec(
            Map<String, String> partitionSpec, ResolvedCatalogMaterializedTable table) {
        ResolvedSchema schema = table.getResolvedSchema();
        Set<String> allPartitionKeys = new HashSet<>(table.getPartitionKeys());

        Set<String> unknownPartitionKeys = new HashSet<>();
        Set<String> nonStringPartitionKeys = new HashSet<>();

        for (String partitionKey : partitionSpec.keySet()) {
            if (!schema.getColumn(partitionKey).isPresent()) {
                unknownPartitionKeys.add(partitionKey);
                continue;
            }

            if (!schema.getColumn(partitionKey)
                    .get()
                    .getDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                nonStringPartitionKeys.add(partitionKey);
            }
        }

        if (!unknownPartitionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "The partition spec contains unknown partition keys:\n\n%s\n\nAll known partition keys are:\n\n%s",
                            String.join("\n", unknownPartitionKeys),
                            String.join("\n", allPartitionKeys)));
        }

        if (!nonStringPartitionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Currently, manually refreshing materialized table only supports specifying char and string type"
                                    + " partition keys. All specific partition keys with unsupported types are:\n\n%s",
                            String.join("\n", nonStringPartitionKeys)));
        }
    }

    @VisibleForTesting
    protected static String getManuallyRefreshStatement(
            String tableIdentifier, String query, Map<String, String> partitionSpec) {
        StringBuilder insertStatement =
                new StringBuilder(
                        String.format(
                                "INSERT OVERWRITE %s\n  SELECT * FROM (%s)",
                                tableIdentifier, query));
        if (!partitionSpec.isEmpty()) {
            insertStatement.append("\n  WHERE ");
            insertStatement.append(
                    partitionSpec.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "%s = '%s'", entry.getKey(), entry.getValue()))
                            .reduce((s1, s2) -> s1 + " AND " + s2)
                            .get());
        }

        return insertStatement.toString();
    }

    private static ResultFetcher callDropMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            DropMaterializedTableOperation dropMaterializedTableOperation) {
        ObjectIdentifier tableIdentifier = dropMaterializedTableOperation.getTableIdentifier();
        boolean tableExists = operationExecutor.tableExists(tableIdentifier);
        if (!tableExists) {
            if (dropMaterializedTableOperation.isIfExists()) {
                LOG.info(
                        "Materialized table {} does not exists, skip the drop operation.",
                        tableIdentifier);
                return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
            } else {
                throw new ValidationException(
                        String.format(
                                "Materialized table with identifier %s does not exist.",
                                tableIdentifier));
            }
        }

        CatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(operationExecutor, tableIdentifier);

        if (CatalogMaterializedTable.RefreshStatus.ACTIVATED
                == materializedTable.getRefreshStatus()) {
            ContinuousRefreshHandler refreshHandler =
                    deserializeContinuousHandler(
                            materializedTable.getSerializedRefreshHandler(),
                            operationExecutor.getSessionContext().getUserClassloader());
            // get job running status
            JobStatus jobStatus = getJobStatus(operationExecutor, handle, refreshHandler);
            if (!jobStatus.isTerminalState()) {
                try {
                    cancelJob(operationExecutor, handle, refreshHandler.getJobId());
                } catch (Exception e) {
                    jobStatus = getJobStatus(operationExecutor, handle, refreshHandler);
                    if (!jobStatus.isTerminalState()) {
                        throw new SqlExecutionException(
                                String.format(
                                        "Failed to drop the materialized table %s because the continuous refresh job %s could not be canceled."
                                                + " The current status of the continuous refresh job is %s.",
                                        tableIdentifier, refreshHandler.getJobId(), jobStatus),
                                e);
                    } else {
                        LOG.warn(
                                "An exception occurred while canceling the continuous refresh job {} for materialized table {},"
                                        + " but since the job is in a terminal state, skip the cancel operation.",
                                refreshHandler.getJobId(),
                                tableIdentifier);
                    }
                }
            } else {
                LOG.info(
                        "No need to cancel the continuous refresh job {} for materialized table {} as it is not currently running.",
                        refreshHandler.getJobId(),
                        tableIdentifier);
            }
        } else if (CatalogMaterializedTable.RefreshStatus.INITIALIZING
                == materializedTable.getRefreshStatus()) {
            throw new ValidationException(
                    String.format(
                            "Current refresh status of materialized table %s is initializing, skip the drop operation.",
                            tableIdentifier.asSerializableString()));
        }

        operationExecutor.callExecutableOperation(handle, dropMaterializedTableOperation);

        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private static JobStatus getJobStatus(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ContinuousRefreshHandler refreshHandler) {
        ResultFetcher resultFetcher =
                operationExecutor.callDescribeJobOperation(
                        operationExecutor.getTableEnvironment(),
                        handle,
                        new DescribeJobOperation(refreshHandler.getJobId()));
        List<RowData> result = fetchAllResults(resultFetcher);
        String jobStatus = result.get(0).getString(2).toString();
        return JobStatus.valueOf(jobStatus);
    }

    private static void cancelJob(
            OperationExecutor operationExecutor, OperationHandle handle, String jobId) {
        operationExecutor.callStopJobOperation(
                operationExecutor.getTableEnvironment(),
                handle,
                new StopJobOperation(jobId, false, false));
    }

    private static String stopJobWithSavepoint(
            OperationExecutor executor, OperationHandle handle, String jobId) {
        // check savepoint dir is configured
        Optional<String> savepointDir =
                executor.getSessionContext().getSessionConf().getOptional(SAVEPOINT_DIRECTORY);
        if (!savepointDir.isPresent()) {
            throw new ValidationException(
                    "Savepoint directory is not configured, can't stop job with savepoint.");
        }
        ResultFetcher resultFetcher =
                executor.callStopJobOperation(
                        executor.getTableEnvironment(),
                        handle,
                        new StopJobOperation(jobId, true, false));
        List<RowData> results = fetchAllResults(resultFetcher);
        return results.get(0).getString(0).toString();
    }

    private static ContinuousRefreshHandler deserializeContinuousHandler(
            byte[] serializedRefreshHandler, ClassLoader classLoader) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                    serializedRefreshHandler, classLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new SqlExecutionException(
                    "Deserialize ContinuousRefreshHandler occur exception.", e);
        }
    }

    private static byte[] serializeContinuousHandler(ContinuousRefreshHandler refreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.serialize(refreshHandler);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    "Serialize ContinuousRefreshHandler occur exception.", e);
        }
    }

    private static ResolvedCatalogMaterializedTable getCatalogMaterializedTable(
            OperationExecutor operationExecutor, ObjectIdentifier tableIdentifier) {
        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable =
                operationExecutor.getTable(tableIdentifier);
        if (MATERIALIZED_TABLE != resolvedCatalogBaseTable.getTableKind()) {
            throw new ValidationException(
                    String.format(
                            "Table %s is not a materialized table, does not support materialized table related operation.",
                            tableIdentifier));
        }

        return (ResolvedCatalogMaterializedTable) resolvedCatalogBaseTable;
    }

    /** Generate insert statement for materialized table. */
    @VisibleForTesting
    protected static String getInsertStatement(
            ObjectIdentifier materializedTableIdentifier,
            String definitionQuery,
            Map<String, String> dynamicOptions) {
        StringBuilder builder =
                new StringBuilder(
                        String.format(
                                "INSERT INTO %s",
                                materializedTableIdentifier.asSerializableString()));

        if (!dynamicOptions.isEmpty()) {
            String hints =
                    dynamicOptions.entrySet().stream()
                            .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(", "));
            builder.append(String.format(" /*+ OPTIONS(%s) */", hints));
        }

        builder.append("\n").append(definitionQuery);
        return builder.toString();
    }

    private static List<RowData> fetchAllResults(ResultFetcher resultFetcher) {
        Long token = 0L;
        List<RowData> results = new ArrayList<>();
        while (token != null) {
            ResultSet result = resultFetcher.fetchResults(token, Integer.MAX_VALUE);
            results.addAll(result.getData());
            token = result.getNextToken();
        }
        return results;
    }
}
