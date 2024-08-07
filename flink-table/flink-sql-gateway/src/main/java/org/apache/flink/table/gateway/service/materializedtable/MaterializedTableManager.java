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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions;
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
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URLClassLoader;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.SCHEDULE_TIME_DATE_FORMATTER_DEFAULT;
import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;
import static org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil.WORKFLOW_SCHEDULER_PREFIX;
import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getEndpointConfig;
import static org.apache.flink.table.gateway.service.utils.Constants.CLUSTER_INFO;
import static org.apache.flink.table.gateway.service.utils.Constants.JOB_ID;
import static org.apache.flink.table.utils.DateTimeUtils.formatTimestampStringWithOffset;
import static org.apache.flink.table.utils.IntervalFreshnessUtils.convertFreshnessToCron;
import static org.apache.flink.table.utils.IntervalFreshnessUtils.convertFreshnessToDuration;

/** Manager is responsible for execute the {@link MaterializedTableOperation}. */
@Internal
public class MaterializedTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedTableManager.class);

    private final URLClassLoader userCodeClassLoader;

    private final @Nullable WorkflowScheduler<? extends RefreshHandler> workflowScheduler;

    private final String restEndpointUrl;

    public MaterializedTableManager(
            Configuration configuration, URLClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
        this.restEndpointUrl = buildRestEndpointUrl(configuration);
        this.workflowScheduler = buildWorkflowScheduler(configuration, userCodeClassLoader);
    }

    private String buildRestEndpointUrl(Configuration configuration) {
        Configuration restEndpointConfig =
                Configuration.fromMap(
                        getEndpointConfig(configuration, SqlGatewayRestEndpointFactory.IDENTIFIER));
        String address = restEndpointConfig.get(SqlGatewayRestOptions.ADDRESS);
        int port = restEndpointConfig.get(SqlGatewayRestOptions.PORT);

        return String.format("http://%s:%s", address, port);
    }

    private WorkflowScheduler<? extends RefreshHandler> buildWorkflowScheduler(
            Configuration configuration, URLClassLoader userCodeClassLoader) {
        return WorkflowSchedulerFactoryUtil.createWorkflowScheduler(
                configuration, userCodeClassLoader);
    }

    public void open() throws Exception {
        if (workflowScheduler != null) {
            workflowScheduler.open();
        }
    }

    public void close() throws Exception {
        if (workflowScheduler != null) {
            workflowScheduler.close();
        }
    }

    public ResultFetcher callMaterializedTableOperation(
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

    private ResultFetcher callCreateMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        CatalogMaterializedTable materializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            createMaterializedTableInContinuousMode(
                    operationExecutor, handle, createMaterializedTableOperation);
        } else {
            createMaterializedTableInFullMode(
                    operationExecutor, handle, createMaterializedTableOperation);
        }
        // Just return ok for unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private void createMaterializedTableInContinuousMode(
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
            operationExecutor.callExecutableOperation(
                    handle, new DropMaterializedTableOperation(materializedTableIdentifier, true));
            throw new SqlExecutionException(
                    String.format(
                            "Submit continuous refresh job for materialized table %s occur exception.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private void createMaterializedTableInFullMode(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        if (workflowScheduler == null) {
            throw new SqlExecutionException(
                    "The workflow scheduler must be configured when creating materialized table in full refresh mode.");
        }
        // create materialized table first
        operationExecutor.callExecutableOperation(handle, createMaterializedTableOperation);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        // convert duration to cron expression
        String cronExpression =
                convertFreshnessToCron(catalogMaterializedTable.getDefinitionFreshness());
        // create full refresh job
        CreateRefreshWorkflow createRefreshWorkflow =
                new CreatePeriodicRefreshWorkflow(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getDefinitionQuery(),
                        cronExpression,
                        getSessionInitializationConf(operationExecutor),
                        Collections.emptyMap(),
                        restEndpointUrl);

        try {
            RefreshHandler refreshHandler =
                    workflowScheduler.createRefreshWorkflow(createRefreshWorkflow);
            RefreshHandlerSerializer refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            byte[] serializedRefreshHandler = refreshHandlerSerializer.serialize(refreshHandler);

            updateRefreshHandler(
                    operationExecutor,
                    handle,
                    materializedTableIdentifier,
                    catalogMaterializedTable,
                    CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                    refreshHandler.asSummaryString(),
                    serializedRefreshHandler);
        } catch (Exception e) {
            // drop materialized table while create refresh workflow occur exception. Thus, weak
            // atomicity is guaranteed
            operationExecutor.callExecutableOperation(
                    handle, new DropMaterializedTableOperation(materializedTableIdentifier, true));
            throw new SqlExecutionException(
                    String.format(
                            "Failed to create refresh workflow for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private ResultFetcher callAlterMaterializedTableSuspend(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableSuspendOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        CatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(operationExecutor, tableIdentifier);

        // Initialization phase doesn't support resume operation.
        if (CatalogMaterializedTable.RefreshStatus.INITIALIZING
                == materializedTable.getRefreshStatus()) {
            throw new SqlExecutionException(
                    String.format(
                            "Materialized table %s is being initialized and does not support suspend operation.",
                            tableIdentifier));
        }

        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            suspendContinuousRefreshJob(
                    operationExecutor, handle, tableIdentifier, materializedTable);
        } else {
            suspendRefreshWorkflow(operationExecutor, handle, tableIdentifier, materializedTable);
        }
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private void suspendContinuousRefreshJob(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable materializedTable) {
        try {
            ContinuousRefreshHandler refreshHandler =
                    deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());

            if (CatalogMaterializedTable.RefreshStatus.SUSPENDED
                    == materializedTable.getRefreshStatus()) {
                throw new SqlExecutionException(
                        String.format(
                                "Materialized table %s continuous refresh job has been suspended, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }

            String savepointPath =
                    stopJobWithSavepoint(operationExecutor, handle, refreshHandler.getJobId());

            ContinuousRefreshHandler updateRefreshHandler =
                    new ContinuousRefreshHandler(
                            refreshHandler.getExecutionTarget(),
                            refreshHandler.getJobId(),
                            savepointPath);

            updateRefreshHandler(
                    operationExecutor,
                    handle,
                    tableIdentifier,
                    materializedTable,
                    CatalogMaterializedTable.RefreshStatus.SUSPENDED,
                    updateRefreshHandler.asSummaryString(),
                    serializeContinuousHandler(updateRefreshHandler));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to suspend the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void suspendRefreshWorkflow(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable materializedTable) {
        if (CatalogMaterializedTable.RefreshStatus.SUSPENDED
                == materializedTable.getRefreshStatus()) {
            throw new SqlExecutionException(
                    String.format(
                            "Materialized table %s refresh workflow has been suspended.",
                            tableIdentifier));
        }

        if (workflowScheduler == null) {
            throw new SqlExecutionException(
                    "The workflow scheduler must be configured when suspending materialized table in full refresh mode.");
        }

        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            materializedTable.getSerializedRefreshHandler(), userCodeClassLoader);
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new SuspendRefreshWorkflow(refreshHandler);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    operationExecutor,
                    handle,
                    tableIdentifier,
                    materializedTable,
                    CatalogMaterializedTable.RefreshStatus.SUSPENDED,
                    refreshHandler.asSummaryString(),
                    materializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to suspend the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private ResultFetcher callAlterMaterializedTableResume(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableResumeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                getCatalogMaterializedTable(operationExecutor, tableIdentifier);

        // Initialization phase doesn't support resume operation.
        if (CatalogMaterializedTable.RefreshStatus.INITIALIZING
                == catalogMaterializedTable.getRefreshStatus()) {
            throw new SqlExecutionException(
                    String.format(
                            "Materialized table %s is being initialized and does not support resume operation.",
                            tableIdentifier));
        }

        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS
                == catalogMaterializedTable.getRefreshMode()) {
            resumeContinuousRefreshJob(
                    operationExecutor,
                    handle,
                    tableIdentifier,
                    catalogMaterializedTable,
                    op.getDynamicOptions());
        } else {
            resumeRefreshWorkflow(
                    operationExecutor,
                    handle,
                    tableIdentifier,
                    catalogMaterializedTable,
                    op.getDynamicOptions());
        }

        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private void resumeContinuousRefreshJob(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(
                        catalogMaterializedTable.getSerializedRefreshHandler());

        // Repeated resume continuous refresh job is not supported
        if (CatalogMaterializedTable.RefreshStatus.ACTIVATED
                == catalogMaterializedTable.getRefreshStatus()) {
            JobStatus jobStatus = getJobStatus(operationExecutor, handle, refreshHandler);
            if (!jobStatus.isGloballyTerminalState()) {
                throw new SqlExecutionException(
                        String.format(
                                "Materialized table %s continuous refresh job has been resumed, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }
        }

        Optional<String> restorePath = refreshHandler.getRestorePath();
        try {
            executeContinuousRefreshJob(
                    operationExecutor,
                    handle,
                    catalogMaterializedTable,
                    tableIdentifier,
                    dynamicOptions,
                    restorePath);
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to resume the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void resumeRefreshWorkflow(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        // Repeated resume refresh workflow is not supported
        if (CatalogMaterializedTable.RefreshStatus.ACTIVATED
                == catalogMaterializedTable.getRefreshStatus()) {
            throw new SqlExecutionException(
                    String.format(
                            "Materialized table %s refresh workflow has been resumed.",
                            tableIdentifier));
        }

        if (workflowScheduler == null) {
            throw new SqlExecutionException(
                    "The workflow scheduler must be configured when resuming materialized table in full refresh mode.");
        }
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            userCodeClassLoader);
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new ResumeRefreshWorkflow(refreshHandler, dynamicOptions);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    operationExecutor,
                    handle,
                    tableIdentifier,
                    catalogMaterializedTable,
                    CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                    refreshHandler.asSummaryString(),
                    catalogMaterializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to resume the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void executeContinuousRefreshJob(
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
        restorePath.ifPresent(s -> customConfig.set(SAVEPOINT_PATH, s));

        // Do not override the user-defined checkpoint interval
        if (!operationExecutor
                .getSessionContext()
                .getSessionConf()
                .contains(CheckpointingOptions.CHECKPOINTING_INTERVAL)) {
            customConfig.set(
                    CheckpointingOptions.CHECKPOINTING_INTERVAL,
                    catalogMaterializedTable.getFreshness());
        }

        String insertStatement =
                getInsertStatement(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getDefinitionQuery(),
                        dynamicOptions);
        // submit flink streaming job
        ResultFetcher resultFetcher =
                operationExecutor.executeStatement(handle, customConfig, insertStatement);

        // get execution.target and jobId, currently doesn't support yarn and k8s, so doesn't
        // get clusterId
        List<RowData> results = fetchAllResults(resultFetcher);
        String jobId = results.get(0).getString(0).toString();
        String executeTarget = operationExecutor.getSessionContext().getSessionConf().get(TARGET);
        ContinuousRefreshHandler continuousRefreshHandler =
                new ContinuousRefreshHandler(executeTarget, jobId);
        byte[] serializedBytes = serializeContinuousHandler(continuousRefreshHandler);

        updateRefreshHandler(
                operationExecutor,
                handle,
                materializedTableIdentifier,
                catalogMaterializedTable,
                CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                continuousRefreshHandler.asSummaryString(),
                serializedBytes);
    }

    private ResultFetcher callAlterMaterializedTableRefreshOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            AlterMaterializedTableRefreshOperation alterMaterializedTableRefreshOperation) {
        ObjectIdentifier materializedTableIdentifier =
                alterMaterializedTableRefreshOperation.getTableIdentifier();

        Map<String, String> partitionSpec =
                alterMaterializedTableRefreshOperation.getPartitionSpec();

        return refreshMaterializedTable(
                operationExecutor,
                handle,
                materializedTableIdentifier,
                partitionSpec,
                Collections.emptyMap(),
                false,
                null);
    }

    public ResultFetcher refreshMaterializedTable(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> staticPartitions,
            Map<String, String> dynamicOptions,
            boolean isPeriodic,
            @Nullable String scheduleTime) {
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(operationExecutor, materializedTableIdentifier);
        Map<String, String> refreshPartitions =
                isPeriodic
                        ? getPeriodRefreshPartition(
                                scheduleTime,
                                materializedTable.getDefinitionFreshness(),
                                materializedTableIdentifier,
                                materializedTable.getOptions(),
                                operationExecutor
                                        .getTableEnvironment()
                                        .getConfig()
                                        .getLocalTimeZone())
                        : staticPartitions;

        validatePartitionSpec(refreshPartitions, materializedTable);

        // Set job name, runtime mode
        Configuration customConfig = new Configuration();
        String jobName =
                isPeriodic
                        ? String.format(
                                "Materialized_table_%s_periodic_refresh_job",
                                materializedTableIdentifier.asSerializableString())
                        : String.format(
                                "Materialized_table_%s_one_time_refresh_job",
                                materializedTableIdentifier.asSerializableString());

        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, BATCH);

        String insertStatement =
                getRefreshStatement(
                        materializedTableIdentifier,
                        materializedTable.getDefinitionQuery(),
                        refreshPartitions,
                        dynamicOptions);

        try {
            LOG.info(
                    "Begin to refreshing the materialized table {}, statement: {}",
                    materializedTableIdentifier,
                    insertStatement);
            ResultFetcher resultFetcher =
                    operationExecutor.executeStatement(handle, customConfig, insertStatement);

            List<RowData> results = fetchAllResults(resultFetcher);
            String jobId = results.get(0).getString(0).toString();
            String executeTarget =
                    operationExecutor.getSessionContext().getSessionConf().get(TARGET);
            Map<StringData, StringData> clusterInfo = new HashMap<>();
            clusterInfo.put(
                    StringData.fromString(TARGET.key()), StringData.fromString(executeTarget));
            // TODO get clusterId

            return ResultFetcher.fromResults(
                    handle,
                    ResolvedSchema.of(
                            Column.physical(JOB_ID, DataTypes.STRING()),
                            Column.physical(
                                    CLUSTER_INFO,
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))),
                    Collections.singletonList(
                            GenericRowData.of(
                                    StringData.fromString(jobId),
                                    new GenericMapData(clusterInfo))));
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Refreshing the materialized table %s occur exception.",
                            materializedTableIdentifier),
                    e);
        }
    }

    @VisibleForTesting
    static Map<String, String> getPeriodRefreshPartition(
            String scheduleTime,
            IntervalFreshness freshness,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> materializedTableOptions,
            ZoneId localZoneId) {
        if (scheduleTime == null) {
            throw new ValidationException(
                    String.format(
                            "The scheduler time must not be null during the periodic refresh of the materialized table %s.",
                            materializedTableIdentifier));
        }

        Set<String> partitionFields =
                materializedTableOptions.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet());
        Map<String, String> refreshPartitions = new HashMap<>();
        for (String partKey : partitionFields) {
            String partField =
                    partKey.substring(
                            PARTITION_FIELDS.length() + 1,
                            partKey.length() - (DATE_FORMATTER.length() + 1));
            String partFieldFormatter = materializedTableOptions.get(partKey);

            String partFiledValue =
                    formatTimestampStringWithOffset(
                            scheduleTime,
                            SCHEDULE_TIME_DATE_FORMATTER_DEFAULT,
                            partFieldFormatter,
                            TimeZone.getTimeZone(localZoneId),
                            -convertFreshnessToDuration(freshness).toMillis());
            if (partFiledValue == null) {
                throw new SqlExecutionException(
                        String.format(
                                "Failed to parse a valid partition value for the field '%s' in materialized table %s using the scheduler time '%s' based on the date format '%s'.",
                                partField,
                                materializedTableIdentifier.asSerializableString(),
                                scheduleTime,
                                SCHEDULE_TIME_DATE_FORMATTER_DEFAULT));
            }
            refreshPartitions.put(partField, partFiledValue);
        }

        return refreshPartitions;
    }

    private void validatePartitionSpec(
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
                            "Currently, refreshing materialized table only supports referring to char, varchar and string type"
                                    + " partition keys. All specified partition keys in partition specs with unsupported types are:\n\n%s",
                            String.join("\n", nonStringPartitionKeys)));
        }
    }

    @VisibleForTesting
    protected static String getRefreshStatement(
            ObjectIdentifier tableIdentifier,
            String definitionQuery,
            Map<String, String> partitionSpec,
            Map<String, String> dynamicOptions) {
        String tableIdentifierWithDynamicOptions =
                generateTableWithDynamicOptions(tableIdentifier, dynamicOptions);
        StringBuilder insertStatement =
                new StringBuilder(
                        String.format(
                                "INSERT OVERWRITE %s\n  SELECT * FROM (%s)",
                                tableIdentifierWithDynamicOptions, definitionQuery));
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

    private ResultFetcher callDropMaterializedTableOperation(
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
        CatalogMaterializedTable.RefreshMode refreshMode = materializedTable.getRefreshMode();
        CatalogMaterializedTable.RefreshStatus refreshStatus = materializedTable.getRefreshStatus();
        if (CatalogMaterializedTable.RefreshStatus.ACTIVATED == refreshStatus
                || CatalogMaterializedTable.RefreshStatus.SUSPENDED == refreshStatus) {
            if (CatalogMaterializedTable.RefreshMode.FULL == refreshMode) {
                deleteRefreshWorkflow(tableIdentifier, materializedTable);
            } else if (CatalogMaterializedTable.RefreshMode.CONTINUOUS == refreshMode
                    && CatalogMaterializedTable.RefreshStatus.ACTIVATED == refreshStatus) {
                cancelContinuousRefreshJob(
                        operationExecutor, handle, tableIdentifier, materializedTable);
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

    private void cancelContinuousRefreshJob(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable materializedTable) {
        ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());
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
    }

    private void deleteRefreshWorkflow(
            ObjectIdentifier tableIdentifier, CatalogMaterializedTable catalogMaterializedTable) {
        if (workflowScheduler == null) {
            throw new SqlExecutionException(
                    "The workflow scheduler must be configured when dropping materialized table in full refresh mode.");
        }
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            userCodeClassLoader);
            DeleteRefreshWorkflow deleteRefreshWorkflow = new DeleteRefreshWorkflow(refreshHandler);
            workflowScheduler.deleteRefreshWorkflow(deleteRefreshWorkflow);
        } catch (Exception e) {
            throw new SqlExecutionException(
                    String.format(
                            "Failed to delete the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    /**
     * Retrieves the session configuration for initializing the periodic refresh job. The function
     * filters out default context configurations and removes unnecessary configurations such as
     * resources download directory and workflow scheduler related configurations.
     *
     * @param operationExecutor The OperationExecutor instance used to access the session context.
     * @return A Map containing the session configurations for initializing session for executing
     *     the periodic refresh job.
     */
    private Map<String, String> getSessionInitializationConf(OperationExecutor operationExecutor) {
        Map<String, String> sessionConf =
                operationExecutor.getSessionContext().getSessionConf().toMap();

        // we only keep the session conf that is not in the default context or the conf value is
        // different from the default context.
        Map<String, String> defaultContextConf =
                operationExecutor.getSessionContext().getDefaultContext().getFlinkConfig().toMap();
        sessionConf
                .entrySet()
                .removeIf(
                        entry -> {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            return defaultContextConf.containsKey(key)
                                    && defaultContextConf.get(key).equals(value);
                        });

        // remove useless conf
        sessionConf.remove(TableConfigOptions.RESOURCES_DOWNLOAD_DIR.key());
        sessionConf.keySet().removeIf(key -> key.startsWith(WORKFLOW_SCHEDULER_PREFIX));

        return sessionConf;
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

    private ContinuousRefreshHandler deserializeContinuousHandler(byte[] serializedRefreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                    serializedRefreshHandler, userCodeClassLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new SqlExecutionException(
                    "Deserialize ContinuousRefreshHandler occur exception.", e);
        }
    }

    private byte[] serializeContinuousHandler(ContinuousRefreshHandler refreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.serialize(refreshHandler);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    "Serialize ContinuousRefreshHandler occur exception.", e);
        }
    }

    private ResolvedCatalogMaterializedTable getCatalogMaterializedTable(
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

    private void updateRefreshHandler(
            OperationExecutor operationExecutor,
            OperationHandle operationHandle,
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable,
            CatalogMaterializedTable.RefreshStatus refreshStatus,
            String refreshHandlerSummary,
            byte[] serializedRefreshHandler) {
        CatalogMaterializedTable updatedMaterializedTable =
                catalogMaterializedTable.copy(
                        refreshStatus, refreshHandlerSummary, serializedRefreshHandler);
        List<TableChange> tableChanges = new ArrayList<>();
        tableChanges.add(TableChange.modifyRefreshStatus(refreshStatus));
        tableChanges.add(
                TableChange.modifyRefreshHandler(refreshHandlerSummary, serializedRefreshHandler));
        AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        materializedTableIdentifier, tableChanges, updatedMaterializedTable);
        // update RefreshHandler to Catalog
        operationExecutor.callExecutableOperation(
                operationHandle, alterMaterializedTableChangeOperation);
    }

    /** Generate insert statement for materialized table. */
    @VisibleForTesting
    protected static String getInsertStatement(
            ObjectIdentifier materializedTableIdentifier,
            String definitionQuery,
            Map<String, String> dynamicOptions) {

        return String.format(
                "INSERT INTO %s\n%s",
                generateTableWithDynamicOptions(materializedTableIdentifier, dynamicOptions),
                definitionQuery);
    }

    private static String generateTableWithDynamicOptions(
            ObjectIdentifier objectIdentifier, Map<String, String> dynamicOptions) {
        StringBuilder builder = new StringBuilder(objectIdentifier.asSerializableString());

        if (!dynamicOptions.isEmpty()) {
            String hints =
                    dynamicOptions.entrySet().stream()
                            .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(", "));
            builder.append(String.format(" /*+ OPTIONS(%s) */", hints));
        }

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
