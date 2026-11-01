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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.StaticResultProvider;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
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
import org.apache.flink.table.delegation.materializedtable.MaterializedTableExecutor;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableResumeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
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
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
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
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.SCHEDULE_TIME_DATE_FORMATTER_DEFAULT;
import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;
import static org.apache.flink.table.catalog.IntervalFreshness.convertFreshnessToCron;
import static org.apache.flink.table.utils.DateTimeUtils.formatTimestampStringWithOffset;

/** Default executor for {@link MaterializedTableOperation}s. */
@Internal
public class DefaultMaterializedTableExecutor implements MaterializedTableExecutor {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMaterializedTableExecutor.class);

    private static final String LEFT_IN_INITIALIZING_STATUS =
            " The table was left in INITIALIZING status. No materialized table statement can drop a"
                    + " table in this status or move it out of it; remove it from the catalog directly.";

    private final ExecutableOperation.Context operationCtx;
    private final MaterializedTableJobSubmitter jobSubmitter;
    private final RefreshJobController refreshJobController;

    public DefaultMaterializedTableExecutor(
            ExecutableOperation.Context operationCtx, MaterializedTableJobSubmitter jobSubmitter) {
        this.operationCtx = operationCtx;
        this.jobSubmitter = jobSubmitter;
        this.refreshJobController =
                new RefreshJobController(
                        operationCtx.getTableConfig(),
                        operationCtx.getResourceManager().getUserClassLoader());
    }

    @Override
    public TableResultInternal execute(MaterializedTableOperation op) {
        try {
            if (op instanceof CreateMaterializedTableOperation) {
                return callCreateMaterializedTableOperation((CreateMaterializedTableOperation) op);
            } else if (op instanceof AlterMaterializedTableRefreshOperation) {
                return callAlterMaterializedTableRefreshOperation(
                        (AlterMaterializedTableRefreshOperation) op);
            } else if (op instanceof AlterMaterializedTableSuspendOperation) {
                return callAlterMaterializedTableSuspend(
                        (AlterMaterializedTableSuspendOperation) op);
            } else if (op instanceof AlterMaterializedTableResumeOperation) {
                return callAlterMaterializedTableResume((AlterMaterializedTableResumeOperation) op);
            } else if (op instanceof DropMaterializedTableOperation) {
                return callDropMaterializedTableOperation((DropMaterializedTableOperation) op);
            } else if (op instanceof AlterMaterializedTableChangeOperation) {
                return callAlterMaterializedTableChangeOperation(
                        (AlterMaterializedTableChangeOperation) op);
            } else if (op instanceof ConvertTableToMaterializedTableOperation) {
                return callConvertTableToMaterializedTableOperation(
                        (ConvertTableToMaterializedTableOperation) op);
            }

            throw new TableException(
                    String.format(
                            "Unsupported Operation %s for materialized table.",
                            op.asSummaryString()));
        } finally {
            refreshJobController.releaseRetrievedJobClients();
        }
    }

    private TableResultInternal callCreateMaterializedTableOperation(
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        ResolvedCatalogMaterializedTable materializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            checkSubmissionTargetBeforeChanges();
            createMaterializedTableInContinuousMode(createMaterializedTableOperation);
        } else {
            createMaterializedTableInFullMode(
                    createMaterializedTableOperation,
                    requireRefreshWorkflowContext(
                            "The workflow scheduler must be configured when creating materialized table in full refresh mode."));
        }
        // Just return ok to unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return TABLE_RESULT_OK;
    }

    private void createMaterializedTableInContinuousMode(
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        // create materialized table first
        createMaterializedTableOperation.execute(operationCtx);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        try {
            executeContinuousRefreshJobOrCancelIt(
                    catalogMaterializedTable, materializedTableIdentifier);
        } catch (Exception e) {
            // drop materialized table if submitting the Flink streaming job encounters an
            // exception. Thus, weak atomicity is guaranteed
            cleanUpAfterFailure(
                    () ->
                            new DropMaterializedTableOperation(materializedTableIdentifier, true)
                                    .execute(operationCtx),
                    e,
                    String.format(
                            "Failed to submit continuous refresh job for materialized table %s, and dropping the table failed as well."
                                    + LEFT_IN_INITIALIZING_STATUS,
                            materializedTableIdentifier));
            throw new TableException(
                    String.format(
                            "Failed to submit continuous refresh job for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private void createMaterializedTableInFullMode(
            CreateMaterializedTableOperation createMaterializedTableOperation,
            RefreshWorkflowContext refreshWorkflowContext) {
        // create materialized table first
        createMaterializedTableOperation.execute(operationCtx);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        try {
            createPeriodicRefreshWorkflow(
                    materializedTableIdentifier, catalogMaterializedTable, refreshWorkflowContext);
        } catch (Exception e) {
            // drop materialized table if creating the refresh workflow encounters an exception, so
            // weak atomicity is guaranteed
            cleanUpAfterFailure(
                    () ->
                            new DropMaterializedTableOperation(materializedTableIdentifier, true)
                                    .execute(operationCtx),
                    e,
                    String.format(
                            "Failed to create refresh workflow for materialized table %s, and dropping the table failed as well."
                                    + LEFT_IN_INITIALIZING_STATUS,
                            materializedTableIdentifier));
            throw new TableException(
                    String.format(
                            "Failed to create refresh workflow for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private TableResultInternal callConvertTableToMaterializedTableOperation(
            ConvertTableToMaterializedTableOperation convertOperation) {
        ResolvedCatalogMaterializedTable materializedTable =
                convertOperation.getMaterializedTable();
        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            convertTableToMaterializedTableInContinuousMode(convertOperation);
        } else {
            convertTableToMaterializedTableInFullMode(
                    convertOperation,
                    requireRefreshWorkflowContext(
                            "The workflow scheduler must be configured when converting a table to a materialized table in full refresh mode."));
        }
        // Just return ok for unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return TABLE_RESULT_OK;
    }

    private void convertTableToMaterializedTableInContinuousMode(
            ConvertTableToMaterializedTableOperation convertOperation) {
        checkSubmissionTargetBeforeChanges();
        // swap the catalog entry from a regular table to a materialized table first
        convertOperation.execute(operationCtx);

        ObjectIdentifier materializedTableIdentifier = convertOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                convertOperation.getMaterializedTable();

        try {
            executeContinuousRefreshJob(
                    catalogMaterializedTable,
                    materializedTableIdentifier,
                    Map.of(),
                    Optional.empty());
        } catch (Exception e) {
            suspendMaterializedTable(materializedTableIdentifier, catalogMaterializedTable);
            throw new TableException(
                    String.format(
                            "Failed to start the continuous refresh job when converting table %s to a materialized table. "
                                    + "The table was converted and left in SUSPENDED status; resume it once the issue is resolved.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private void convertTableToMaterializedTableInFullMode(
            ConvertTableToMaterializedTableOperation convertOperation,
            RefreshWorkflowContext refreshWorkflowContext) {
        // swap the catalog entry from a regular table to a materialized table first
        convertOperation.execute(operationCtx);

        ObjectIdentifier materializedTableIdentifier = convertOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                convertOperation.getMaterializedTable();

        try {
            createPeriodicRefreshWorkflow(
                    materializedTableIdentifier, catalogMaterializedTable, refreshWorkflowContext);
        } catch (Exception e) {
            suspendMaterializedTable(materializedTableIdentifier, catalogMaterializedTable);
            throw new TableException(
                    String.format(
                            "Failed to create the refresh workflow when converting table %s to a materialized table. "
                                    + "The table was converted and left in SUSPENDED status; resume it once the issue is resolved.",
                            materializedTableIdentifier),
                    e);
        }
    }

    /**
     * Creates the periodic refresh workflow for a full-mode materialized table and records the
     * resulting handler with {@code ACTIVATED} status.
     */
    private void createPeriodicRefreshWorkflow(
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            RefreshWorkflowContext refreshWorkflowContext)
            throws Exception {
        final WorkflowScheduler<? extends RefreshHandler> workflowScheduler =
                refreshWorkflowContext.scheduler();
        final IntervalFreshness freshness = catalogMaterializedTable.getDefinitionFreshness();
        final String cronExpression = convertFreshnessToCron(freshness);
        final CreateRefreshWorkflow createRefreshWorkflow =
                new CreatePeriodicRefreshWorkflow(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getExpandedQuery(),
                        cronExpression,
                        refreshWorkflowContext.sessionInitializationConf(),
                        Map.of(),
                        refreshWorkflowContext.restEndpointUrl());

        final RefreshHandler refreshHandler =
                workflowScheduler.createRefreshWorkflow(createRefreshWorkflow);
        final RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        final byte[] serializedRefreshHandler = refreshHandlerSerializer.serialize(refreshHandler);

        updateRefreshHandler(
                materializedTableIdentifier,
                catalogMaterializedTable,
                RefreshStatus.ACTIVATED,
                refreshHandler.asSummaryString(),
                serializedRefreshHandler);
    }

    /** Sets a materialized table refresh status to {@code SUSPENDED}. */
    private void suspendMaterializedTable(
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable) {
        AlterMaterializedTableChangeOperation alterOperation =
                new AlterMaterializedTableChangeOperation(
                        materializedTableIdentifier,
                        oldTable ->
                                List.of(TableChange.modifyRefreshStatus(RefreshStatus.SUSPENDED)),
                        catalogMaterializedTable);
        alterOperation.execute(operationCtx);
    }

    private TableResultInternal callAlterMaterializedTableSuspend(
            AlterMaterializedTableSuspendOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(tableIdentifier);

        // Initialization phase doesn't support suspend operation.
        if (RefreshStatus.INITIALIZING == materializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support suspend operation.",
                            tableIdentifier));
        }

        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            suspendContinuousRefreshJob(tableIdentifier, materializedTable);
        } else {
            suspendRefreshWorkflow(tableIdentifier, materializedTable);
        }
        return TABLE_RESULT_OK;
    }

    private ResolvedCatalogMaterializedTable suspendContinuousRefreshJob(
            ObjectIdentifier tableIdentifier, ResolvedCatalogMaterializedTable materializedTable) {
        final ContinuousRefreshHandler refreshHandler;
        final String savepointPath;
        try {
            refreshHandler =
                    deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());

            if (RefreshStatus.SUSPENDED == materializedTable.getRefreshStatus()) {
                throw new TableException(
                        String.format(
                                "Materialized table %s continuous refresh job has been suspended, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }

            final String savepointDirectory = requireSavepointDirectory();
            savepointPath =
                    refreshJobController.stopWithSavepoint(refreshHandler, savepointDirectory);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to suspend the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }

        // The job has stopped, so from here on a failure must name the savepoint.
        try {
            final ContinuousRefreshHandler updateRefreshHandler =
                    new ContinuousRefreshHandler(
                            refreshHandler.getExecutionTarget(),
                            refreshHandler.getClusterId(),
                            refreshHandler.getJobId(),
                            savepointPath);

            return updateRefreshHandler(
                    tableIdentifier,
                    materializedTable,
                    RefreshStatus.SUSPENDED,
                    updateRefreshHandler.asSummaryString(),
                    serializeContinuousHandler(updateRefreshHandler));
        } catch (Exception e) {
            final String jobId = refreshHandler.getJobId();
            LOG.error(
                    "Stopped the continuous refresh job {} of materialized table {} with savepoint {}, but failed to record it in the catalog.",
                    jobId,
                    tableIdentifier,
                    savepointPath,
                    e);
            throw new TableException(
                    String.format(
                            "Stopped the continuous refresh job %s of materialized table %s with savepoint %s, but failed to record the savepoint in the catalog, which still reports the table as ACTIVATED. To resume from the savepoint, set 'execution.state-recovery.path' to %s before RESUME, and unset it afterwards (RESET in SQL); otherwise RESUME starts the refresh job without restoring state.",
                            jobId, tableIdentifier, savepointPath, savepointPath),
                    e);
        }
    }

    private void suspendRefreshWorkflow(
            ObjectIdentifier tableIdentifier, ResolvedCatalogMaterializedTable materializedTable) {
        if (RefreshStatus.SUSPENDED == materializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s refresh workflow has been suspended.",
                            tableIdentifier));
        }

        WorkflowScheduler<? extends RefreshHandler> workflowScheduler =
                requireRefreshWorkflowContext(
                                "The workflow scheduler must be configured when suspending materialized table in full refresh mode.")
                        .scheduler();

        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            materializedTable.getSerializedRefreshHandler(),
                            operationCtx.getResourceManager().getUserClassLoader());
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new SuspendRefreshWorkflow(refreshHandler);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    tableIdentifier,
                    materializedTable,
                    RefreshStatus.SUSPENDED,
                    refreshHandler.asSummaryString(),
                    materializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to suspend the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private TableResultInternal callAlterMaterializedTableResume(
            AlterMaterializedTableResumeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                getCatalogMaterializedTable(tableIdentifier);

        // Initialization phase doesn't support resume operation.
        if (RefreshStatus.INITIALIZING == catalogMaterializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support resume operation.",
                            tableIdentifier));
        }

        if (RefreshMode.CONTINUOUS == catalogMaterializedTable.getRefreshMode()) {
            resumeContinuousRefreshJob(
                    tableIdentifier, catalogMaterializedTable, op.getDynamicOptions());
        } else {
            resumeRefreshWorkflow(
                    tableIdentifier, catalogMaterializedTable, op.getDynamicOptions());
        }

        return TABLE_RESULT_OK;
    }

    private void resumeContinuousRefreshJob(
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        final ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(
                        catalogMaterializedTable.getSerializedRefreshHandler());

        // Repeated resume continuous refresh job is not supported
        if (RefreshStatus.ACTIVATED == catalogMaterializedTable.getRefreshStatus()) {
            final JobStatus jobStatus = refreshJobController.getJobStatus(refreshHandler);
            if (!jobStatus.isGloballyTerminalState()) {
                throw new TableException(
                        String.format(
                                "Materialized table %s continuous refresh job has been resumed, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }
            // an ACTIVATED handler never carries a savepoint, so only a configured path keeps state
            if (operationCtx.getTableConfig().getOptional(SAVEPOINT_PATH).isEmpty()) {
                LOG.warn(
                        "Materialized table {} is ACTIVATED, but its continuous refresh job {} has already reached {}. Starting a new refresh job without restoring state; to restore a savepoint instead, set 'execution.state-recovery.path' to it before RESUME, and unset it afterwards (RESET in SQL).",
                        tableIdentifier,
                        refreshHandler.getJobId(),
                        jobStatus);
            }
        }

        checkRestartTargetBeforeChanges(
                tableIdentifier, catalogMaterializedTable.getSerializedRefreshHandler());
        final Optional<String> restorePath = refreshHandler.getRestorePath();
        try {
            executeContinuousRefreshJob(
                    catalogMaterializedTable, tableIdentifier, dynamicOptions, restorePath);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to resume the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void resumeRefreshWorkflow(
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        // Repeated resume refresh workflow is not supported
        if (RefreshStatus.ACTIVATED == catalogMaterializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s refresh workflow has been resumed.",
                            tableIdentifier));
        }

        WorkflowScheduler<? extends RefreshHandler> workflowScheduler =
                requireRefreshWorkflowContext(
                                "The workflow scheduler must be configured when resuming materialized table in full refresh mode.")
                        .scheduler();
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            operationCtx.getResourceManager().getUserClassLoader());
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new ResumeRefreshWorkflow(refreshHandler, dynamicOptions);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    tableIdentifier,
                    catalogMaterializedTable,
                    RefreshStatus.ACTIVATED,
                    refreshHandler.asSummaryString(),
                    catalogMaterializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to resume the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void executeContinuousRefreshJob(
            CatalogMaterializedTable catalogMaterializedTable,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> dynamicOptions,
            Optional<String> restorePath) {
        recordContinuousRefreshJob(
                materializedTableIdentifier,
                catalogMaterializedTable,
                submitContinuousRefreshJob(
                        catalogMaterializedTable,
                        materializedTableIdentifier,
                        dynamicOptions,
                        restorePath));
    }

    private void executeContinuousRefreshJobOrCancelIt(
            CatalogMaterializedTable catalogMaterializedTable,
            ObjectIdentifier materializedTableIdentifier) {
        final ContinuousRefreshHandler refreshHandler =
                submitContinuousRefreshJob(
                        catalogMaterializedTable,
                        materializedTableIdentifier,
                        Map.of(),
                        Optional.empty());
        try {
            recordContinuousRefreshJob(
                    materializedTableIdentifier, catalogMaterializedTable, refreshHandler);
        } catch (Exception e) {
            throw cancelUnrecordedRefreshJob(materializedTableIdentifier, refreshHandler, e);
        }
    }

    private ContinuousRefreshHandler submitContinuousRefreshJob(
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
        if (operationCtx
                .getTableConfig()
                .getOptional(CheckpointingOptions.CHECKPOINTING_INTERVAL)
                .isEmpty()) {

            final Duration freshness =
                    validateAndGetIntervalFreshness(catalogMaterializedTable).toDuration();
            customConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, freshness);
        }

        String insertStatement =
                getInsertStatement(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getExpandedQuery(),
                        dynamicOptions);

        final RefreshJobResult result =
                jobSubmitter.submitRefreshJob(customConfig, insertStatement);
        refreshJobController.register(result);
        return new ContinuousRefreshHandler(
                result.getTarget().getExecutionTarget(),
                result.getTarget().getClusterId(),
                result.getJobId());
    }

    private void recordContinuousRefreshJob(
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable,
            ContinuousRefreshHandler refreshHandler) {
        updateRefreshHandler(
                materializedTableIdentifier,
                resolveCatalogMaterializedTable(catalogMaterializedTable),
                RefreshStatus.ACTIVATED,
                refreshHandler.asSummaryString(),
                serializeContinuousHandler(refreshHandler));
    }

    private TableException cancelUnrecordedRefreshJob(
            ObjectIdentifier tableIdentifier,
            ContinuousRefreshHandler refreshHandler,
            Exception recordFailure) {
        final String jobId = refreshHandler.getJobId();
        LOG.error(
                "Failed to record the continuous refresh job {} of materialized table {} in the catalog; cancelling it.",
                jobId,
                tableIdentifier,
                recordFailure);
        try {
            refreshJobController.cancel(refreshHandler);
        } catch (Exception cancelFailure) {
            LOG.error(
                    "Failed to cancel the continuous refresh job {} of materialized table {} (execution.target={}, cluster id={}), which the catalog does not record. It may still be running and must be cancelled manually.",
                    jobId,
                    tableIdentifier,
                    refreshHandler.getExecutionTarget(),
                    refreshHandler.getClusterId(),
                    cancelFailure);
            return withSuppressed(
                    String.format(
                            "Failed to record the continuous refresh job %s of materialized table %s in the catalog, and cancelling it failed as well. The job may still be running (execution.target=%s, cluster id=%s) and must be cancelled manually.",
                            jobId,
                            tableIdentifier,
                            refreshHandler.getExecutionTarget(),
                            refreshHandler.getClusterId()),
                    cancelFailure,
                    recordFailure);
        }
        return new TableException(
                String.format(
                        "Failed to record the continuous refresh job %s of materialized table %s in the catalog. The job was cancelled.",
                        jobId, tableIdentifier),
                recordFailure);
    }

    private TableResultInternal callAlterMaterializedTableRefreshOperation(
            AlterMaterializedTableRefreshOperation op) {
        ObjectIdentifier materializedTableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(materializedTableIdentifier);

        boolean isPeriodic = op.isPeriodic();
        Map<String, String> refreshPartitions =
                isPeriodic
                        ? getPeriodRefreshPartition(
                                op.getScheduleTime(),
                                materializedTable.getDefinitionFreshness(),
                                materializedTableIdentifier,
                                materializedTable.getOptions(),
                                operationCtx.getTableConfig().getLocalTimeZone())
                        : op.getPartitionSpec();

        validatePartitionSpec(refreshPartitions, materializedTable);
        checkSubmissionTargetBeforeChanges();

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
                        materializedTable.getExpandedQuery(),
                        refreshPartitions,
                        op.getDynamicOptions());

        try {
            LOG.info(
                    "Starting refresh of the materialized table {}, statement: {}",
                    materializedTableIdentifier,
                    insertStatement);
            final RefreshJobResult result =
                    jobSubmitter.submitRefreshJob(customConfig, insertStatement);

            final Row row = Row.of(result.getJobId(), result.getTarget().toClusterInfo());
            ResolvedSchema schema =
                    ResolvedSchema.of(
                            Column.physical("job id", DataTypes.STRING()),
                            Column.physical(
                                    "cluster info",
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(schema)
                    .resultProvider(
                            new StaticResultProvider(
                                    List.of(row),
                                    DefaultMaterializedTableExecutor::refreshRowToInternalRow))
                    .build();
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to refresh the materialized table %s.",
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
                            -freshness.toDuration().toMillis());
            if (partFiledValue == null) {
                throw new TableException(
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
    static String getRefreshStatement(
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

    private TableResultInternal callAlterMaterializedTableChangeOperation(
            AlterMaterializedTableChangeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable oldMaterializedTable =
                getCatalogMaterializedTable(tableIdentifier);

        if (RefreshMode.FULL == oldMaterializedTable.getRefreshMode()) {
            // directly apply the alter operation
            return op.copyAsTableChangeOperation().execute(operationCtx);
        }

        if (RefreshStatus.ACTIVATED == oldMaterializedTable.getRefreshStatus()) {
            alterActivatedContinuousMaterializedTable(op, oldMaterializedTable);
        } else if (RefreshStatus.SUSPENDED == oldMaterializedTable.getRefreshStatus()) {
            alterSuspendedContinuousMaterializedTable(op, oldMaterializedTable);
        } else {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support alter operation.",
                            tableIdentifier));
        }

        return TABLE_RESULT_OK;
    }

    /** Stops the refresh job, applies the change and restarts it, rolling back on failure. */
    private void alterActivatedContinuousMaterializedTable(
            AlterMaterializedTableChangeOperation op,
            ResolvedCatalogMaterializedTable oldMaterializedTable) {
        final ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        // check the target before the stop, so a restart that could never run keeps the job
        // running and the table untouched
        checkRestartTargetBeforeChanges(
                tableIdentifier, oldMaterializedTable.getSerializedRefreshHandler());

        // 1. suspend the materialized table
        ResolvedCatalogMaterializedTable suspendMaterializedTable =
                suspendContinuousRefreshJob(tableIdentifier, oldMaterializedTable);

        // 2. alter materialized table schema & query definition
        final AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        op.getTableIdentifier(),
                        oldTable -> op.getTableChanges(),
                        suspendMaterializedTable,
                        op.getAsQueryOperation());
        alterMaterializedTableChangeOperation.execute(operationCtx);

        // 3. resume the materialized table
        try {
            executeContinuousRefreshJob(
                    alterMaterializedTableChangeOperation.getNewTable(),
                    tableIdentifier,
                    Map.of(),
                    Optional.empty());
        } catch (Exception e) {
            final ContinuousRefreshHandler suspendedRefreshHandler =
                    deserializeContinuousHandler(
                            suspendMaterializedTable.getSerializedRefreshHandler());
            LOG.warn(
                    "Failed to start the continuous refresh job for materialized table {} using new query {}, rollback to origin query {} with savepoint {}.",
                    tableIdentifier,
                    op.getNewTable().getExpandedQuery(),
                    suspendMaterializedTable.getExpandedQuery(),
                    suspendedRefreshHandler.getRestorePath().orElse(null),
                    e);

            // Roll back the changes to the materialized table and restore the continuous
            // refresh job
            generateRollbackAlterMaterializedTableOperation(
                            suspendMaterializedTable, alterMaterializedTableChangeOperation)
                    .execute(operationCtx);
            executeContinuousRefreshJob(
                    suspendMaterializedTable,
                    tableIdentifier,
                    Map.of(),
                    suspendedRefreshHandler.getRestorePath());

            throw new TableException(
                    String.format(
                            "Failed to start the continuous refresh job using new query %s when altering materialized table %s select query.",
                            op.getNewTable().getExpandedQuery(), tableIdentifier),
                    e);
        }
    }

    private void alterSuspendedContinuousMaterializedTable(
            AlterMaterializedTableChangeOperation op,
            ResolvedCatalogMaterializedTable oldMaterializedTable) {
        // alter schema & definition query & refresh handler (reset savepoint path of refresh
        // handler)
        List<TableChange> tableChanges = new ArrayList<>(op.getTableChanges());
        tableChanges.add(
                generateResetSavepointTableChange(
                        deserializeContinuousHandler(
                                oldMaterializedTable.getSerializedRefreshHandler())));

        AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        op.getTableIdentifier(),
                        oldTable -> tableChanges,
                        oldMaterializedTable,
                        op.getAsQueryOperation());

        alterMaterializedTableChangeOperation.execute(operationCtx);
    }

    private static TableException withSuppressed(
            String message, Exception cause, Exception suppressed) {
        final TableException failure = new TableException(message, cause);
        failure.addSuppressed(suppressed);
        return failure;
    }

    private static void cleanUpAfterFailure(
            Runnable cleanup, Exception failure, String cleanupFailureMessage) {
        try {
            cleanup.run();
        } catch (Exception cleanupFailure) {
            throw withSuppressed(cleanupFailureMessage, cleanupFailure, failure);
        }
    }

    private AlterMaterializedTableChangeOperation generateRollbackAlterMaterializedTableOperation(
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            AlterMaterializedTableChangeOperation op) {

        return new AlterMaterializedTableChangeOperation(
                op.getTableIdentifier(),
                oldTable -> List.of(),
                oldMaterializedTable,
                op.getAsQueryOperation());
    }

    private TableChange.ModifyRefreshHandler generateResetSavepointTableChange(
            ContinuousRefreshHandler continuousRefreshHandler) {
        ContinuousRefreshHandler resetContinuousRefreshHandler =
                new ContinuousRefreshHandler(
                        continuousRefreshHandler.getExecutionTarget(),
                        continuousRefreshHandler.getClusterId(),
                        continuousRefreshHandler.getJobId());

        return TableChange.modifyRefreshHandler(
                resetContinuousRefreshHandler.asSummaryString(),
                serializeContinuousHandler(resetContinuousRefreshHandler));
    }

    private TableResultInternal callDropMaterializedTableOperation(
            DropMaterializedTableOperation dropMaterializedTableOperation) {
        ObjectIdentifier tableIdentifier = dropMaterializedTableOperation.getTableIdentifier();
        boolean tableExists =
                operationCtx
                        .getCatalogManager()
                        .getCatalog(tableIdentifier.getCatalogName())
                        .map(catalog -> catalog.tableExists(tableIdentifier.toObjectPath()))
                        .orElse(false);
        if (!tableExists) {
            if (dropMaterializedTableOperation.isIfExists()) {
                LOG.info(
                        "Materialized table {} does not exists, skip the drop operation.",
                        tableIdentifier);
                return TABLE_RESULT_OK;
            } else {
                throw new ValidationException(
                        String.format(
                                "Materialized table with identifier %s does not exist.",
                                tableIdentifier));
            }
        }

        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(tableIdentifier);
        RefreshStatus refreshStatus = materializedTable.getRefreshStatus();
        if (RefreshStatus.ACTIVATED == refreshStatus || RefreshStatus.SUSPENDED == refreshStatus) {
            RefreshMode refreshMode = materializedTable.getRefreshMode();
            if (RefreshMode.FULL == refreshMode) {
                deleteRefreshWorkflow(tableIdentifier, materializedTable);
            } else if (RefreshMode.CONTINUOUS == refreshMode
                    && RefreshStatus.ACTIVATED == refreshStatus) {
                cancelContinuousRefreshJob(tableIdentifier, materializedTable);
            }
        } else if (RefreshStatus.INITIALIZING == refreshStatus) {
            throw new ValidationException(
                    String.format(
                            "Current refresh status of materialized table %s is initializing, skip the drop operation.",
                            tableIdentifier.asSerializableString()));
        }

        dropMaterializedTableOperation.execute(operationCtx);

        return TABLE_RESULT_OK;
    }

    private void cancelContinuousRefreshJob(
            ObjectIdentifier tableIdentifier, CatalogMaterializedTable materializedTable) {
        final ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());
        if (refreshJobController.getJobStatus(refreshHandler).isTerminalState()) {
            LOG.info(
                    "No need to cancel the continuous refresh job {} for materialized table {} as it is not currently running.",
                    refreshHandler.getJobId(),
                    tableIdentifier);
            return;
        }

        final Exception cancelFailure;
        try {
            refreshJobController.cancel(refreshHandler);
            return;
        } catch (Exception e) {
            cancelFailure = e;
        }

        final JobStatus jobStatus = refreshJobController.getJobStatus(refreshHandler);
        if (!jobStatus.isTerminalState()) {
            throw new TableException(
                    String.format(
                            "Failed to drop the materialized table %s because the continuous refresh job %s could not be canceled."
                                    + " The current status of the continuous refresh job is %s.",
                            tableIdentifier, refreshHandler.getJobId(), jobStatus),
                    cancelFailure);
        }
        LOG.warn(
                "An exception occurred while canceling the continuous refresh job {} for materialized table {},"
                        + " but since the job is in a terminal state, skip the cancel operation.",
                refreshHandler.getJobId(),
                tableIdentifier,
                cancelFailure);
    }

    private void deleteRefreshWorkflow(
            ObjectIdentifier tableIdentifier, CatalogMaterializedTable catalogMaterializedTable) {
        WorkflowScheduler<? extends RefreshHandler> workflowScheduler =
                requireRefreshWorkflowContext(
                                "The workflow scheduler must be configured when dropping materialized table in full refresh mode.")
                        .scheduler();
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            operationCtx.getResourceManager().getUserClassLoader());
            DeleteRefreshWorkflow deleteRefreshWorkflow = new DeleteRefreshWorkflow(refreshHandler);
            workflowScheduler.deleteRefreshWorkflow(deleteRefreshWorkflow);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to delete the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private ContinuousRefreshHandler deserializeContinuousHandler(byte[] serializedRefreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                    serializedRefreshHandler,
                    operationCtx.getResourceManager().getUserClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new TableException("Failed to deserialize ContinuousRefreshHandler.", e);
        }
    }

    private byte[] serializeContinuousHandler(ContinuousRefreshHandler refreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.serialize(refreshHandler);
        } catch (IOException e) {
            throw new TableException("Failed to serialize ContinuousRefreshHandler.", e);
        }
    }

    private ResolvedCatalogMaterializedTable getCatalogMaterializedTable(
            ObjectIdentifier tableIdentifier) {
        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable =
                operationCtx
                        .getCatalogManager()
                        .getTableOrError(tableIdentifier)
                        .getResolvedTable();
        if (MATERIALIZED_TABLE != resolvedCatalogBaseTable.getTableKind()) {
            throw new ValidationException(
                    String.format(
                            "Table %s is not a materialized table, does not support materialized table related operation.",
                            tableIdentifier));
        }

        return (ResolvedCatalogMaterializedTable) resolvedCatalogBaseTable;
    }

    private ResolvedCatalogMaterializedTable resolveCatalogMaterializedTable(
            CatalogMaterializedTable materializedTable) {
        return operationCtx.getCatalogManager().resolveCatalogMaterializedTable(materializedTable);
    }

    private ResolvedCatalogMaterializedTable updateRefreshHandler(
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            RefreshStatus refreshStatus,
            String refreshHandlerSummary,
            byte[] serializedRefreshHandler) {
        List<TableChange> tableChanges = new ArrayList<>();
        tableChanges.add(TableChange.modifyRefreshStatus(refreshStatus));
        tableChanges.add(
                TableChange.modifyRefreshHandler(refreshHandlerSummary, serializedRefreshHandler));
        AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        materializedTableIdentifier,
                        oldTable -> tableChanges,
                        catalogMaterializedTable);
        // update RefreshHandler to Catalog
        alterMaterializedTableChangeOperation.execute(operationCtx);

        return resolveCatalogMaterializedTable(alterMaterializedTableChangeOperation.getNewTable());
    }

    /** Generate insert statement for materialized table. */
    @VisibleForTesting
    static String getInsertStatement(
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

    private void checkSubmissionTargetBeforeChanges() {
        if (!jobSubmitter.supportsApplicationTargets()) {
            RefreshJobTarget.forNewJob(operationCtx.getTableConfig());
        }
    }

    private void checkRestartTargetBeforeChanges(
            ObjectIdentifier tableIdentifier, byte[] serializedRefreshHandler) {
        if (!jobSubmitter.supportsApplicationTargets()) {
            RefreshJobTarget.forRestart(
                    operationCtx.getTableConfig(),
                    tableIdentifier,
                    deserializeContinuousHandler(serializedRefreshHandler));
        }
    }

    private String requireSavepointDirectory() {
        return operationCtx
                .getTableConfig()
                .getOptional(CheckpointingOptions.SAVEPOINT_DIRECTORY)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Savepoint directory is not configured, can't stop job with savepoint."));
    }

    private static IntervalFreshness validateAndGetIntervalFreshness(
            final CatalogMaterializedTable catalogMaterializedTable) {
        return Optional.ofNullable(catalogMaterializedTable.getDefinitionFreshness())
                .orElseThrow(() -> new TableException("Freshness cannot be null"));
    }

    private RefreshWorkflowContext requireRefreshWorkflowContext(String missingSchedulerMessage) {
        return jobSubmitter
                .getRefreshWorkflowContext()
                .orElseThrow(() -> new TableException(missingSchedulerMessage));
    }

    private static RowData refreshRowToInternalRow(Row row) {
        final Map<StringData, StringData> internalClusterInfo = new HashMap<>();
        row.<Map<String, String>>getFieldAs(1)
                .forEach(
                        (key, value) ->
                                internalClusterInfo.put(
                                        StringData.fromString(key), StringData.fromString(value)));
        return GenericRowData.of(
                StringData.fromString(row.getFieldAs(0)), new GenericMapData(internalClusterInfo));
    }
}
