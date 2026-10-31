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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/**
 * The default implementation of a {@link MaterializedTableJobSubmitter}.
 *
 * <p>It does not support application targets (see {@link #supportsApplicationTargets()}) and does
 * not expose a {@link WorkflowScheduler}, so full-mode materialized tables cannot be created,
 * suspended, resumed or dropped here, while a one-time REFRESH still works.
 */
@Internal
public class DefaultMaterializedTableJobSubmitter implements MaterializedTableJobSubmitter {

    private static final String DEFAULT_REFRESH_JOB_NAME = "materialized-table-refresh";

    private final ExecutableOperation.Context operationCtx;
    private final Planner planner;
    private final Executor sessionExecutor;

    public DefaultMaterializedTableJobSubmitter(
            ExecutableOperation.Context operationCtx, Planner planner, Executor sessionExecutor) {
        this.operationCtx = operationCtx;
        this.planner = planner;
        this.sessionExecutor = sessionExecutor;
    }

    @Override
    public RefreshJobResult submitRefreshJob(
            Configuration executionConfig, String insertStatement) {
        final RefreshJobTarget target = RefreshJobTarget.forNewJob(operationCtx.getTableConfig());
        final boolean isStreamingRefresh = STREAMING == executionConfig.get(RUNTIME_MODE);
        final JobClient jobClient =
                isStreamingRefresh == operationCtx.isStreamingMode()
                        ? submitWithSessionPlanner(target, executionConfig, insertStatement)
                        : submitWithThrowawayPlanner(target, executionConfig, insertStatement);

        return new RefreshJobResult(target, jobClient.getJobID().toString(), jobClient);
    }

    @Override
    public boolean supportsApplicationTargets() {
        return false;
    }

    @Override
    public Optional<RefreshWorkflowContext> getRefreshWorkflowContext() {
        return Optional.empty();
    }

    private static Configuration refreshConfiguration(
            Configuration tableConfiguration,
            Configuration executionConfig,
            RefreshJobTarget target) {
        final Configuration refreshConfig = new Configuration(tableConfiguration);
        refreshConfig.addAll(executionConfig);
        target.applyTo(refreshConfig);
        return refreshConfig;
    }

    private JobClient submitWithSessionPlanner(
            RefreshJobTarget target, Configuration executionConfig, String insertStatement) {
        final ModifyOperation modifyOperation =
                parseSingleInsertOperation(planner, insertStatement);
        operationCtx.getResourceManager().addJarConfiguration(operationCtx.getTableConfig());

        final Configuration pipelineConfig =
                refreshConfiguration(
                        operationCtx.getTableConfig().getConfiguration(), executionConfig, target);
        return submitPipeline(planner, modifyOperation, createRefreshExecutor(), pipelineConfig);
    }

    private JobClient submitWithThrowawayPlanner(
            RefreshJobTarget target, Configuration executionConfig, String insertStatement) {
        final Configuration refreshConfig =
                refreshConfiguration(
                        operationCtx.getTableConfig().getConfiguration(), executionConfig, target);
        final TableConfig refreshTableConfig = TableConfig.getDefault();
        refreshTableConfig.setRootConfiguration(
                operationCtx.getTableConfig().getRootConfiguration());
        refreshTableConfig.addConfiguration(refreshConfig);

        final Executor refreshExecutor = createRefreshExecutor();
        final Planner refreshPlanner =
                PlannerFactoryUtil.createPlanner(
                        refreshExecutor,
                        refreshTableConfig,
                        operationCtx.getResourceManager().getUserClassLoader(),
                        operationCtx.getModuleManager(),
                        operationCtx.getCatalogManager(),
                        operationCtx.getFunctionCatalog());

        final ModifyOperation modifyOperation =
                parseSingleInsertOperation(refreshPlanner, insertStatement);
        operationCtx.getResourceManager().addJarConfiguration(refreshTableConfig);

        return submitPipeline(
                refreshPlanner,
                modifyOperation,
                refreshExecutor,
                refreshTableConfig.getConfiguration());
    }

    private Executor createRefreshExecutor() {
        final ExecutorFactory executorFactory =
                FactoryUtil.discoverFactory(
                        operationCtx.getResourceManager().getUserClassLoader(),
                        ExecutorFactory.class,
                        ExecutorFactory.DEFAULT_IDENTIFIER);
        return executorFactory.create(
                Configuration.fromMap(sessionExecutor.getConfiguration().toMap()));
    }

    private static JobClient submitPipeline(
            Planner planner,
            ModifyOperation modifyOperation,
            Executor refreshExecutor,
            Configuration pipelineConfig) {
        final List<Transformation<?>> transformations = planner.translate(List.of(modifyOperation));
        final Pipeline pipeline =
                refreshExecutor.createPipeline(
                        transformations, pipelineConfig, DEFAULT_REFRESH_JOB_NAME);
        try {
            return refreshExecutor.executeAsync(pipeline);
        } catch (Exception e) {
            throw new TableException("Failed to submit the materialized table refresh job.", e);
        }
    }

    private static ModifyOperation parseSingleInsertOperation(
            Planner planner, String insertStatement) {
        final List<Operation> operations = planner.getParser().parse(insertStatement);
        if (operations.size() != 1 || !(operations.get(0) instanceof ModifyOperation)) {
            throw new TableException(
                    String.format(
                            "The materialized table refresh statement must translate to a single"
                                    + " INSERT operation, but was: %s",
                            insertStatement));
        }
        return (ModifyOperation) operations.get(0);
    }
}
