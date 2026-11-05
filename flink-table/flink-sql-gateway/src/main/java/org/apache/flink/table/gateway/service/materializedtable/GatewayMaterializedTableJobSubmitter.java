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
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.configuration.PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID;
import static org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil.WORKFLOW_SCHEDULER_PREFIX;

/** SQL Gateway implementation of {@link MaterializedTableJobSubmitter}. */
@Internal
public class GatewayMaterializedTableJobSubmitter implements MaterializedTableJobSubmitter {

    private static final Logger LOG =
            LoggerFactory.getLogger(GatewayMaterializedTableJobSubmitter.class);

    private final OperationExecutor operationExecutor;
    private final OperationHandle handle;
    private final MaterializedTableContext context;

    public GatewayMaterializedTableJobSubmitter(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            MaterializedTableContext context) {
        this.operationExecutor = operationExecutor;
        this.handle = handle;
        this.context = context;
    }

    @Override
    public RefreshJobResult submitRefreshJob(
            Configuration executionConfig, String insertStatement) {
        final String executionTarget =
                operationExecutor.getSessionContext().getSessionConf().get(TARGET);
        if (executionTarget == null
                || executionTarget.isEmpty()
                || "local".equals(executionTarget)) {
            final String errorMessage =
                    String.format(
                            "Unsupported execution target detected: %s."
                                    + "Currently, only the following execution targets are supported: "
                                    + "'remote', 'yarn-session', 'yarn-application', 'kubernetes-session', 'kubernetes-application'. ",
                            executionTarget);
            LOG.error(errorMessage);
            throw new ValidationException(errorMessage);
        }

        if (executionTarget.endsWith("application")) {
            return deployApplication(executionConfig, insertStatement);
        }

        final String clusterId =
                operationExecutor
                        .getSessionClusterId()
                        .orElseThrow(
                                () -> {
                                    final String errorMessage =
                                            String.format(
                                                    "No cluster ID found when executing materialized table refresh job. Execution target is : %s",
                                                    executionTarget);
                                    LOG.error(errorMessage);
                                    return new ValidationException(errorMessage);
                                });
        final ResultFetcher resultFetcher =
                operationExecutor.executeStatement(handle, executionConfig, insertStatement);
        final List<RowData> results = fetchAllResults(resultFetcher);
        final String jobId = results.get(0).getString(0).toString();

        return new RefreshJobResult(new RefreshJobTarget(executionTarget, clusterId), jobId, null);
    }

    @Override
    public boolean supportsApplicationTargets() {
        return true;
    }

    @Override
    public Optional<RefreshWorkflowContext> getRefreshWorkflowContext() {
        return Optional.ofNullable(context.workflowScheduler())
                .map(GatewayRefreshWorkflowContext::new);
    }

    private RefreshJobResult deployApplication(
            Configuration executionConfig, String insertStatement) {
        final Configuration mergedConfig =
                new Configuration(operationExecutor.getSessionContext().getSessionConf());
        mergedConfig.addAll(executionConfig);
        final JobID jobId = new JobID();
        mergedConfig.set(PIPELINE_FIXED_JOB_ID, jobId.toString());

        try {
            final String clusterId =
                    SqlGatewayServiceImpl.deployApplicationCluster(
                                    mergedConfig, null, insertStatement)
                            .toString();

            return new RefreshJobResult(
                    new RefreshJobTarget(mergedConfig.get(TARGET), clusterId),
                    jobId.toString(),
                    null);
        } catch (Throwable t) {
            LOG.error("Failed to deploy script {} to application cluster.", insertStatement, t);
            throw new SqlGatewayException("Failed to deploy script to cluster.", t);
        }
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

    /** Gateway {@link RefreshWorkflowContext}. */
    private class GatewayRefreshWorkflowContext implements RefreshWorkflowContext {

        private final WorkflowScheduler<? extends RefreshHandler> scheduler;

        private GatewayRefreshWorkflowContext(
                WorkflowScheduler<? extends RefreshHandler> scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public WorkflowScheduler<? extends RefreshHandler> scheduler() {
            return scheduler;
        }

        @Override
        public String restEndpointUrl() {
            return context.restEndpointUrl();
        }

        @Override
        public Map<String, String> sessionInitializationConf() {
            Map<String, String> sessionConf =
                    operationExecutor.getSessionContext().getSessionConf().toMap();

            // we only keep the session conf that is not in the default context or the conf value is
            // different from the default context.
            Map<String, String> defaultContextConf =
                    operationExecutor
                            .getSessionContext()
                            .getDefaultContext()
                            .getFlinkConfig()
                            .toMap();
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
    }
}
