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

package org.apache.flink.table.gateway.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.DeleteEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.ResumeEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.header.materializedtable.scheduler.SuspendEmbeddedSchedulerWorkflowHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowResponseBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.EmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.ResumeEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.workflow.scheduler.EmbeddedQuartzScheduler;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;
import org.apache.flink.table.workflow.WorkflowScheduler;
import org.apache.flink.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A workflow scheduler plugin implementation for {@link EmbeddedQuartzScheduler}. It is used to
 * create, modify refresh workflow for materialized table.
 */
@PublicEvolving
public class EmbeddedWorkflowScheduler implements WorkflowScheduler<EmbeddedRefreshHandler> {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedWorkflowScheduler.class);

    private final Configuration configuration;
    private final String restAddress;
    private final int port;

    private RestClient restClient;

    public EmbeddedWorkflowScheduler(Configuration configuration) {
        this.configuration = configuration;
        this.restAddress = configuration.get(RestOptions.ADDRESS);
        this.port = configuration.get(RestOptions.PORT);
    }

    @Override
    public void open() throws WorkflowException {
        try {
            restClient = new RestClient(configuration, Executors.directExecutor());
        } catch (Exception e) {
            throw new WorkflowException(
                    "Could not create RestClient to connect to embedded scheduler.", e);
        }
    }

    @Override
    public void close() throws WorkflowException {
        restClient.closeAsync();
    }

    @Override
    public EmbeddedRefreshHandlerSerializer getRefreshHandlerSerializer() {
        return EmbeddedRefreshHandlerSerializer.INSTANCE;
    }

    @Override
    public EmbeddedRefreshHandler createRefreshWorkflow(CreateRefreshWorkflow createRefreshWorkflow)
            throws WorkflowException {
        if (createRefreshWorkflow instanceof CreatePeriodicRefreshWorkflow) {
            CreatePeriodicRefreshWorkflow periodicRefreshWorkflow =
                    (CreatePeriodicRefreshWorkflow) createRefreshWorkflow;
            ObjectIdentifier materializedTableIdentifier =
                    periodicRefreshWorkflow.getMaterializedTableIdentifier();
            CreateEmbeddedSchedulerWorkflowRequestBody requestBody =
                    new CreateEmbeddedSchedulerWorkflowRequestBody(
                            materializedTableIdentifier.asSerializableString(),
                            periodicRefreshWorkflow.getCronExpression(),
                            periodicRefreshWorkflow.getInitConfig(),
                            periodicRefreshWorkflow.getExecutionConfig(),
                            periodicRefreshWorkflow.getRestEndpointUrl());
            CreateEmbeddedSchedulerWorkflowResponseBody responseBody;
            try {
                responseBody =
                        restClient
                                .sendRequest(
                                        restAddress,
                                        port,
                                        CreateEmbeddedSchedulerWorkflowHeaders.getInstance(),
                                        EmptyMessageParameters.getInstance(),
                                        requestBody)
                                .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error(
                        "Failed to create periodic refresh workflow for materialized table {}.",
                        materializedTableIdentifier,
                        e);
                throw new WorkflowException(
                        String.format(
                                "Failed to create periodic refresh workflow for materialized table %s.",
                                materializedTableIdentifier),
                        e);
            }

            return new EmbeddedRefreshHandler(
                    responseBody.getWorkflowName(), responseBody.getWorkflowGroup());
        } else {
            LOG.error(
                    "Unsupported create refresh workflow type {}.",
                    createRefreshWorkflow.getClass().getSimpleName());
            throw new WorkflowException(
                    String.format(
                            "Unsupported create refresh workflow type %s.",
                            createRefreshWorkflow.getClass().getSimpleName()));
        }
    }

    @Override
    public void modifyRefreshWorkflow(
            ModifyRefreshWorkflow<EmbeddedRefreshHandler> modifyRefreshWorkflow)
            throws WorkflowException {
        EmbeddedRefreshHandler embeddedRefreshHandler = modifyRefreshWorkflow.getRefreshHandler();

        if (modifyRefreshWorkflow instanceof SuspendRefreshWorkflow) {
            EmbeddedSchedulerWorkflowRequestBody suspendRequestBody =
                    new EmbeddedSchedulerWorkflowRequestBody(
                            embeddedRefreshHandler.getWorkflowName(),
                            embeddedRefreshHandler.getWorkflowGroup());
            try {
                restClient
                        .sendRequest(
                                restAddress,
                                port,
                                SuspendEmbeddedSchedulerWorkflowHeaders.getInstance(),
                                EmptyMessageParameters.getInstance(),
                                suspendRequestBody)
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error(
                        "Failed to suspend refresh workflow {}.",
                        embeddedRefreshHandler.asSummaryString(),
                        e);
                throw new WorkflowException(
                        String.format(
                                "Failed to suspend refresh workflow %s.",
                                embeddedRefreshHandler.asSummaryString()),
                        e);
            }
        } else if (modifyRefreshWorkflow instanceof ResumeRefreshWorkflow) {
            ResumeEmbeddedSchedulerWorkflowRequestBody requestBody =
                    new ResumeEmbeddedSchedulerWorkflowRequestBody(
                            embeddedRefreshHandler.getWorkflowName(),
                            embeddedRefreshHandler.getWorkflowGroup(),
                            ((ResumeRefreshWorkflow<?>) modifyRefreshWorkflow).getDynamicOptions());
            try {
                restClient
                        .sendRequest(
                                restAddress,
                                port,
                                ResumeEmbeddedSchedulerWorkflowHeaders.getInstance(),
                                EmptyMessageParameters.getInstance(),
                                requestBody)
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error(
                        "Failed to resume refresh workflow {}.",
                        embeddedRefreshHandler.asSummaryString(),
                        e);
                throw new WorkflowException(
                        String.format(
                                "Failed to resume refresh workflow %s.",
                                embeddedRefreshHandler.asSummaryString()),
                        e);
            }
        } else {
            LOG.error(
                    "Unsupported modify refresh workflow type {}.",
                    modifyRefreshWorkflow.getClass().getSimpleName());
            throw new WorkflowException(
                    String.format(
                            "Unsupported modify refresh workflow type %s.",
                            modifyRefreshWorkflow.getClass().getSimpleName()));
        }
    }

    @Override
    public void deleteRefreshWorkflow(
            DeleteRefreshWorkflow<EmbeddedRefreshHandler> deleteRefreshWorkflow)
            throws WorkflowException {
        EmbeddedRefreshHandler embeddedRefreshHandler = deleteRefreshWorkflow.getRefreshHandler();
        EmbeddedSchedulerWorkflowRequestBody requestBody =
                new EmbeddedSchedulerWorkflowRequestBody(
                        embeddedRefreshHandler.getWorkflowName(),
                        embeddedRefreshHandler.getWorkflowGroup());
        try {
            restClient
                    .sendRequest(
                            restAddress,
                            port,
                            DeleteEmbeddedSchedulerWorkflowHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            requestBody)
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error(
                    "Failed to delete refresh workflow {}.",
                    embeddedRefreshHandler.asSummaryString(),
                    e);
            throw new WorkflowException(
                    String.format(
                            "Failed to delete refresh workflow %s.",
                            embeddedRefreshHandler.asSummaryString()),
                    e);
        }
    }
}
