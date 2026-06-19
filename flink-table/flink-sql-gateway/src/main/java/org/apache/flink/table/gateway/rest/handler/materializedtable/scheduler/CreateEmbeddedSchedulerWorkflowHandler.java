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

package org.apache.flink.table.gateway.rest.handler.materializedtable.scheduler;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.gateway.workflow.WorkflowInfo;
import org.apache.flink.table.gateway.workflow.scheduler.EmbeddedQuartzScheduler;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.quartz.JobDetail;
import org.quartz.JobKey;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to create workflow in embedded scheduler. */
public class CreateEmbeddedSchedulerWorkflowHandler
        extends AbstractSqlGatewayRestHandler<
                CreateEmbeddedSchedulerWorkflowRequestBody,
                CreateEmbeddedSchedulerWorkflowResponseBody,
                EmptyMessageParameters> {

    private final EmbeddedQuartzScheduler quartzScheduler;

    public CreateEmbeddedSchedulerWorkflowHandler(
            SqlGatewayService service,
            EmbeddedQuartzScheduler quartzScheduler,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            CreateEmbeddedSchedulerWorkflowRequestBody,
                            CreateEmbeddedSchedulerWorkflowResponseBody,
                            EmptyMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
        this.quartzScheduler = quartzScheduler;
    }

    @Override
    protected CompletableFuture<CreateEmbeddedSchedulerWorkflowResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<CreateEmbeddedSchedulerWorkflowRequestBody> request)
            throws RestHandlerException {
        String materializedTableIdentifier =
                request.getRequestBody().getMaterializedTableIdentifier();
        String cronExpression = request.getRequestBody().getCronExpression();
        Map<String, String> initConfig = request.getRequestBody().getInitConfig();
        Map<String, String> executionConfig = request.getRequestBody().getExecutionConfig();
        String restEndpointURL = request.getRequestBody().getRestEndpointUrl();
        WorkflowInfo workflowInfo =
                new WorkflowInfo(
                        materializedTableIdentifier,
                        Collections.emptyMap(),
                        initConfig == null ? Collections.emptyMap() : initConfig,
                        executionConfig == null ? Collections.emptyMap() : executionConfig,
                        restEndpointURL);
        try {
            JobDetail jobDetail =
                    quartzScheduler.createScheduleWorkflow(workflowInfo, cronExpression);
            JobKey jobKey = jobDetail.getKey();
            return CompletableFuture.completedFuture(
                    new CreateEmbeddedSchedulerWorkflowResponseBody(
                            jobKey.getName(), jobKey.getGroup()));
        } catch (Exception e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
