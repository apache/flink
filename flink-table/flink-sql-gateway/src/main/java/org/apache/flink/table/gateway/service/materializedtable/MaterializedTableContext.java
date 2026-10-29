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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.workflow.WorkflowScheduler;

import javax.annotation.Nullable;

import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getEndpointConfig;

/**
 * Session-scoped holder for the SQL Gateway capabilities a {@link
 * GatewayMaterializedTableJobSubmitter} needs to run materialized table jobs. It owns the lifecycle
 * of the workflow scheduler and is created once per session in {@code SessionContext}.
 */
@Internal
public class MaterializedTableContext {

    private final @Nullable WorkflowScheduler<? extends RefreshHandler> workflowScheduler;

    private final String restEndpointUrl;

    public MaterializedTableContext(Configuration configuration, ClassLoader userCodeClassLoader) {
        this.restEndpointUrl = buildRestEndpointUrl(configuration);
        this.workflowScheduler = buildWorkflowScheduler(configuration, userCodeClassLoader);
    }

    public @Nullable WorkflowScheduler<? extends RefreshHandler> workflowScheduler() {
        return workflowScheduler;
    }

    public String restEndpointUrl() {
        return restEndpointUrl;
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

    private static String buildRestEndpointUrl(Configuration configuration) {
        Configuration restEndpointConfig =
                Configuration.fromMap(
                        getEndpointConfig(configuration, SqlGatewayRestEndpointFactory.IDENTIFIER));
        String address = restEndpointConfig.get(SqlGatewayRestOptions.ADDRESS);
        int port = restEndpointConfig.get(SqlGatewayRestOptions.PORT);

        return String.format("http://%s:%s", address, port);
    }

    private static WorkflowScheduler<? extends RefreshHandler> buildWorkflowScheduler(
            Configuration configuration, ClassLoader userCodeClassLoader) {
        return WorkflowSchedulerFactoryUtil.createWorkflowScheduler(
                configuration, userCodeClassLoader);
    }
}
