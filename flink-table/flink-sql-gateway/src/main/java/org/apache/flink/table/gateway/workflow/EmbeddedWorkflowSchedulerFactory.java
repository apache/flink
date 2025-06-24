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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.WorkflowSchedulerFactory;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getEndpointConfig;
import static org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointFactory.rebuildRestEndpointOptions;

/** The {@link WorkflowSchedulerFactory} to create the {@link EmbeddedWorkflowScheduler}. */
@PublicEvolving
public class EmbeddedWorkflowSchedulerFactory implements WorkflowSchedulerFactory {

    public static final String IDENTIFIER = "embedded";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public WorkflowScheduler<?> createWorkflowScheduler(Context context) {
        Map<String, String> flinkConfigMap = context.getConfiguration().toMap();
        Configuration configuration = Configuration.fromMap(flinkConfigMap);
        Map<String, String> restEndpointConfigMap =
                getEndpointConfig(configuration, SqlGatewayRestEndpointFactory.IDENTIFIER);
        // Use the SqlGateway rest endpoint config
        Configuration restConfig =
                rebuildRestEndpointOptions(restEndpointConfigMap, flinkConfigMap);
        return new EmbeddedWorkflowScheduler(restConfig);
    }
}
