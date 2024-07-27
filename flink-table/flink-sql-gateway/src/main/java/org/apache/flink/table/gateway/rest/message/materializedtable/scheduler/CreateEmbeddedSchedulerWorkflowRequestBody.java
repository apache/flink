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

package org.apache.flink.table.gateway.rest.message.materializedtable.scheduler;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/** {@link RequestBody} for create workflow in embedded scheduler. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateEmbeddedSchedulerWorkflowRequestBody implements RequestBody {

    private static final String FIELD_NAME_MATERIALIZED_TABLE = "materializedTableIdentifier";
    private static final String FIELD_NAME_CRON_EXPRESSION = "cronExpression";
    private static final String FIELD_NAME_INIT_CONFIG = "initConfig";
    private static final String FIELD_NAME_EXECUTION_CONFIG = "executionConfig";
    private static final String FIELD_NAME_REST_ENDPOINT_URL = "restEndpointUrl";

    @JsonProperty(FIELD_NAME_MATERIALIZED_TABLE)
    private final String materializedTableIdentifier;

    @JsonProperty(FIELD_NAME_CRON_EXPRESSION)
    private final String cronExpression;

    @JsonProperty(FIELD_NAME_INIT_CONFIG)
    private final Map<String, String> initConfig;

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    @Nullable
    private final Map<String, String> executionConfig;

    @JsonProperty(FIELD_NAME_REST_ENDPOINT_URL)
    private final String restEndpointUrl;

    @JsonCreator
    public CreateEmbeddedSchedulerWorkflowRequestBody(
            @JsonProperty(FIELD_NAME_MATERIALIZED_TABLE) String materializedTableIdentifier,
            @JsonProperty(FIELD_NAME_CRON_EXPRESSION) String cronExpression,
            @Nullable @JsonProperty(FIELD_NAME_INIT_CONFIG) Map<String, String> initConfig,
            @Nullable @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
                    Map<String, String> executionConfig,
            @JsonProperty(FIELD_NAME_REST_ENDPOINT_URL) String restEndpointUrl) {
        this.materializedTableIdentifier = materializedTableIdentifier;
        this.cronExpression = cronExpression;
        this.initConfig = initConfig;
        this.executionConfig = executionConfig;
        this.restEndpointUrl = restEndpointUrl;
    }

    public String getMaterializedTableIdentifier() {
        return materializedTableIdentifier;
    }

    @Nullable
    public Map<String, String> getInitConfig() {
        return initConfig;
    }

    @Nullable
    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public String getRestEndpointUrl() {
        return restEndpointUrl;
    }
}
