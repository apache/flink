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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * The {@link WorkflowInfo} represents the required information of one materialized table background
 * workflow. It is used by {@code EmbeddedSchedulerJob} when trigger the execution of workflow.
 *
 * <p>Note: All member variables need be able to be serialized as json string.
 */
public class WorkflowInfo {

    private final String materializedTableIdentifier;

    private final Map<String, String> dynamicOptions;
    private final Map<String, String> initConfig;
    private final Map<String, String> executionConfig;

    private final String restEndpointUrl;

    @JsonCreator
    public WorkflowInfo(
            @JsonProperty("materializedTableIdentifier") String materializedTableIdentifier,
            @JsonProperty("dynamicOptions") Map<String, String> dynamicOptions,
            @JsonProperty("initConfig") Map<String, String> initConfig,
            @JsonProperty("executionConfig") Map<String, String> executionConfig,
            @JsonProperty("restEndpointUrl") String restEndpointUrl) {
        this.materializedTableIdentifier = materializedTableIdentifier;
        this.dynamicOptions = dynamicOptions;
        this.initConfig = initConfig;
        this.executionConfig = executionConfig;
        this.restEndpointUrl = restEndpointUrl;
    }

    public String getMaterializedTableIdentifier() {
        return materializedTableIdentifier;
    }

    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    public Map<String, String> getInitConfig() {
        return initConfig;
    }

    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }

    public String getRestEndpointUrl() {
        return restEndpointUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkflowInfo that = (WorkflowInfo) o;
        return Objects.equals(materializedTableIdentifier, that.materializedTableIdentifier)
                && Objects.equals(initConfig, that.initConfig)
                && Objects.equals(executionConfig, that.executionConfig)
                && Objects.equals(restEndpointUrl, that.restEndpointUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                materializedTableIdentifier,
                dynamicOptions,
                initConfig,
                executionConfig,
                restEndpointUrl);
    }

    @Override
    public String toString() {
        return "WorkflowInfo{"
                + "materializedTableIdentifier='"
                + materializedTableIdentifier
                + '\''
                + ", dynamicOptions="
                + dynamicOptions
                + ", initConfig="
                + initConfig
                + ", executionConfig="
                + executionConfig
                + ", restEndpointUrl='"
                + restEndpointUrl
                + '\''
                + '}';
    }
}
