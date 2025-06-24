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

/** {@link RequestBody} for resume workflow in embedded scheduler. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResumeEmbeddedSchedulerWorkflowRequestBody implements RequestBody {

    private static final String FIELD_NAME_WORKFLOW_NAME = "workflowName";
    private static final String FIELD_NAME_WORKFLOW_GROUP = "workflowGroup";
    private static final String FIELD_NAME_DYNAMIC_OPTIONS = "dynamicOptions";

    @JsonProperty(FIELD_NAME_WORKFLOW_NAME)
    private final String workflowName;

    @JsonProperty(FIELD_NAME_WORKFLOW_GROUP)
    private final String workflowGroup;

    @JsonProperty(FIELD_NAME_DYNAMIC_OPTIONS)
    @Nullable
    private final Map<String, String> dynamicOptions;

    @JsonCreator
    public ResumeEmbeddedSchedulerWorkflowRequestBody(
            @JsonProperty(FIELD_NAME_WORKFLOW_NAME) String workflowName,
            @JsonProperty(FIELD_NAME_WORKFLOW_GROUP) String workflowGroup,
            @JsonProperty(FIELD_NAME_DYNAMIC_OPTIONS) Map<String, String> dynamicOptions) {
        this.workflowName = workflowName;
        this.workflowGroup = workflowGroup;
        this.dynamicOptions = dynamicOptions;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getWorkflowGroup() {
        return workflowGroup;
    }

    @Nullable
    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }
}
