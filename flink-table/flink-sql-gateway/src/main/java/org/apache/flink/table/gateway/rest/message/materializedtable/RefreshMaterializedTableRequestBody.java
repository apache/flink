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

package org.apache.flink.table.gateway.rest.message.materializedtable;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/** {@link RequestBody} for executing materialized table refresh operation. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RefreshMaterializedTableRequestBody implements RequestBody {

    private static final String FIELD_NAME_IS_PERIODIC = "isPeriodic";
    private static final String FIELD_NAME_SCHEDULE_TIME = "scheduleTime";
    private static final String FIELD_NAME_DYNAMIC_OPTIONS = "dynamicOptions";
    private static final String FIELD_NAME_STATIC_PARTITIONS = "staticPartitions";
    private static final String FIELD_NAME_EXECUTION_CONFIG = "executionConfig";

    @JsonProperty(FIELD_NAME_IS_PERIODIC)
    private final boolean isPeriodic;

    @JsonProperty(FIELD_NAME_SCHEDULE_TIME)
    @Nullable
    private final String scheduleTime;

    @JsonProperty(FIELD_NAME_DYNAMIC_OPTIONS)
    @Nullable
    private final Map<String, String> dynamicOptions;

    @JsonProperty(FIELD_NAME_STATIC_PARTITIONS)
    @Nullable
    private final Map<String, String> staticPartitions;

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    @Nullable
    private final Map<String, String> executionConfig;

    @JsonCreator
    public RefreshMaterializedTableRequestBody(
            @JsonProperty(FIELD_NAME_IS_PERIODIC) boolean isPeriodic,
            @Nullable @JsonProperty(FIELD_NAME_SCHEDULE_TIME) String scheduleTime,
            @Nullable @JsonProperty(FIELD_NAME_DYNAMIC_OPTIONS) Map<String, String> dynamicOptions,
            @Nullable @JsonProperty(FIELD_NAME_STATIC_PARTITIONS)
                    Map<String, String> staticPartitions,
            @Nullable @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
                    Map<String, String> executionConfig) {
        this.isPeriodic = isPeriodic;
        this.scheduleTime = scheduleTime;
        this.dynamicOptions = dynamicOptions;
        this.staticPartitions = staticPartitions;
        this.executionConfig = executionConfig;
    }

    public boolean isPeriodic() {
        return isPeriodic;
    }

    @Nullable
    public String getScheduleTime() {
        return scheduleTime;
    }

    @Nullable
    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @Nullable
    public Map<String, String> getStaticPartitions() {
        return staticPartitions;
    }

    @Nullable
    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }
}
