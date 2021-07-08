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

package org.apache.flink.table.planner.plan.logical;

import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A windowing strategy that gets windows by calculating on time attribute column. */
@JsonTypeName("TimeAttribute")
public class TimeAttributeWindowingStrategy extends WindowingStrategy {
    public static final String FIELD_NAME_TIME_ATTRIBUTE_INDEX = "timeAttributeIndex";

    @JsonProperty(FIELD_NAME_TIME_ATTRIBUTE_INDEX)
    private final int timeAttributeIndex;

    @JsonCreator
    public TimeAttributeWindowingStrategy(
            @JsonProperty(FIELD_NAME_WINDOW) WindowSpec window,
            @JsonProperty(value = FIELD_NAME_TIME_ATTRIBUTE_TYPE) LogicalType timeAttributeType,
            @JsonProperty(FIELD_NAME_TIME_ATTRIBUTE_INDEX) int timeAttributeIndex) {
        super(window, timeAttributeType);
        this.timeAttributeIndex = timeAttributeIndex;
    }

    @Override
    public String toSummaryString(String[] inputFieldNames) {
        checkArgument(timeAttributeIndex >= 0 && timeAttributeIndex < inputFieldNames.length);
        String windowing = String.format("time_col=[%s]", inputFieldNames[timeAttributeIndex]);
        return window.toSummaryString(windowing);
    }

    public int getTimeAttributeIndex() {
        return timeAttributeIndex;
    }
}
