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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.tools.RelBuilder;

/**
 * Information necessary to create a window aggregate.
 *
 * <p>Similar to {@link RelBuilder.AggCall} or {@link RelBuilder.GroupKey}.
 */
public class PlannerNamedWindowProperty {
    public static final String FIELD_NAME_NAME = "name";
    public static final String FIELD_NAME_PROPERTY = "property";

    @JsonProperty(FIELD_NAME_NAME)
    private final String name;

    @JsonProperty(FIELD_NAME_PROPERTY)
    private final PlannerWindowProperty property;

    @JsonCreator
    public PlannerNamedWindowProperty(
            @JsonProperty(FIELD_NAME_NAME) String name,
            @JsonProperty(FIELD_NAME_PROPERTY) PlannerWindowProperty property) {
        this.name = name;
        this.property = property;
    }

    public String getName() {
        return name;
    }

    public PlannerWindowProperty getProperty() {
        return property;
    }
}
