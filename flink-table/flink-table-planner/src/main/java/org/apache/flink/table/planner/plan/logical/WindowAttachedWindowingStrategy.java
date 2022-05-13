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

/**
 * A windowing strategy that gets windows from input columns as windows have been assigned and
 * attached to the physical columns.
 */
@JsonTypeName("WindowAttached")
public class WindowAttachedWindowingStrategy extends WindowingStrategy {
    public static final String FIELD_NAME_WINDOW_START = "windowStart";
    public static final String FIELD_NAME_WINDOW_END = "windowEnd";

    @JsonProperty(FIELD_NAME_WINDOW_START)
    private final int windowStart;

    @JsonProperty(FIELD_NAME_WINDOW_END)
    private final int windowEnd;

    @JsonCreator
    public WindowAttachedWindowingStrategy(
            @JsonProperty(FIELD_NAME_WINDOW) WindowSpec window,
            @JsonProperty(value = FIELD_NAME_TIME_ATTRIBUTE_TYPE) LogicalType timeAttributeType,
            @JsonProperty(FIELD_NAME_WINDOW_START) int windowStart,
            @JsonProperty(FIELD_NAME_WINDOW_END) int windowEnd) {
        super(window, timeAttributeType);
        checkArgument(windowEnd >= 0);
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    /**
     * Creates a {@link WindowAttachedWindowingStrategy} which only {@link #windowEnd} is attatched.
     */
    public WindowAttachedWindowingStrategy(
            WindowSpec window, LogicalType timeAttributeType, int windowEnd) {
        super(window, timeAttributeType);
        checkArgument(windowEnd >= 0);
        this.windowStart = -1;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toSummaryString(String[] inputFieldNames) {
        checkArgument(windowEnd < inputFieldNames.length);
        final String windowing;
        if (windowStart < 0) {
            windowing = String.format("win_end=[%s]", inputFieldNames[windowEnd]);
        } else {
            checkArgument(windowStart < inputFieldNames.length);
            windowing =
                    String.format(
                            "win_start=[%s], win_end=[%s]",
                            inputFieldNames[windowStart], inputFieldNames[windowEnd]);
        }
        return window.toSummaryString(windowing);
    }

    public int getWindowStart() {
        return windowStart;
    }

    public int getWindowEnd() {
        return windowEnd;
    }
}
