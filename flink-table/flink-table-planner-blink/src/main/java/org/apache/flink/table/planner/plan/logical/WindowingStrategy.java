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
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/** Logical representation of a windowing strategy. */
public abstract class WindowingStrategy {
    protected final WindowSpec window;
    protected final LogicalType timeAttributeType;
    protected final boolean isRowtime;

    protected WindowingStrategy(WindowSpec window, LogicalType timeAttributeType) {
        this(window, timeAttributeType, LogicalTypeChecks.isRowtimeAttribute(timeAttributeType));
    }

    protected WindowingStrategy(
            WindowSpec window, LogicalType timeAttributeType, boolean isRowtime) {
        this.window = window;
        this.timeAttributeType = timeAttributeType;
        this.isRowtime = isRowtime;
    }

    public abstract String toSummaryString(String[] inputFieldNames);

    public WindowSpec getWindow() {
        return window;
    }

    public LogicalType getTimeAttributeType() {
        return timeAttributeType;
    }

    public boolean isRowtime() {
        return isRowtime;
    }
}
