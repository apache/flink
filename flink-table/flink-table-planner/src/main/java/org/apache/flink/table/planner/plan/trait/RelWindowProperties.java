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

package org.apache.flink.table.planner.plan.trait;

import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** It describes the information of window properties of a RelNode. */
public class RelWindowProperties {

    private final ImmutableBitSet windowStartColumns;
    private final ImmutableBitSet windowEndColumns;
    private final ImmutableBitSet windowTimeColumns;
    private final WindowSpec windowSpec;
    private final LogicalType timeAttributeType;

    /**
     * Creates a {@link RelWindowProperties}, may return null if the window properties can't be
     * propagated (loss window start and window end columns).
     */
    public static @Nullable RelWindowProperties create(
            ImmutableBitSet windowStartColumns,
            ImmutableBitSet windowEndColumns,
            ImmutableBitSet windowTimeColumns,
            WindowSpec windowSpec,
            LogicalType timeAttributeType) {
        if (windowStartColumns.isEmpty() || windowEndColumns.isEmpty()) {
            // the broadcast of window properties require both window_start and window_end
            return null;
        } else {
            return new RelWindowProperties(
                    windowStartColumns,
                    windowEndColumns,
                    windowTimeColumns,
                    windowSpec,
                    timeAttributeType);
        }
    }

    private RelWindowProperties(
            ImmutableBitSet windowStartColumns,
            ImmutableBitSet windowEndColumns,
            ImmutableBitSet windowTimeColumns,
            WindowSpec windowSpec,
            LogicalType timeAttributeType) {
        checkArgument(
                LogicalTypeChecks.isTimeAttribute(timeAttributeType),
                "Time attribute shouldn't be a '%s' type.",
                timeAttributeType);
        this.windowStartColumns = checkNotNull(windowStartColumns);
        this.windowEndColumns = checkNotNull(windowEndColumns);
        this.windowTimeColumns = checkNotNull(windowTimeColumns);
        this.windowSpec = checkNotNull(windowSpec);
        this.timeAttributeType = checkNotNull(timeAttributeType);
    }

    public @Nullable RelWindowProperties copy(
            ImmutableBitSet windowStartColumns,
            ImmutableBitSet windowEndColumns,
            ImmutableBitSet windowTimeColumns) {
        return create(
                windowStartColumns,
                windowEndColumns,
                windowTimeColumns,
                windowSpec,
                this.timeAttributeType);
    }

    public ImmutableBitSet getWindowStartColumns() {
        return windowStartColumns;
    }

    public ImmutableBitSet getWindowEndColumns() {
        return windowEndColumns;
    }

    public ImmutableBitSet getWindowTimeColumns() {
        return windowTimeColumns;
    }

    public ImmutableBitSet getWindowColumns() {
        return windowStartColumns.union(windowEndColumns).union(windowTimeColumns);
    }

    public WindowSpec getWindowSpec() {
        return windowSpec;
    }

    public boolean isRowtime() {
        return LogicalTypeChecks.isRowtimeAttribute(timeAttributeType);
    }

    public LogicalType getTimeAttributeType() {
        return timeAttributeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelWindowProperties that = (RelWindowProperties) o;
        return Objects.equals(windowStartColumns, that.windowStartColumns)
                && Objects.equals(windowEndColumns, that.windowEndColumns)
                && Objects.equals(windowTimeColumns, that.windowTimeColumns)
                && Objects.equals(windowSpec, that.windowSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStartColumns, windowEndColumns, windowTimeColumns, windowSpec);
    }

    @Override
    public String toString() {
        return String.format(
                "[windowStart=%s, windowEnd=%s, windowTime=%s, window=%s]",
                windowStartColumns, windowEndColumns, windowTimeColumns, windowSpec);
    }
}
