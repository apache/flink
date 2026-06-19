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

package org.apache.flink.table.planner.plan.abilities.sink;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the row-level
 * update mode, columns & required physical column indices to/from JSON, but also can update
 * existing data for {@link org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("RowLevelUpdate")
public class RowLevelUpdateSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_UPDATED_COLUMNS = "updatedColumns";
    public static final String FIELD_NAME_ROW_LEVEL_UPDATE_MODE = "rowLevelUpdateMode";
    public static final String FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES =
            "requiredPhysicalColumnIndices";

    @JsonProperty(FIELD_NAME_UPDATED_COLUMNS)
    @Nonnull
    private final List<Column> updatedColumns;

    @JsonProperty(FIELD_NAME_ROW_LEVEL_UPDATE_MODE)
    @Nonnull
    private final SupportsRowLevelUpdate.RowLevelUpdateMode rowLevelUpdateMode;

    @JsonProperty(FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES)
    @Nonnull
    private final int[] requiredPhysicalColumnIndices;

    @JsonIgnore @Nullable private final RowLevelModificationScanContext scanContext;

    @JsonCreator
    public RowLevelUpdateSpec(
            @JsonProperty(FIELD_NAME_UPDATED_COLUMNS) @Nonnull List<Column> updatedColumns,
            @JsonProperty(FIELD_NAME_ROW_LEVEL_UPDATE_MODE) @Nonnull
                    SupportsRowLevelUpdate.RowLevelUpdateMode rowLevelUpdateMode,
            @Nullable RowLevelModificationScanContext scanContext,
            @JsonProperty(FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES) @Nonnull
                    int[] requiredPhysicalColumnIndices) {
        this.updatedColumns = updatedColumns;
        this.rowLevelUpdateMode = rowLevelUpdateMode;
        this.scanContext = scanContext;
        this.requiredPhysicalColumnIndices = requiredPhysicalColumnIndices;
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsRowLevelUpdate) {
            ((SupportsRowLevelUpdate) tableSink).applyRowLevelUpdate(updatedColumns, scanContext);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsRowLevelUpdate.",
                            tableSink.getClass().getName()));
        }
    }

    @Nonnull
    public SupportsRowLevelUpdate.RowLevelUpdateMode getRowLevelUpdateMode() {
        return rowLevelUpdateMode;
    }

    public int[] getRequiredPhysicalColumnIndices() {
        return requiredPhysicalColumnIndices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowLevelUpdateSpec that = (RowLevelUpdateSpec) o;
        return Objects.equals(updatedColumns, that.updatedColumns)
                && rowLevelUpdateMode == that.rowLevelUpdateMode
                && Arrays.equals(requiredPhysicalColumnIndices, that.requiredPhysicalColumnIndices)
                && Objects.equals(scanContext, that.scanContext);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(updatedColumns, rowLevelUpdateMode, scanContext);
        result = 31 * result + Arrays.hashCode(requiredPhysicalColumnIndices);
        return result;
    }
}
