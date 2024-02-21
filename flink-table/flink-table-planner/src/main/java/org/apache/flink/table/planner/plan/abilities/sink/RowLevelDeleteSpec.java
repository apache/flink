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
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * A sub-class of {@link SinkAbilitySpec} that can not only serialize/deserialize the row-level
 * delete mode & required physical column indices to/from JSON, but also can delete existing data
 * for {@link org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("RowLevelDelete")
public class RowLevelDeleteSpec implements SinkAbilitySpec {
    public static final String FIELD_NAME_ROW_LEVEL_DELETE_MODE = "rowLevelDeleteMode";
    public static final String FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES =
            "requiredPhysicalColumnIndices";

    @JsonProperty(FIELD_NAME_ROW_LEVEL_DELETE_MODE)
    @Nonnull
    private final SupportsRowLevelDelete.RowLevelDeleteMode rowLevelDeleteMode;

    @JsonIgnore @Nullable private final RowLevelModificationScanContext scanContext;

    @JsonProperty(FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES)
    @Nonnull
    private final int[] requiredPhysicalColumnIndices;

    @JsonCreator
    public RowLevelDeleteSpec(
            @JsonProperty(FIELD_NAME_ROW_LEVEL_DELETE_MODE) @Nonnull
                    SupportsRowLevelDelete.RowLevelDeleteMode rowLevelDeleteMode,
            @Nullable RowLevelModificationScanContext scanContext,
            @JsonProperty(FIELD_NAME_REQUIRED_PHYSICAL_COLUMN_INDICES) @Nonnull
                    int[] requiredPhysicalColumnIndices) {
        this.rowLevelDeleteMode = Preconditions.checkNotNull(rowLevelDeleteMode);
        this.scanContext = scanContext;
        this.requiredPhysicalColumnIndices = requiredPhysicalColumnIndices;
    }

    @Override
    public void apply(DynamicTableSink tableSink) {
        if (tableSink instanceof SupportsRowLevelDelete) {
            ((SupportsRowLevelDelete) tableSink).applyRowLevelDelete(scanContext);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsRowLevelDelete.",
                            tableSink.getClass().getName()));
        }
    }

    @Nonnull
    public SupportsRowLevelDelete.RowLevelDeleteMode getRowLevelDeleteMode() {
        return rowLevelDeleteMode;
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
        RowLevelDeleteSpec that = (RowLevelDeleteSpec) o;
        return rowLevelDeleteMode == that.rowLevelDeleteMode
                && Objects.equals(scanContext, that.scanContext)
                && Arrays.equals(requiredPhysicalColumnIndices, that.requiredPhysicalColumnIndices);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowLevelDeleteMode, scanContext);
        result = 31 * result + Arrays.hashCode(requiredPhysicalColumnIndices);
        return result;
    }
}
