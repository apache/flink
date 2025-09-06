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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Optional;

/** Mock implementation of {@link TableSemantics} for testing purposes. */
public class TableSemanticsMock implements TableSemantics {

    private final DataType dataType;
    private final int[] partitionByColumns;
    private final int[] orderByColumns;
    private final int timeColumn;
    private final ChangelogMode changelogMode;

    public TableSemanticsMock(DataType dataType) {
        this(dataType, new int[0], new int[0], -1, null);
    }

    public TableSemanticsMock(
            DataType dataType,
            int[] partitionByColumns,
            int[] orderByColumns,
            int timeColumn,
            @Nullable ChangelogMode changelogMode) {
        this.dataType = dataType;
        this.partitionByColumns = partitionByColumns;
        this.orderByColumns = orderByColumns;
        this.timeColumn = timeColumn;
        this.changelogMode = changelogMode;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public int[] partitionByColumns() {
        return partitionByColumns;
    }

    @Override
    public int[] orderByColumns() {
        return orderByColumns;
    }

    @Override
    public int timeColumn() {
        return timeColumn;
    }

    @Override
    public Optional<ChangelogMode> changelogMode() {
        return Optional.ofNullable(changelogMode);
    }
}
