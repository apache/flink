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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** {@link TableSemantics} implementation for {@link ProcessTableFunctionTestHarness}. */
@Internal
class TestHarnessTableSemantics implements TableSemantics {
    private final DataType dataType;
    private final int[] partitionByColumns;
    private final List<int[]> upsertKeyColumns;
    private final int timeColumnIndex;

    TestHarnessTableSemantics(DataType dataType, int[] partitionByColumns) {
        this(dataType, partitionByColumns, Collections.emptyList(), -1);
    }

    TestHarnessTableSemantics(DataType dataType, int[] partitionByColumns, int timeColumnIndex) {
        this(dataType, partitionByColumns, Collections.emptyList(), timeColumnIndex);
    }

    TestHarnessTableSemantics(
            DataType dataType,
            int[] partitionByColumns,
            List<int[]> upsertKeyColumns,
            int timeColumnIndex) {
        this.dataType = dataType;
        this.partitionByColumns = partitionByColumns;
        this.upsertKeyColumns = upsertKeyColumns;
        this.timeColumnIndex = timeColumnIndex;
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
        return new int[0];
    }

    @Override
    public TableSemantics.SortDirection[] orderByDirections() {
        return new TableSemantics.SortDirection[0];
    }

    @Override
    public int timeColumn() {
        return timeColumnIndex;
    }

    @Override
    public Optional<ChangelogMode> changelogMode() {
        return Optional.empty();
    }

    @Override
    public List<int[]> upsertKeyColumns() {
        return upsertKeyColumns;
    }
}
