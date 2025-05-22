/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions.agg;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** One segment of a bundled aggregate call, where all rows in the segment are for the same key. */
@PublicEvolving
public class BundledKeySegment {

    /** The common key of the segment. */
    private final RowData key;

    /** The rows, where all rows are for the common key. */
    private final List<RowData> rows;

    /** The accumulator value under the current key. Can be null. */
    private final List<RowData> accumulators;

    /**
     * If set, returns the updated value after each row is applied rather than only the final value.
     */
    private final boolean updatedValuesAfterEachRow;

    public BundledKeySegment(
            RowData key,
            List<RowData> rows,
            @Nullable RowData accumulator,
            boolean updatedValuesAfterEachRow) {
        this.key = key;
        this.rows = rows;
        this.accumulators =
                accumulator == null
                        ? Collections.emptyList()
                        : Collections.singletonList(accumulator);
        this.updatedValuesAfterEachRow = updatedValuesAfterEachRow;
    }

    public RowData getKey() {
        return key;
    }

    public List<RowData> getRows() {
        return rows;
    }

    @Nullable
    public RowData getAccumulator() {
        Preconditions.checkState(accumulators.size() <= 1);
        return accumulators.isEmpty() ? null : accumulators.get(0);
    }

    public List<RowData> getAccumulatorsToMerge() {
        return accumulators;
    }

    public boolean getUpdatedValuesAfterEachRow() {
        return updatedValuesAfterEachRow;
    }

    public static BundledKeySegment of(
            RowData key,
            List<RowData> rows,
            @Nullable RowData accumulator,
            boolean updatedValuesAfterEachRow) {
        return new BundledKeySegment(key, rows, accumulator, updatedValuesAfterEachRow);
    }
}
