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

package org.apache.flink.table.runtime.operators.deduplicate.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.operators.deduplicate.asyncprocessing.AsyncStateRowTimeDeduplicateFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import static org.apache.flink.table.runtime.operators.deduplicate.utils.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.utils.DeduplicateFunctionHelper.isDuplicate;
import static org.apache.flink.table.runtime.operators.deduplicate.utils.DeduplicateFunctionHelper.updateDeduplicateResult;

/**
 * A helper to deduplicate data with row time in {@link RowTimeDeduplicateFunction} and {@link
 * AsyncStateRowTimeDeduplicateFunction}.
 */
public abstract class RowTimeDeduplicateFunctionHelper {

    private final boolean generateUpdateBefore;
    private final boolean generateInsert;
    private final int rowtimeIndex;
    private final boolean keepLastRow;

    public RowTimeDeduplicateFunctionHelper(
            boolean generateUpdateBefore,
            boolean generateInsert,
            int rowtimeIndex,
            boolean keepLastRow) {
        this.generateUpdateBefore = generateUpdateBefore;
        this.generateInsert = generateInsert;
        this.rowtimeIndex = rowtimeIndex;
        this.keepLastRow = keepLastRow;
    }

    /**
     * Processes element to deduplicate on keys with row time semantic, sends current element if it
     * is last or first row, retracts previous element if needed.
     *
     * @param currentRow latest row received by deduplicate function
     * @param prevRow previous row received by deduplicate function. `null` if current row is the
     *     first row
     * @param out underlying collector
     */
    public void deduplicateOnRowTime(
            RowData currentRow, @Nullable RowData prevRow, Collector<RowData> out)
            throws Exception {
        checkInsertOnly(currentRow);

        if (isDuplicate(prevRow, currentRow, rowtimeIndex, keepLastRow)) {
            updateDeduplicateResult(generateUpdateBefore, generateInsert, prevRow, currentRow, out);
            updateState(currentRow);
        }
    }

    protected abstract void updateState(RowData currentRow) throws Exception;
}
