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

import java.util.Collections;
import java.util.List;

/**
 * The result of applying rows to the provided accumulator(s) in {@link BundledKeySegment}. The
 * result is an updated accumulator and values evaluated at the start, end, and potentially after
 * each row.
 */
@PublicEvolving
public class BundledKeySegmentApplied {

    /** The accumulator value after all rows are applied. */
    private final RowData accumulator;

    /** The value before any rows are applied. */
    private final RowData startingValue;

    /** The value after all rows are applied. */
    private final RowData finalValue;

    /** The value after each row is applied. */
    private final List<RowData> updatedValuesAfterEachRow;

    public BundledKeySegmentApplied(
            RowData accumulator,
            RowData startingValue,
            RowData finalValue,
            List<RowData> updatedValuesAfterEachRow) {
        this.accumulator = accumulator;
        this.startingValue = startingValue;
        this.finalValue = finalValue;
        this.updatedValuesAfterEachRow = updatedValuesAfterEachRow;
    }

    public static BundledKeySegmentApplied of(
            RowData accumulator,
            RowData startingValue,
            RowData finalValue,
            List<RowData> updatedValuesAfterEachRow) {
        return new BundledKeySegmentApplied(
                accumulator, startingValue, finalValue, updatedValuesAfterEachRow);
    }

    public static BundledKeySegmentApplied of(RowData accumulator) {
        return new BundledKeySegmentApplied(accumulator, null, null, Collections.emptyList());
    }

    public RowData getAccumulator() {
        return accumulator;
    }

    public RowData getStartingValue() {
        return startingValue;
    }

    public RowData getFinalValue() {
        return finalValue;
    }

    public List<RowData> getUpdatedValuesAfterEachRow() {
        return updatedValuesAfterEachRow;
    }

    @Override
    public String toString() {
        return "{" + accumulator + "," + startingValue + "," + finalValue;
    }
}
