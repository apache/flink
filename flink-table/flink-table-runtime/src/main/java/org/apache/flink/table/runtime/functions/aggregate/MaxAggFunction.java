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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in MAX aggregate function. */
@Internal
public final class MaxAggFunction<T extends Comparable<T>>
        extends BuiltInAggregateFunction<T, RowData> {

    private static final long serialVersionUID = 1L;

    private final transient DataType valueDataType;

    public MaxAggFunction(LogicalType valueType) {
        this.valueDataType = toInternalDataType(valueType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.ROW(DataTypes.FIELD("maxValue", valueDataType.nullable()))
                .bridgedTo(RowData.class);
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataType;
    }

    @Override
    public RowData createAccumulator() {
        GenericRowData acc = new GenericRowData(1);
        acc.setField(0, null);
        return acc;
    }

    public void accumulate(RowData rowData, Comparable value) {
        if (value != null) {
            GenericRowData acc = (GenericRowData) rowData;
            Comparable maxValue = (Comparable) acc.getField(0);
            maxValue = max(maxValue, value);
            acc.setField(0, maxValue);
        }
    }

    // See optimization in FlinkRelMdModifiedMonotonicity.
    // This function can ignore retraction message:
    // SQL: SELECT MAX(cnt), SUM(cnt) FROM (SELECT count(a) as cnt FROM T GROUP BY b)
    // The cnt is modified increasing, so the MAX(cnt) can ignore retraction message. But this
    // doesn't mean that the node won't receive the retraction message, because there are other
    // aggregate operators that need retraction message, such as SUM(cnt).
    public void retract(RowData rowData, Comparable value) throws Exception {}

    public void merge(RowData rowData, Iterable<RowData> its) throws Exception {
        GenericRowData acc = (GenericRowData) rowData;
        Comparable maxValue = (Comparable) acc.getField(0);
        for (RowData otherAcc : its) {
            Comparable value = (Comparable) ((GenericRowData) otherAcc).getField(0);
            maxValue = max(maxValue, value);
        }
        acc.setField(0, maxValue);
    }

    public void resetAccumulator(RowData rowData) {
        GenericRowData acc = (GenericRowData) rowData;
        acc.setField(0, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getValue(RowData rowData) {
        GenericRowData acc = (GenericRowData) rowData;
        return (T) acc.getField(0);
    }

    public static <T extends Comparable<T>> T max(T a, T b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return (a.compareTo(b) >= 0) ? a : b;
    }
}
