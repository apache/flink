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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Iterator;
import java.util.function.BiFunction;

/** Built-in FIRST_VALUE with retraction aggregate function. */
@Internal
public final class FirstValueWithRetractAggFunction<T>
        extends FirstLastValueWithRetractAggFunctionBase<T> {

    private static final BiFunction<Long, Long, Boolean> comparator = (x, y) -> (x - y) > 0;

    public FirstValueWithRetractAggFunction(LogicalType valueType) {
        super(valueType);
    }

    @SuppressWarnings("unchecked")
    public void accumulate(FirstLastValueWithRetractAccumulator<T> acc, Object value)
            throws Exception {
        accumulate(acc, value, comparator);
    }

    @SuppressWarnings("unchecked")
    public void accumulate(FirstLastValueWithRetractAccumulator<T> acc, Object value, Long order)
            throws Exception {
        accumulate(acc, value, order, comparator);
    }

    public void accumulate(FirstLastValueWithRetractAccumulator<T> acc, StringData value)
            throws Exception {
        accumulate(acc, (Object) ((BinaryStringData) value).copy(), comparator);
    }

    public void accumulate(
            FirstLastValueWithRetractAccumulator<T> acc, StringData value, Long order)
            throws Exception {
        accumulate(acc, (Object) ((BinaryStringData) value).copy(), order, comparator);
    }

    public void merge(
            FirstLastValueWithRetractAccumulator<T> acc,
            Iterable<FirstLastValueWithRetractAccumulator<T>> its)
            throws Exception {
        merge(acc, its, comparator);
    }

    /**
     * Update the first value if it is retracted. Use the minimal order which is greater than or
     * equal to current value.
     */
    @Override
    void updateValue(FirstLastValueWithRetractAccumulator<T> acc) throws Exception {
        Long startKey = acc.order;
        Iterator<Long> iter = acc.orderToValueMap.keys().iterator();
        // find the minimal order which is greater than or equal to `startKey`
        Long nextKey = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Long key = iter.next();
            if (key >= startKey && key < nextKey) {
                nextKey = key;
            }
        }

        if (nextKey != Long.MAX_VALUE) {
            acc.value = acc.orderToValueMap.get(nextKey).get(0);
            acc.order = nextKey;
        } else {
            acc.value = null;
            acc.order = null;
        }
    }
}
