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
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/** Built-in ARRAY_AGG aggregate function. */
@Internal
public final class ArrayAggFunction<T>
        extends BuiltInAggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<T>> {

    private static final long serialVersionUID = -5860934997657147836L;

    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;

    private transient MethodHandle equalityHandle;

    private transient DataType elementDataType;

    public ArrayAggFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_AGG, context);
        this.elementDataType =
                DataTypeUtils.toInternalDataType(
                        context.getCallContext().getArgumentDataTypes().get(0));
        this.equalityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", elementDataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", elementDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                ArrayAggAccumulator.class,
                DataTypes.FIELD("list", ListView.newListViewDataType(elementDataType.notNull())),
                DataTypes.FIELD(
                        "retractList", ListView.newListViewDataType(elementDataType.notNull())));
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for ARRAY_AGG with retraction. */
    public static class ArrayAggAccumulator<T> {
        public ListView<T> list;
        public ListView<T> retractList;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ArrayAggAccumulator<?> that = (ArrayAggAccumulator<?>) o;
            return Objects.equals(list, that.list) && Objects.equals(retractList, that.retractList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(list, retractList);
        }
    }

    @Override
    public ArrayAggAccumulator<T> createAccumulator() {
        final ArrayAggAccumulator<T> acc = new ArrayAggAccumulator<>();
        acc.list = new ListView<>();
        acc.retractList = new ListView<>();
        return acc;
    }

    public void accumulate(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            acc.list.add(value);
        }
    }

    public void retract(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            if (!remove(acc.list.get(), value)) {
                acc.retractList.add(value);
            }
        }
    }

    public void merge(ArrayAggAccumulator<T> acc, Iterable<ArrayAggAccumulator<T>> its)
            throws Exception {
        for (ArrayAggAccumulator<T> otherAcc : its) {
            // merge list of acc and other
            List<T> buffer = new LinkedList<>();
            for (T element : acc.list.get()) {
                buffer.add(element);
            }
            for (T element : otherAcc.list.get()) {
                buffer.add(element);
            }
            // merge retract list of acc and other
            List<T> retractBuffer = new ArrayList<>();
            for (T element : acc.retractList.get()) {
                retractBuffer.add(element);
            }
            for (T element : otherAcc.retractList.get()) {
                retractBuffer.add(element);
            }

            // merge list & retract list
            List<T> newRetractBuffer = new ArrayList<>();
            for (T element : retractBuffer) {
                if (!remove(buffer, element)) {
                    newRetractBuffer.add(element);
                }
            }

            // update to acc
            acc.list.clear();
            acc.list.addAll(buffer);
            acc.retractList.clear();
            acc.retractList.addAll(newRetractBuffer);
        }
    }

    private boolean remove(Iterable<T> iterable, T value) {
        try {
            Iterator<T> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                if ((boolean) equalityHandle.invoke(iterator.next(), value)) {
                    iterator.remove();
                    return true;
                }
            }
            return false;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public ArrayData getValue(ArrayAggAccumulator<T> acc) {
        try {
            List<T> accList = acc.list.getList();
            if (accList == null || accList.isEmpty()) {
                // array_agg returns null rather than an empty array when there are no input rows.
                return null;
            } else {
                return new GenericArrayData(accList.toArray());
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public void resetAccumulator(ArrayAggAccumulator<T> acc) {
        acc.list.clear();
        acc.retractList.clear();
    }
}
