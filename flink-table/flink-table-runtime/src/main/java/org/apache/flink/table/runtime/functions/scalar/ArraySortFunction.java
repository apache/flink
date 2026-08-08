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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Comparator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;

/** Implementation of ARRAY_SORT function. */
@Internal
public class ArraySortFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;
    private final SpecializedFunction.ExpressionEvaluator compareEvaluator;

    private transient MethodHandle compareHandle;

    public ArraySortFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_SORT, context);
        final DataType elementDataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType()
                        .toInternal();
        elementGetter =
                ArrayData.createElementGetter(elementDataType.toInternal().getLogicalType());
        compareEvaluator =
                context.createEvaluator(
                        unresolvedCall(
                                BuiltInFunctionDefinitions.INTERNAL_COMPARE,
                                $("element1"),
                                $("element2")),
                        DataTypes.INT().notNull(),
                        DataTypes.FIELD("element1", elementDataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", elementDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        compareHandle = compareEvaluator.open(context);
    }

    public @Nullable ArrayData eval(ArrayData array) {
        return eval(array, true, true);
    }

    public @Nullable ArrayData eval(ArrayData array, Boolean ascendingOrder) {
        return eval(array, ascendingOrder, ascendingOrder);
    }

    public @Nullable ArrayData eval(ArrayData array, Boolean ascendingOrder, Boolean nullFirst) {
        try {
            if (array == null || ascendingOrder == null || nullFirst == null) {
                return null;
            }
            if (array.size() == 0) {
                return array;
            }
            Object[] elements = new Object[array.size()];
            for (int i = 0; i < array.size(); i++) {
                elements[i] = elementGetter.getElementOrNull(array, i);
            }
            Comparator<Object> ascendingComparator = new ArraySortComparator(ascendingOrder);
            Arrays.sort(
                    elements,
                    nullFirst
                            ? Comparator.nullsFirst(ascendingComparator)
                            : Comparator.nullsLast(ascendingComparator));
            return new GenericArrayData(elements);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private class ArraySortComparator implements Comparator<Object> {
        private final boolean isAscending;

        public ArraySortComparator(boolean isAscending) {
            this.isAscending = isAscending;
        }

        @Override
        public int compare(Object o1, Object o2) {
            try {
                int cmp = (int) compareHandle.invoke(o1, o2);
                return isAscending ? cmp : -cmp;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        compareEvaluator.close();
    }
}
