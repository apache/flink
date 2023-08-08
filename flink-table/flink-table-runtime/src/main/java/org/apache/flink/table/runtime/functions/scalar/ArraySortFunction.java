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

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of ARRAY_SORT function. */
@Internal
public class ArraySortFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;
    private final SpecializedFunction.ExpressionEvaluator greaterEvaluator;

    private transient MethodHandle greaterHandle;

    public ArraySortFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_SORT, context);
        final DataType elementDataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
        greaterEvaluator =
                context.createEvaluator(
                        $("element1").isGreater($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", elementDataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", elementDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        greaterHandle = greaterEvaluator.open(context);
    }

    public @Nullable ArrayData eval(ArrayData array, Boolean... ascendingOrder) {
        try {
            if (array == null || ascendingOrder.length > 0 && ascendingOrder[0] == null) {
                return null;
            }
            if (array.size() == 0) {
                return array;
            }

            Object[] elements = new Object[array.size()];
            for (int i = 0; i < array.size(); i++) {
                elements[i] = elementGetter.getElementOrNull(array, i);
            }

            boolean isAscending = ascendingOrder.length > 0 ? ascendingOrder[0] : true;
            Object[] temp = new Object[elements.length];
            mergeSort(elements, temp, 0, elements.length - 1, isAscending);

            return new GenericArrayData(elements);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private void mergeSort(Object[] array, Object[] temp, int left, int right, boolean isAscending)
            throws Throwable {
        if (left >= right) {
            return;
        }
        int mid = left + (right - left) / 2;
        mergeSort(array, temp, left, mid, isAscending);
        mergeSort(array, temp, mid + 1, right, isAscending);
        merge(array, temp, left, mid, right, isAscending);
    }

    private void merge(
            Object[] array, Object[] temp, int left, int mid, int right, boolean isAscending)
            throws Throwable {
        int i = left, j = mid + 1, k = 0;

        while (i <= mid && j <= right) {
            if (array[i] == null) {
                if (isAscending) {
                    temp[k++] = array[i++];
                } else {
                    temp[k++] = array[j++];
                }
            } else if (array[j] == null) {
                if (isAscending) {
                    temp[k++] = array[j++];
                } else {
                    temp[k++] = array[i++];
                }
            } else {
                try {
                    boolean result = (boolean) (greaterHandle.invoke(array[i], array[j]));
                    if (isAscending) {
                        if (result) {
                            temp[k++] = array[j++];
                        } else {
                            temp[k++] = array[i++];
                        }
                    } else {
                        if (result) {
                            temp[k++] = array[i++];
                        } else {
                            temp[k++] = array[j++];
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        while (i <= mid) {
            if (array[i] == null && !isAscending) {
                break;
            }
            temp[k++] = array[i++];
        }

        while (j <= right) {
            if (array[j] == null && !isAscending) {
                break;
            }
            temp[k++] = array[j++];
        }
        while (i <= mid) {
            temp[k++] = array[i++];
        }

        while (j <= right) {
            temp[k++] = array[j++];
        }

        for (int index = 0; index < k; index++) {
            array[left + index] = temp[index];
        }
    }

    @Override
    public void close() throws Exception {
        greaterEvaluator.close();
    }
}
