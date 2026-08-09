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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#ARRAY_FLATTEN}.
 *
 * <p>Flattens a nested array by one level.
 *
 * <p>NULL handling:
 *
 * <ul>
 *   <li>If the input array is NULL, returns NULL
 *   <li>NULL inner arrays are skipped
 *   <li>NULL elements within arrays are preserved
 * </ul>
 */
@Internal
public class ArrayFlattenFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter outerElementGetter;
    private final ArrayData.ElementGetter innerElementGetter;

    public ArrayFlattenFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_FLATTEN, context);

        // Get the input data type (ARRAY<ARRAY<T>>)
        final DataType inputDataType = context.getCallContext().getArgumentDataTypes().get(0);
        // Get the inner array type (ARRAY<T>)
        final DataType innerArrayDataType =
                ((CollectionDataType) inputDataType).getElementDataType();
        // Get the element type (T)
        final DataType elementDataType =
                ((CollectionDataType) innerArrayDataType).getElementDataType();

        // Create element getters
        // Outer getter retrieves inner arrays from the outer array
        outerElementGetter = ArrayData.createElementGetter(innerArrayDataType.getLogicalType());
        // Inner getter retrieves elements from inner arrays
        innerElementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
    }

    /**
     * Flattens a nested array by one level.
     *
     * @param array the input array of arrays
     * @return the flattened array, or NULL if input is NULL
     */
    public @Nullable ArrayData eval(ArrayData array) {
        if (array == null) {
            return null;
        }

        try {
            List<Object> result = new ArrayList<>();

            // Iterate through outer array
            for (int i = 0; i < array.size(); i++) {
                ArrayData innerArray = (ArrayData) outerElementGetter.getElementOrNull(array, i);

                if (innerArray == null) {
                    // Skip NULL inner arrays
                    continue;
                }

                // Iterate through inner array and add all elements (including NULL)
                for (int j = 0; j < innerArray.size(); j++) {
                    Object element = innerElementGetter.getElementOrNull(innerArray, j);
                    result.add(element); // Preserve NULL elements
                }
            }

            return new GenericArrayData(result.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
