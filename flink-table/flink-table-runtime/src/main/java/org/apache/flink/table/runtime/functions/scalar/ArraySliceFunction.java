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

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_SLICE}. */
@Internal
public class ArraySliceFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    public ArraySliceFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_SLICE, context);
        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
    }

    public @Nullable ArrayData eval(
            @Nullable ArrayData array, @Nullable Integer start, @Nullable Integer end) {
        try {
            if (array == null || start == null || end == null) {
                return null;
            }
            if (array.size() == 0) {
                return array;
            }
            int startIndex = start;
            int endIndex = end;
            startIndex += startIndex < 0 ? array.size() + 1 : 0;
            endIndex += endIndex < 0 ? array.size() + 1 : 0;
            startIndex = Math.max(1, startIndex);
            endIndex = endIndex == 0 ? 1 : Math.min(endIndex, array.size());
            if (endIndex < startIndex) {
                return new GenericArrayData(new Object[0]);
            }
            if (startIndex == 1 && endIndex == array.size()) {
                return array;
            }
            List<Object> slicedArray = new ArrayList<>();
            for (int i = startIndex - 1; i <= endIndex - 1; i++) {
                slicedArray.add(elementGetter.getElementOrNull(array, i));
            }
            return new GenericArrayData(slicedArray.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    public @Nullable ArrayData eval(@Nullable ArrayData array, @Nullable Integer start) {
        return array == null ? null : eval(array, start, array.size());
    }
}
