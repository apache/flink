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

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_CONCAT}. */
@Internal
public class ArrayConcatFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    public ArrayConcatFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_CONCAT, context);
        final DataType dataType =
                // Since input arrays could be with different nullability we can not rely just on
                // the first element.
                ((CollectionDataType) context.getCallContext().getOutputDataType().get())
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
    }

    public @Nullable ArrayData eval(ArrayData... arrays) {
        if (arrays == null || arrays.length == 0) {
            return null;
        }
        if (arrays.length == 1) {
            return arrays[0];
        }
        try {
            List<Object> list = new ArrayList<>();
            for (ArrayData array : arrays) {
                if (array != null) {
                    appendElements(array, elementGetter, list);
                } else {
                    return null;
                }
            }
            return new GenericArrayData(list.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    void appendElements(ArrayData array, ArrayData.ElementGetter elementGetter, List<Object> list)
            throws Throwable {
        for (int i = 0; i < array.size(); ++i) {
            final Object element = elementGetter.getElementOrNull(array, i);
            list.add(element);
        }
    }
}
