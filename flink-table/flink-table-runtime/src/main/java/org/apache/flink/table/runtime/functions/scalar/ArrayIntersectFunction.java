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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_INTERSECT}. */
@Internal
public class ArrayIntersectFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    public ArrayIntersectFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_INTERSECT, context);
        final DataType elementDataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
    }

    public @Nullable ArrayData eval(ArrayData haystack, ArrayData needle) {
        int len1 = haystack.size();
        int len2 = needle.size();
        Object[] rs = new Object[Math.min(len1, len2)];
        Set<Object> hs = new HashSet<>();
        for (int i = 0; i < len1; i++) {
            final Object element1 = elementGetter.getElementOrNull(haystack, i);
            hs.add(element1);
        }

        int idx = 0;
        for (int j = 0; j < len2; j++) {
            final Object ele = elementGetter.getElementOrNull(needle, j);
            if (hs.contains(ele)) {
                rs[idx++] = ele;
            }
        }

        return new GenericArrayData(Arrays.copyOf(rs, idx));
    }
}
