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

import java.util.LinkedHashSet;
import java.util.Set;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_DISTINCT}. */
@Internal
public class ArrayDistinctFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    public ArrayDistinctFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_DISTINCT, context);
        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
    }

    public @Nullable ArrayData eval(ArrayData haystack) {
        try {
            if (haystack == null) {
                return null;
            }
            Set set = new LinkedHashSet<>();
            final int size = haystack.size();
            for (int pos = 0; pos < size; pos++) {
                final Object element = elementGetter.getElementOrNull(haystack, pos);
                set.add(element);
            }
            return new GenericArrayData(set.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
