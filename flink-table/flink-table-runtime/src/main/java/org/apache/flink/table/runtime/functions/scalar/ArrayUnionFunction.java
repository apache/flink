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
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_UNION}. */
@Internal
public class ArrayUnionFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    public ArrayUnionFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_UNION, context);

        final DataType dataType =
                // Since input arrays could be with different nullability we can not rely just on
                // the first element.
                ((CollectionDataType) context.getCallContext().getOutputDataType().get())
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
        equalityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
    }

    public @Nullable ArrayData eval(ArrayData array1, ArrayData array2) {
        try {
            if (array1 == null || array2 == null) {
                return null;
            }

            List list = new ArrayList();
            Boolean[] alreadyIncludeNull = {Boolean.FALSE};
            distinct(array1, elementGetter, equalityHandle, alreadyIncludeNull, list);
            distinct(array2, elementGetter, equalityHandle, alreadyIncludeNull, list);
            return new GenericArrayData(list.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    List distinct(
            ArrayData array,
            ArrayData.ElementGetter elementGetter,
            MethodHandle equalityHandle,
            Boolean[] alreadyIncludeNull,
            List list)
            throws Throwable {

        for (int i = 0; i < array.size(); i++) {
            final Object element = elementGetter.getElementOrNull(array, i);

            boolean found = false;
            if (element == null) {
                if (alreadyIncludeNull[0]) {
                    found = true;
                } else {
                    alreadyIncludeNull[0] = Boolean.TRUE;
                }
            } else {
                // check elem is already stored in arrayBuffer or not?
                int j = 0;
                while (!found && j < list.size()) {
                    Object va = list.get(j);
                    if (va != null && (boolean) equalityHandle.invoke(element, va)) {
                        found = true;
                    }
                    j = j + 1;
                }
            }
            if (!found) {
                list.add(element);
            }
        }
        return list;
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
