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
import org.apache.flink.table.api.Expressions;
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

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_INTERSECT}. */
@Internal
public class ArrayIntersectFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    public ArrayIntersectFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_INTERSECT, context);

        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
        equalityEvaluator =
                context.createEvaluator(
                        Expressions.call("HASHCODE", $("element1")),
                        DataTypes.INT(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()));
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

            List list = new ArrayList<>();
            boolean alreadySeenNull = false;
            for (int i = 0; i < array1.size(); i++) {
                boolean found = false;
                final Object element1 = elementGetter.getElementOrNull(array1, i);
                if (array1.isNullAt(i)) {
                    if (!alreadySeenNull) {
                        for (int j = 0; j < array2.size(); j++) {
                            found = array2.isNullAt(j);
                            if (found) {
                                break;
                            }
                        }
                        alreadySeenNull = true;
                    }
                } else {
                    for (int j = 0; j < array2.size(); j++) {
                        if (!array2.isNullAt(j)) {
                            final Object element2 = elementGetter.getElementOrNull(array2, j);
                            if ((boolean) equalityHandle.invoke(element1)) {
                                boolean foundArrayBuffer = false;
                                for (int k = 0; k < list.size(); k++) {
                                    Object va = list.get(k);
                                    foundArrayBuffer =
                                            (va != null) && (boolean) equalityHandle.invoke(va);
                                    if (foundArrayBuffer) {
                                        break;
                                    }
                                }
                                found = !foundArrayBuffer;
                            }
                        }
                        if (found) {
                            break;
                        }
                    }
                }
                if (found) {
                    list.add(element1);
                }
            }
            return new GenericArrayData(list.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
