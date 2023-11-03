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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_CONTAINS_SEQ}. */
@Internal
public class ArrayContainsSeqFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;

    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    public ArrayContainsSeqFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_CONTAINS_SEQ, context);

        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(1))
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

    public @Nullable Boolean eval(ArrayData array1, ArrayData array2) {
        try {
            if (array1.size() < array2.size()) {
                return false;
            }
            int i = 0;
            int j = 0;
            boolean isEqual = false;
            while (i < array1.size()) {
                int k = i;
                int l = 0;
                while (j < array2.size()) {
                    final Object element1 = elementGetter.getElementOrNull(array1, k);
                    final Object element2 = elementGetter.getElementOrNull(array2, j);
                    // handle nullability before to avoid SQL three-value logic for equality
                    // because in SQL `NULL == NULL` would return `NULL` and not `TRUE`
                    if (element1 == null && element2 == null) {
                        isEqual = true;
                    } else if (element1 != null && element2 != null) {
                        isEqual = (boolean) equalityHandle.invoke(element1, element2);
                    }
                    if (isEqual) {
                        k++;
                        l++;
                    }
                    j++;
                }
                if (l == array2.size()) {
                    return true;
                }
                j = 0;
                i++;
            }
            return false;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
