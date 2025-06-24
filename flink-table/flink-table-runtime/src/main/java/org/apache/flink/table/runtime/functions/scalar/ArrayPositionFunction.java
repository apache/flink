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
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_POSITION}. */
@Internal
public class ArrayPositionFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;
    private final ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    public ArrayPositionFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_POSITION, context);
        final DataType elementDataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        final DataType needleDataType = context.getCallContext().getArgumentDataTypes().get(1);
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
        equalityEvaluator =
                context.createEvaluator(
                        $("element").isEqual($("needle")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element", elementDataType.notNull().toInternal()),
                        DataTypes.FIELD("needle", needleDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
    }

    public @Nullable Integer eval(ArrayData haystack, Object needle) {
        try {
            if (haystack == null || needle == null) {
                return null;
            }
            final int size = haystack.size();
            for (int pos = 0; pos < size; pos++) {
                final Object element = elementGetter.getElementOrNull(haystack, pos);
                if (element != null) {
                    final boolean isEqual = (boolean) equalityHandle.invoke(element, needle);
                    if (isEqual) {
                        return Integer.valueOf(pos + 1);
                    }
                }
            }
            return 0;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
