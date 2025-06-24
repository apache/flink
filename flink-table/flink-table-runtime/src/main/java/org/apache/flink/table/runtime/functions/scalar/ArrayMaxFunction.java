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

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_MAX}. */
@Internal
public class ArrayMaxFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;
    private final SpecializedFunction.ExpressionEvaluator compareEvaluator;
    private transient MethodHandle compareHandle;

    public ArrayMaxFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_MAX, context);

        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());
        compareEvaluator =
                context.createEvaluator(
                        $("element1").isGreater($("element2")),
                        DataTypes.BOOLEAN().notNull(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        compareHandle = compareEvaluator.open(context);
    }

    public @Nullable Object eval(ArrayData array) {
        try {
            if (array == null || array.size() == 0) {
                return null;
            }

            Object maxElement = null;
            for (int i = 0; i < array.size(); i++) {
                Object element = elementGetter.getElementOrNull(array, i);
                if (element != null) {
                    if (maxElement == null || (boolean) compareHandle.invoke(element, maxElement)) {
                        maxElement = element;
                    }
                }
            }
            return maxElement;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        compareEvaluator.close();
    }
}
