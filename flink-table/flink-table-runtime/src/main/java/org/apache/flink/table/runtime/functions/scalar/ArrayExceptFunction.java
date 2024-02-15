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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.util.EqualityAndHashcodeProvider;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_EXCEPT}. */
@Internal
public class ArrayExceptFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;
    private final EqualityAndHashcodeProvider equalityAndHashcodeProvider;

    public ArrayExceptFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_EXCEPT, context);
        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType()
                        .toInternal();
        elementGetter = ArrayData.createElementGetter(dataType.toInternal().getLogicalType());
        this.equalityAndHashcodeProvider = new EqualityAndHashcodeProvider(context, dataType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityAndHashcodeProvider.open(context);
    }

    @Override
    public void close() throws Exception {
        equalityAndHashcodeProvider.close();
    }

    public @Nullable ArrayData eval(ArrayData arrayOne, ArrayData arrayTwo) {
        try {
            if (arrayOne == null) {
                return null;
            }

            List<Object> list = new ArrayList<>();
            Set<ObjectContainer> seen = new HashSet<>();

            boolean isNullPresentInArrayTwo = false;
            if (arrayTwo != null) {
                for (int pos = 0; pos < arrayTwo.size(); pos++) {
                    final Object element = elementGetter.getElementOrNull(arrayTwo, pos);
                    if (element == null) {
                        isNullPresentInArrayTwo = true;
                    } else {
                        ObjectContainer objectContainer = new ObjectContainer(element);
                        seen.add(objectContainer);
                    }
                }
            }
            boolean isNullPresentInArrayOne = false;
            for (int pos = 0; pos < arrayOne.size(); pos++) {
                final Object element = elementGetter.getElementOrNull(arrayOne, pos);
                if (element == null) {
                    isNullPresentInArrayOne = true;
                } else {
                    ObjectContainer objectContainer = new ObjectContainer(element);
                    if (!seen.contains(objectContainer)) {
                        seen.add(objectContainer);
                        list.add(element);
                    }
                }
            }
            if (!isNullPresentInArrayTwo && isNullPresentInArrayOne) {
                list.add(null);
            }
            return new GenericArrayData(list.toArray());
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private class ObjectContainer {

        private final Object o;

        public ObjectContainer(Object o) {
            this.o = o;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ObjectContainer)) {
                return false;
            }
            ObjectContainer that = (ObjectContainer) other;
            return equalityAndHashcodeProvider.equals(this.o, that.o);
        }

        @Override
        public int hashCode() {
            return equalityAndHashcodeProvider.hashCode(o);
        }
    }
}
