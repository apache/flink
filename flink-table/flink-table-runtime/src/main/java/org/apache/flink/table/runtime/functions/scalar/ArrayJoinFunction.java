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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Iterator;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_JOIN}. */
@Internal
public class ArrayJoinFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;

    public ArrayJoinFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_JOIN, context);
        final DataType elementDataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
    }

    public @Nullable StringData eval(
            ArrayData array, StringData delimiter, StringData... nullReplacement) {
        try {
            if (array == null
                    || delimiter == null
                    || (nullReplacement.length != 0 && nullReplacement[0] == null)) {
                return null;
            }
            final StringData normalizedReplacement =
                    (nullReplacement.length != 0 && nullReplacement[0] != null)
                            ? nullReplacement[0]
                            : null;
            return BinaryStringDataUtil.concatWs(
                    (BinaryStringData) delimiter,
                    () ->
                            new ArrayIterator(
                                    elementGetter,
                                    array,
                                    (BinaryStringData) normalizedReplacement));
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    static final class ArrayIterator implements Iterator<BinaryStringData> {

        private final int size;
        private final ArrayData.ElementGetter elementGetter;
        private final ArrayData arrayData;
        private final BinaryStringData nullReplacement;
        private int currentPos;

        public ArrayIterator(
                ArrayData.ElementGetter elementGetter,
                ArrayData arrayData,
                BinaryStringData nullReplacement) {
            this.size = arrayData.size();
            this.elementGetter = elementGetter;
            this.arrayData = arrayData;
            this.nullReplacement = nullReplacement;
        }

        @Override
        public boolean hasNext() {
            return currentPos < size;
        }

        @Override
        public BinaryStringData next() {
            if (!hasNext()) {
                return null;
            }
            final Object str = elementGetter.getElementOrNull(arrayData, currentPos);
            currentPos++;

            if (str == null && nullReplacement != null) {
                return nullReplacement;
            }

            return (BinaryStringData) str;
        }
    }
}
