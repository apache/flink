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
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_FROM_ARRAYS}. */
@Internal
public class MapFromArraysFunction extends BuiltInScalarFunction {
    public MapFromArraysFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_FROM_ARRAYS, context);
    }

    public @Nullable MapData eval(@Nullable ArrayData keysArray, @Nullable ArrayData valuesArray) {
        if (keysArray == null || valuesArray == null) {
            return null;
        }

        if (keysArray.size() != valuesArray.size()) {
            throw new FlinkRuntimeException(
                    "Invalid function MAP_FROM_ARRAYS call:\n"
                            + "The length of the keys array "
                            + keysArray.size()
                            + " is not equal to the length of the values array "
                            + valuesArray.size());
        }
        return new MapDataForMapFromArrays(keysArray, valuesArray);
    }

    private static class MapDataForMapFromArrays implements MapData {
        private final ArrayData keyArray;
        private final ArrayData valueArray;

        public MapDataForMapFromArrays(ArrayData keyArray, ArrayData valueArray) {
            this.keyArray = keyArray;
            this.valueArray = valueArray;
        }

        @Override
        public int size() {
            return keyArray.size();
        }

        @Override
        public ArrayData keyArray() {
            return keyArray;
        }

        @Override
        public ArrayData valueArray() {
            return valueArray;
        }
    }
}
