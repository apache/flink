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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.KeyValueDataType;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_ENTRIES}. */
@Internal
public class MapEntriesFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter keyElementGetter;
    private final ArrayData.ElementGetter valueElementGetter;

    public MapEntriesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_ENTRIES, context);
        KeyValueDataType inputType =
                ((KeyValueDataType) context.getCallContext().getArgumentDataTypes().get(0));
        keyElementGetter =
                ArrayData.createElementGetter(inputType.getKeyDataType().getLogicalType());
        valueElementGetter =
                ArrayData.createElementGetter(inputType.getValueDataType().getLogicalType());
    }

    public @Nullable ArrayData eval(@Nullable MapData input) {
        if (input == null) {
            return null;
        }

        ArrayData keys = input.keyArray();
        ArrayData values = input.valueArray();
        int size = input.size();

        Object[] resultData = new Object[size];
        for (int pos = 0; pos < size; pos++) {
            final Object key = keyElementGetter.getElementOrNull(keys, pos);
            final Object value = valueElementGetter.getElementOrNull(values, pos);
            resultData[pos] = GenericRowData.of(key, value);
        }
        return new GenericArrayData(resultData);
    }
}
