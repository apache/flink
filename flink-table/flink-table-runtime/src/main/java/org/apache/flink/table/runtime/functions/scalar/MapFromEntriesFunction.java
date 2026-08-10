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
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.util.EqualityAndHashcodeProvider;
import org.apache.flink.table.runtime.util.ObjectContainer;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_FROM_ENTRIES}. */
@Internal
public class MapFromEntriesFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter entryElementGetter;
    private final RowData.FieldGetter keyFieldGetter;
    private final RowData.FieldGetter valueFieldGetter;

    private final EqualityAndHashcodeProvider keyEqualityAndHashcodeProvider;

    public MapFromEntriesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_FROM_ENTRIES, context);
        final DataType arrayDataType = context.getCallContext().getArgumentDataTypes().get(0);
        final DataType entryDataType = ((CollectionDataType) arrayDataType).getElementDataType();
        final List<DataType> fieldDataTypes = entryDataType.getChildren();
        final DataType keyDataType = fieldDataTypes.get(0);
        final DataType valueDataType = fieldDataTypes.get(1);

        entryElementGetter = ArrayData.createElementGetter(entryDataType.getLogicalType());
        keyFieldGetter = RowData.createFieldGetter(keyDataType.getLogicalType(), 0);
        valueFieldGetter = RowData.createFieldGetter(valueDataType.getLogicalType(), 1);

        keyEqualityAndHashcodeProvider =
                new EqualityAndHashcodeProvider(context, keyDataType.toInternal());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        keyEqualityAndHashcodeProvider.open(context);
    }

    public @Nullable MapData eval(@Nullable ArrayData input) {
        if (input == null) {
            return null;
        }

        // a duplicate key keeps the position of its first occurrence and the last value wins
        final Map<ObjectContainer, Object> entries = new LinkedHashMap<>();
        for (int pos = 0; pos < input.size(); pos++) {
            final RowData entry = (RowData) entryElementGetter.getElementOrNull(input, pos);
            if (entry == null) {
                return null;
            }
            entries.put(
                    wrapKey(keyFieldGetter.getFieldOrNull(entry)),
                    valueFieldGetter.getFieldOrNull(entry));
        }
        return new MapDataForMapFromEntries(
                new GenericArrayData(
                        entries.keySet().stream()
                                .map(key -> key == null ? null : key.getObject())
                                .toArray()),
                new GenericArrayData(entries.values().toArray()));
    }

    /**
     * Wraps the given key so that it is hashed and compared with the generated hashcode and
     * equality of the key type, which implement SQL semantics for internal data structures unlike
     * {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    private @Nullable ObjectContainer wrapKey(@Nullable Object key) {
        if (key == null) {
            return null;
        }
        return new ObjectContainer(
                key,
                keyEqualityAndHashcodeProvider::equals,
                keyEqualityAndHashcodeProvider::hashCode);
    }

    @Override
    public void close() throws Exception {
        keyEqualityAndHashcodeProvider.close();
    }

    private static class MapDataForMapFromEntries implements MapData {
        private final GenericArrayData keyArray;
        private final GenericArrayData valueArray;

        MapDataForMapFromEntries(GenericArrayData keyArray, GenericArrayData valueArray) {
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
