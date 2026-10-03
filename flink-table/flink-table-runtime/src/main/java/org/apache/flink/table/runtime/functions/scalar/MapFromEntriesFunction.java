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
import org.apache.flink.table.runtime.util.MapDataContainer;
import org.apache.flink.table.runtime.util.ObjectContainer;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nullable;

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
        final List<DataType> fieldDataTypes = DataType.getFieldDataTypes(entryDataType);
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

        final int size = input.size();
        // a duplicate key keeps the position of its first occurrence and the last value wins
        final Map<ObjectContainer, Object> entries =
                CollectionUtil.newLinkedHashMapWithExpectedSize(size);
        for (int pos = 0; pos < size; pos++) {
            final RowData entry = (RowData) entryElementGetter.getElementOrNull(input, pos);
            if (entry == null) {
                return null;
            }
            entries.put(
                    wrapKey(keyFieldGetter.getFieldOrNull(entry)),
                    valueFieldGetter.getFieldOrNull(entry));
        }
        final int distinctKeyCount = entries.size();

        final Object[] keys = new Object[distinctKeyCount];
        final Object[] values = new Object[distinctKeyCount];
        int pos = 0;
        for (Map.Entry<ObjectContainer, Object> entry : entries.entrySet()) {
            final ObjectContainer key = entry.getKey();
            keys[pos] = key == null ? null : key.getObject();
            values[pos] = entry.getValue();
            pos++;
        }
        return new MapDataContainer(new GenericArrayData(keys), new GenericArrayData(values));
    }

    /**
     * Hashes and compares the key with SQL semantics instead of {@link Object#equals}. A {@code
     * null} key is returned unwrapped, so all {@code null} keys are treated as equal and collapse
     * into a single entry.
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
}
