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
import org.apache.flink.table.data.ArrayBasedMapData;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.stream.IntStream;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_FROM_ENTRIES}. */
@Internal
public class MapFromEntriesFunction extends BuiltInScalarFunction {
    private final RowData.FieldGetter[] fieldGetters;

    public MapFromEntriesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_FROM_ENTRIES, context);
        RowType rowType =
                (RowType)
                        ((CollectionDataType)
                                        context.getCallContext().getArgumentDataTypes().get(0))
                                .getElementDataType()
                                .getLogicalType();
        fieldGetters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                        .toArray(RowData.FieldGetter[]::new);
    }

    public @Nullable MapData eval(@Nullable ArrayData input) {
        if (input == null) {
            return null;
        }

        int size = input.size();
        Object[] keys = new Object[size];
        Object[] values = new Object[size];
        for (int pos = 0; pos < size; pos++) {
            if (input.isNullAt(pos)) {
                return null;
            }
            RowData rowData = input.getRow(pos, 2);
            keys[pos] = fieldGetters[0].getFieldOrNull(rowData);
            values[pos] = fieldGetters[1].getFieldOrNull(rowData);
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
    }
}
