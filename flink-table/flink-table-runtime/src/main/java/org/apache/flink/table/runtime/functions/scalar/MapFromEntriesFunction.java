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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MAPKEY_DEDUP_POLICY;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_FROM_ENTRIES}. */
@Internal
public class MapFromEntriesFunction extends BuiltInScalarFunction {
    private final RowData.FieldGetter[] fieldGetters;
    private boolean mapKeyDedupThrowException;

    public MapFromEntriesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_FROM_ENTRIES, context);
        ExecutionConfigOptions.MapKeyDedupPolicy mapKeyDedupPolicy =
                context.getConfiguration().get(TABLE_EXEC_MAPKEY_DEDUP_POLICY);
        switch (mapKeyDedupPolicy) {
            case EXCEPTION:
                mapKeyDedupThrowException = true;
                break;
            case LAST_WIN:
                mapKeyDedupThrowException = false;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown mapKey deduplicate policy strategy: " + mapKeyDedupPolicy);
        }
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
        Map<Object, Object> map = new HashMap<>();
        for (int pos = 0; pos < size; pos++) {
            if (input.isNullAt(pos)) {
                return null;
            }

            RowData rowData = input.getRow(pos, 2);
            final Object key = fieldGetters[0].getFieldOrNull(rowData);
            if (key == null) {
                throw new FlinkRuntimeException(
                        "Invalid function MAP_FROM_ENTRIES call:\n" + "Map key can not be null");
            }
            if (mapKeyDedupThrowException && map.containsKey(key)) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Invalid function MAP_FROM_ENTRIES call:\n"
                                        + "Duplicate keys %s are not allowed",
                                key));
            }
            final Object value = fieldGetters[1].getFieldOrNull(rowData);
            map.put(key, value);
        }
        return new GenericMapData(map);
    }
}
