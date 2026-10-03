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

package org.apache.flink.state.table;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Reads a single flattened keyed list/map state, emitting one row per list element / map entry
 * instead of one row per key: {@code (state_key, index, value)} for LIST, {@code (state_key,
 * map_key, value)} for MAP.
 *
 * <p>Shares value-conversion logic ({@link StateValueConverter}) with {@link KeyedStateReader}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class FlattenedKeyedStateReader extends KeyedStateReaderFunction<Object, RowData> {

    private final RowType rowType;
    private final FlattenedStateTableMapping mapping;
    private final StateValueConverter converter = new StateValueConverter();

    private transient State state;

    public FlattenedKeyedStateReader(RowType rowType, FlattenedStateTableMapping mapping) {
        this.rowType = rowType;
        this.mapping = mapping;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        switch (mapping.getStateType()) {
            case LIST:
                state =
                        getRuntimeContext()
                                .getListState((ListStateDescriptor) mapping.getStateDescriptor());
                break;

            case MAP:
                state =
                        getRuntimeContext()
                                .getMapState((MapStateDescriptor) mapping.getStateDescriptor());
                break;

            default:
                throw new UnsupportedOperationException(
                        "Unsupported flattened state type: " + mapping.getStateType());
        }
    }

    @Override
    public void close() {
        state = null;
    }

    @Override
    public void readKey(Object key, Context context, Collector<RowData> out) throws Exception {
        LogicalType keyLogicalType =
                rowType.getFields()
                        .get(FlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX)
                        .getType();
        Object convertedKey = converter.getValue(keyLogicalType, key);

        switch (mapping.getStateType()) {
            case LIST:
                readList(convertedKey, out);
                break;

            case MAP:
                readMap(convertedKey, out);
                break;

            default:
                throw new UnsupportedOperationException(
                        "Unsupported flattened state type: " + mapping.getStateType());
        }
    }

    private void readList(Object convertedKey, Collector<RowData> out) throws Exception {
        LogicalType valueLogicalType =
                rowType.getFields().get(FlattenedStateTableMapping.VALUE_COLUMN_INDEX).getType();
        LogicalType indexLogicalType =
                rowType.getFields().get(FlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX).getType();

        Iterable<Object> values = (Iterable<Object>) ((ListState) state).get();
        converter.writeListRows(
                values,
                () -> {
                    GenericRowData row = new GenericRowData(RowKind.INSERT, 3);
                    row.setField(FlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX, convertedKey);
                    return row;
                },
                FlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX,
                FlattenedStateTableMapping.VALUE_COLUMN_INDEX,
                indexLogicalType,
                valueLogicalType,
                out);
    }

    private void readMap(Object convertedKey, Collector<RowData> out) throws Exception {
        LogicalType valueLogicalType =
                rowType.getFields().get(FlattenedStateTableMapping.VALUE_COLUMN_INDEX).getType();
        LogicalType mapKeyLogicalType =
                rowType.getFields().get(FlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX).getType();

        Iterable<Map.Entry<Object, Object>> entries = ((MapState) state).entries();
        converter.writeMapRows(
                entries,
                () -> {
                    GenericRowData row = new GenericRowData(RowKind.INSERT, 3);
                    row.setField(FlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX, convertedKey);
                    return row;
                },
                FlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX,
                FlattenedStateTableMapping.VALUE_COLUMN_INDEX,
                mapKeyLogicalType,
                valueLogicalType,
                out);
    }
}
