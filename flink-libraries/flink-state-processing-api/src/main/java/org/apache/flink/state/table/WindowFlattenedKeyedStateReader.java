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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.state.api.functions.WindowKeyedStateReaderFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Reads a single flattened namespaced (e.g. window-scoped) keyed list/map state, emitting one row
 * per list element / map entry per {@code (key, namespace)} pair: {@code (state_key, state_window,
 * index, value)} for LIST, {@code (state_key, state_window, map_key, value)} for MAP.
 *
 * <p>Unlike {@link FlattenedKeyedStateReader}, the state is not cached in {@code open()}: the
 * namespace varies per {@code (key, namespace)} pair being processed, so it is fetched fresh,
 * scoped to the current namespace, via {@link WindowKeyedStateReaderFunction.Context#getState}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WindowFlattenedKeyedStateReader
        extends WindowKeyedStateReaderFunction<Object, RowData> {

    private final RowType rowType;
    private final WindowFlattenedStateTableMapping mapping;
    private final StateValueConverter converter = new StateValueConverter();

    public WindowFlattenedKeyedStateReader(
            final RowType rowType, final WindowFlattenedStateTableMapping mapping) {
        this.rowType = rowType;
        this.mapping = mapping;
    }

    @Override
    public void readKey(Object key, Object namespace, Context context, Collector<RowData> out)
            throws Exception {
        LogicalType keyLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX)
                        .getType();
        Object convertedKey = converter.getValue(keyLogicalType, key);

        LogicalType windowLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.WINDOW_COLUMN_INDEX)
                        .getType();
        Object convertedWindow = converter.getValue(windowLogicalType, namespace);

        State state = context.getState(mapping.getStateDescriptor());

        switch (mapping.getStateType()) {
            case LIST:
                readList(convertedKey, convertedWindow, (ListState) state, out);
                break;

            case MAP:
                readMap(convertedKey, convertedWindow, (MapState) state, out);
                break;

            default:
                throw new UnsupportedOperationException(
                        "Unsupported flattened state type: " + mapping.getStateType());
        }
    }

    private void readList(
            Object convertedKey, Object convertedWindow, ListState state, Collector<RowData> out)
            throws Exception {
        LogicalType valueLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.VALUE_COLUMN_INDEX)
                        .getType();
        LogicalType indexLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX)
                        .getType();

        Iterable<Object> values = (Iterable<Object>) state.get();
        converter.writeListRows(
                values,
                () -> {
                    GenericRowData row = new GenericRowData(RowKind.INSERT, 4);
                    row.setField(
                            WindowFlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX, convertedKey);
                    row.setField(
                            WindowFlattenedStateTableMapping.WINDOW_COLUMN_INDEX, convertedWindow);
                    return row;
                },
                WindowFlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX,
                WindowFlattenedStateTableMapping.VALUE_COLUMN_INDEX,
                indexLogicalType,
                valueLogicalType,
                out);
    }

    private void readMap(
            Object convertedKey, Object convertedWindow, MapState state, Collector<RowData> out)
            throws Exception {
        LogicalType valueLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.VALUE_COLUMN_INDEX)
                        .getType();
        LogicalType mapKeyLogicalType =
                rowType.getFields()
                        .get(WindowFlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX)
                        .getType();

        Iterable<Map.Entry<Object, Object>> entries = state.entries();
        converter.writeMapRows(
                entries,
                () -> {
                    GenericRowData row = new GenericRowData(RowKind.INSERT, 4);
                    row.setField(
                            WindowFlattenedStateTableMapping.STATE_KEY_COLUMN_INDEX, convertedKey);
                    row.setField(
                            WindowFlattenedStateTableMapping.WINDOW_COLUMN_INDEX, convertedWindow);
                    return row;
                },
                WindowFlattenedStateTableMapping.SUB_KEY_COLUMN_INDEX,
                WindowFlattenedStateTableMapping.VALUE_COLUMN_INDEX,
                mapKeyLogicalType,
                valueLogicalType,
                out);
    }
}
