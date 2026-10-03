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

import java.util.List;

/**
 * Namespaced (e.g. window-scoped) keyed state reader function for value, list and map state types.
 *
 * <p>Unlike {@link KeyedStateReader}, states are not registered/cached in {@code open()}: the
 * namespace varies per {@code (key, namespace)} pair being processed, so each state must be fetched
 * fresh, scoped to the current namespace, via {@link
 * WindowKeyedStateReaderFunction.Context#getState}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WindowKeyedStateReader extends WindowKeyedStateReaderFunction<Object, RowData> {

    private final RowType rowType;
    private final WindowStateTableMapping mapping;
    private final StateValueConverter converter = new StateValueConverter();

    public WindowKeyedStateReader(final RowType rowType, final WindowStateTableMapping mapping) {
        this.rowType = rowType;
        this.mapping = mapping;
    }

    @Override
    public void readKey(Object key, Object namespace, Context context, Collector<RowData> out)
            throws Exception {
        GenericRowData row = new GenericRowData(RowKind.INSERT, rowType.getFieldCount());

        List<RowType.RowField> fields = rowType.getFields();

        int keyColumnIndex = mapping.getKeyColumnIndex();
        if (keyColumnIndex >= 0) {
            LogicalType keyLogicalType = fields.get(keyColumnIndex).getType();
            row.setField(keyColumnIndex, converter.getValue(keyLogicalType, key));
        }

        int windowColumnIndex = mapping.getWindowColumnIndex();
        if (windowColumnIndex >= 0) {
            LogicalType windowLogicalType = fields.get(windowColumnIndex).getType();
            row.setField(windowColumnIndex, converter.getValue(windowLogicalType, namespace));
        }

        for (StateValueColumnConfiguration columnConfig : mapping.getValueColumns()) {
            LogicalType valueLogicalType = fields.get(columnConfig.getColumnIndex()).getType();
            switch (columnConfig.getStateType()) {
                case VALUE:
                    State valueState = context.getState(columnConfig.getStateDescriptor());
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(
                                    valueLogicalType,
                                    StateValueConverter.readValueLikeState(
                                            valueState, columnConfig.getActualStateKind())));
                    break;

                case LIST:
                    ListState listState =
                            (ListState) context.getState(columnConfig.getStateDescriptor());
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(valueLogicalType, listState.get()));
                    break;

                case MAP:
                    MapState mapState =
                            (MapState) context.getState(columnConfig.getStateDescriptor());
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(valueLogicalType, mapState.entries()));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type: " + columnConfig.getStateType());
            }
        }

        out.collect(row);
    }
}
