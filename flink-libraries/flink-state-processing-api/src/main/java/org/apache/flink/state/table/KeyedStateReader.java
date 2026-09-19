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
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Keyed state reader function for value, list and map state types. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KeyedStateReader extends KeyedStateReaderFunction<Object, RowData> {

    private final StateTableMapping mapping;
    private final RowType rowType;
    private final StateValueConverter converter = new StateValueConverter();

    /**
     * States keyed by state name. Populated from {@link StateTableMapping#getAllValueColumns()} so
     * that ALL original states are registered with the Flink runtime (required for key enumeration
     * in {@code KeyedStateReaderOperator.getKeysAndNamespaces}), even when some columns have been
     * projected out.
     */
    private final Map<String, State> states = new HashMap<>();

    public KeyedStateReader(final RowType rowType, final StateTableMapping mapping) {
        this.mapping = mapping;
        this.rowType = rowType;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Register ALL original value columns so that key enumeration always works,
        // even when a projection has removed some (or all) value columns from the output.
        for (StateValueColumnConfiguration columnConfig : mapping.getAllValueColumns()) {
            switch (columnConfig.getStateType()) {
                case VALUE:
                    states.put(
                            columnConfig.getStateName(), getOrCreateValueLikeState(columnConfig));
                    break;

                case LIST:
                    states.put(
                            columnConfig.getStateName(),
                            getRuntimeContext()
                                    .getListState(
                                            (ListStateDescriptor)
                                                    columnConfig.getStateDescriptor()));
                    break;

                case MAP:
                    states.put(
                            columnConfig.getStateName(),
                            getRuntimeContext()
                                    .getMapState(
                                            (MapStateDescriptor)
                                                    columnConfig.getStateDescriptor()));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type: " + columnConfig.getStateType());
            }
        }
    }

    /**
     * Registers the VALUE-shaped state for {@code columnConfig}, using the state-getter matching
     * its {@link StateValueColumnConfiguration#getActualStateKind()} (e.g. {@code
     * getReducingState}/{@code getAggregatingState} for a {@code .reduce()}/{@code .aggregate()}
     * window function's window-contents state, which is registered as {@code REDUCING}/{@code
     * AGGREGATING} rather than plain {@code VALUE}).
     */
    private State getOrCreateValueLikeState(StateValueColumnConfiguration columnConfig)
            throws Exception {
        StateDescriptor descriptor = columnConfig.getStateDescriptor();
        switch (columnConfig.getActualStateKind()) {
            case REDUCING:
                return getRuntimeContext().getReducingState((ReducingStateDescriptor) descriptor);
            case AGGREGATING:
                return getRuntimeContext()
                        .getAggregatingState((AggregatingStateDescriptor) descriptor);
            default:
                return getRuntimeContext().getState((ValueStateDescriptor) descriptor);
        }
    }

    @Override
    public void close() {
        states.clear();
    }

    @Override
    public void readKey(Object key, Context context, Collector<RowData> out) throws Exception {
        GenericRowData row = new GenericRowData(RowKind.INSERT, rowType.getFieldCount());

        List<RowType.RowField> fields = rowType.getFields();

        int columnIndex = mapping.getKeyColumnIndex();
        if (columnIndex >= 0) {
            LogicalType keyLogicalType = fields.get(columnIndex).getType();
            row.setField(columnIndex, converter.getValue(keyLogicalType, key));
        }

        // Only write the projected value columns to the output row.
        for (StateValueColumnConfiguration columnConfig : mapping.getValueColumns()) {
            LogicalType valueLogicalType = fields.get(columnConfig.getColumnIndex()).getType();
            State state = states.get(columnConfig.getStateName());
            switch (columnConfig.getStateType()) {
                case VALUE:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(
                                    valueLogicalType,
                                    StateValueConverter.readValueLikeState(
                                            state, columnConfig.getActualStateKind())));
                    break;

                case LIST:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(
                                    valueLogicalType,
                                    StateValueConverter.readListLikeState((ListState) state)));
                    break;

                case MAP:
                    row.setField(
                            columnConfig.getColumnIndex(),
                            converter.getValue(valueLogicalType, ((MapState) state).entries()));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type: " + columnConfig.getStateType());
            }
        }

        out.collect(row);
    }
}
