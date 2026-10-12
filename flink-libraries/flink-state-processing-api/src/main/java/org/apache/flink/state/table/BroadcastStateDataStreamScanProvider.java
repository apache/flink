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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Savepoint data stream scan provider for an operator {@code BroadcastState} table, exposing one
 * row per map entry as {@code (map_key, value)}.
 */
@Internal
public class BroadcastStateDataStreamScanProvider
        extends AbstractNonKeyedDataStreamScanProvider<BroadcastStateTableMapping> {

    public BroadcastStateDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<BroadcastStateTableMapping> mappingSupplier,
            final RowType rowType) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected DataStream<RowData> readState(
            SavepointReader savepointReader, BroadcastStateTableMapping mapping) throws Exception {
        LogicalType mapKeyLogicalType =
                rowType.getFields().get(BroadcastStateTableMapping.MAP_KEY_COLUMN_INDEX).getType();
        LogicalType valueLogicalType =
                rowType.getFields().get(BroadcastStateTableMapping.VALUE_COLUMN_INDEX).getType();

        TypeSerializer<Object> mapKeySerializer =
                (TypeSerializer<Object>) mapping.getMapKeyTypeSerializer();
        TypeSerializer<Object> valueSerializer =
                (TypeSerializer<Object>) mapping.getValueTypeSerializer();

        DataStream<Tuple2<Object, Object>> raw =
                savepointReader.readBroadcastState(
                        operatorIdentifier,
                        mapping.getStateName(),
                        externalTypeInfo(mapKeyLogicalType, mapKeySerializer),
                        externalTypeInfo(valueLogicalType, valueSerializer),
                        mapKeySerializer,
                        valueSerializer);

        return raw.map(new BroadcastStateRowMapper(mapKeyLogicalType, valueLogicalType))
                .returns(InternalTypeInfo.of(rowType));
    }

    /** Converts a raw {@code (map_key, value)} tuple into its {@link RowData} representation. */
    private static class BroadcastStateRowMapper
            implements MapFunction<Tuple2<Object, Object>, RowData> {

        private final LogicalType mapKeyLogicalType;
        private final LogicalType valueLogicalType;
        private final StateValueConverter converter = new StateValueConverter();

        private BroadcastStateRowMapper(
                LogicalType mapKeyLogicalType, LogicalType valueLogicalType) {
            this.mapKeyLogicalType = mapKeyLogicalType;
            this.valueLogicalType = valueLogicalType;
        }

        @Override
        public RowData map(Tuple2<Object, Object> entry) {
            GenericRowData row = new GenericRowData(RowKind.INSERT, 2);
            row.setField(
                    BroadcastStateTableMapping.MAP_KEY_COLUMN_INDEX,
                    converter.getValue(mapKeyLogicalType, entry.f0));
            row.setField(
                    BroadcastStateTableMapping.VALUE_COLUMN_INDEX,
                    converter.getValue(valueLogicalType, entry.f1));
            return row;
        }
    }
}
