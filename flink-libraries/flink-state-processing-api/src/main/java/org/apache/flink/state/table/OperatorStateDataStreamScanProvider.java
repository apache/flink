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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Savepoint data stream scan provider for an operator {@code ListState}/{@code UnionState} table.
 *
 * <p>There is no synthetic ordering column: a structured (ROW-typed) value is flattened into one
 * table column per field, while a scalar value gets a single value column named after the state.
 */
@Internal
public class OperatorStateDataStreamScanProvider
        extends AbstractNonKeyedDataStreamScanProvider<OperatorStateTableMapping> {

    public OperatorStateDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<OperatorStateTableMapping> mappingSupplier,
            final RowType rowType) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected DataStream<RowData> readState(
            SavepointReader savepointReader, OperatorStateTableMapping mapping) throws Exception {
        LogicalType valueLogicalType = mapping.getValueLogicalType();
        TypeSerializer<Object> valueSerializer =
                (TypeSerializer<Object>) mapping.getValueTypeSerializer();
        TypeInformation<Object> valueTypeInfo = externalTypeInfo(valueLogicalType, valueSerializer);

        DataStream<Object> raw;
        switch (mapping.getKind()) {
            case LIST:
                raw =
                        savepointReader.readListState(
                                operatorIdentifier,
                                mapping.getStateName(),
                                valueTypeInfo,
                                valueSerializer);
                break;
            case UNION:
                raw =
                        savepointReader.readUnionState(
                                operatorIdentifier,
                                mapping.getStateName(),
                                valueTypeInfo,
                                valueSerializer);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator state kind: " + mapping.getKind());
        }

        return raw.map(new OperatorStateRowMapper(valueLogicalType))
                .returns(InternalTypeInfo.of(rowType));
    }

    /**
     * Converts a raw state element into its {@link RowData} representation: a ROW-typed value is
     * returned as-is (its fields are the table's flattened columns), while any other value is
     * wrapped into a single-column row.
     */
    private static class OperatorStateRowMapper implements MapFunction<Object, RowData> {

        private final LogicalType valueLogicalType;
        private final StateValueConverter converter = new StateValueConverter();

        private OperatorStateRowMapper(LogicalType valueLogicalType) {
            this.valueLogicalType = valueLogicalType;
        }

        @Override
        public RowData map(Object value) {
            Object converted = converter.getValue(valueLogicalType, value);
            if (valueLogicalType.is(LogicalTypeRoot.ROW)) {
                return converted != null
                        ? (RowData) converted
                        : new GenericRowData(
                                RowKind.INSERT, ((RowType) valueLogicalType).getFieldCount());
            }
            GenericRowData row = new GenericRowData(RowKind.INSERT, 1);
            row.setField(0, converted);
            return row;
        }
    }
}
