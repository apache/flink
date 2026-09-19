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

import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Savepoint data stream scan provider for the general keyed state table, emitting one row per key
 * (see {@link KeyedStateReader}).
 */
@SuppressWarnings("rawtypes")
public class SavepointDataStreamScanProvider
        extends AbstractMultiColumnScanProvider<StateTableMapping> {

    public SavepointDataStreamScanProvider(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<StateTableMapping> mappingSupplier,
            RowType rowType,
            @Nullable SavepointKeyFilter keyFilter) {
        super(stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType, keyFilter);
    }

    @Override
    protected DataStream<RowData> readState(
            SavepointReader savepointReader, StateTableMapping mapping) throws Exception {
        return readVoidNamespaceKeyedState(
                savepointReader, mapping, new KeyedStateReader(rowType, mapping));
    }
}
