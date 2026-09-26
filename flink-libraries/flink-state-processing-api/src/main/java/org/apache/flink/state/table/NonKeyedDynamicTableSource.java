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
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * Dynamic table source for a single non-keyed operator state: {@code ListState}/{@code UnionState}
 * (value flattened into the table's columns) or {@code BroadcastState} (fixed {@code (map_key,
 * map_value)} schema).
 *
 * <p>Unlike keyed state (see {@link AbstractSavepointDynamicTableSource}), non-keyed state has no
 * key column, so neither filter nor projection push-down is supported. The scan is delegated to the
 * {@link DataStreamScanProvider} supplied by {@link SavepointDynamicTableSourceFactory}, so a
 * single table-source class serves every non-keyed state kind.
 */
@Internal
public class NonKeyedDynamicTableSource<M> implements ScanTableSource {

    /** Builds the {@link DataStreamScanProvider} for a given set of scan-time arguments. */
    interface ScanProviderFactory<M> {
        DataStreamScanProvider create(
                @Nullable String stateBackendType,
                String statePath,
                OperatorIdentifier operatorIdentifier,
                Supplier<M> mappingSupplier,
                RowType rowType);
    }

    @Nullable private final String stateBackendType;
    private final String statePath;
    private final OperatorIdentifier operatorIdentifier;
    private final Supplier<M> mappingSupplier;
    private final RowType rowType;
    private final String summaryString;
    private final ScanProviderFactory<M> scanProviderFactory;

    public NonKeyedDynamicTableSource(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            final String summaryString,
            final ScanProviderFactory<M> scanProviderFactory) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.mappingSupplier = mappingSupplier;
        this.rowType = rowType;
        this.summaryString = summaryString;
        this.scanProviderFactory = scanProviderFactory;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return scanProviderFactory.create(
                stateBackendType, statePath, operatorIdentifier, mappingSupplier, rowType);
    }

    @Override
    public DynamicTableSource copy() {
        // All fields are immutable and there is no projection/filter push-down on this source.
        return this;
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }
}
