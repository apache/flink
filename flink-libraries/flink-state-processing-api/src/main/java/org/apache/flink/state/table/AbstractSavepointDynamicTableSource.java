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
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;

/**
 * Shared planning-time state and behaviour for {@link SavepointDynamicTableSource} and {@link
 * FlattenedSavepointDynamicTableSource}: the common constructor fields, key-column filter push-down
 * (via {@link SavepointFilterTranslator}) and the fixed insert-only changelog mode.
 *
 * <p>The scan itself is delegated to the {@link DataStreamScanProvider} supplied by {@link
 * SavepointDynamicTableSourceFactory} as a constructor reference (e.g. {@code
 * SavepointDataStreamScanProvider::new}), so a single table-source class serves every keyed state
 * table kind without a subclass per kind.
 */
@Internal
abstract class AbstractSavepointDynamicTableSource<M extends SavepointStateMapping>
        implements ScanTableSource, SupportsFilterPushDown {

    /** Builds the {@link DataStreamScanProvider} for a given set of scan-time arguments. */
    interface ScanProviderFactory<M extends SavepointStateMapping> {
        DataStreamScanProvider create(
                @Nullable String stateBackendType,
                String statePath,
                OperatorIdentifier operatorIdentifier,
                Supplier<M> mappingSupplier,
                RowType rowType,
                @Nullable SavepointKeyFilter keyFilter);
    }

    @Nullable protected final String stateBackendType;
    protected final String statePath;
    protected final OperatorIdentifier operatorIdentifier;
    protected final String summaryString;
    protected final ScanProviderFactory<M> scanProviderFactory;

    protected Supplier<M> mappingSupplier;
    protected RowType rowType;

    /**
     * Index of the (single) key column in {@link #rowType}. Tracked eagerly so filter push-down can
     * reference it during planning without resolving the lazy mapping, and updated by projection
     * push-down where supported.
     */
    protected int keyColumnIndex;

    @Nullable protected SavepointKeyFilter keyFilter;

    protected AbstractSavepointDynamicTableSource(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final int keyColumnIndex,
            final Supplier<M> mappingSupplier,
            final RowType rowType,
            final String summaryString,
            final ScanProviderFactory<M> scanProviderFactory) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.keyColumnIndex = keyColumnIndex;
        this.mappingSupplier = mappingSupplier;
        this.rowType = rowType;
        this.summaryString = summaryString;
        this.scanProviderFactory = scanProviderFactory;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        return SavepointFilterTranslator.applyKeyColumnFilters(
                keyColumnIndex, rowType, filters, kf -> this.keyFilter = kf);
    }

    @Override
    public final DynamicTableSource copy() {
        AbstractSavepointDynamicTableSource<M> copy = newInstance();
        copy.keyFilter = this.keyFilter;
        return copy;
    }

    /**
     * Creates a fresh instance carrying the same constructor state as this one (used by {@link
     * #copy()}, which separately copies the mutable {@link #keyFilter}).
     */
    protected abstract AbstractSavepointDynamicTableSource<M> newInstance();

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return scanProviderFactory.create(
                stateBackendType,
                statePath,
                operatorIdentifier,
                mappingSupplier,
                rowType,
                keyFilter);
    }
}
